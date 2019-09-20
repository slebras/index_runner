"""
Takes workspace kafka event data and generates new Elasticsearch index upates
(creations, updates, deletes, etc)
Pushes work to es_writer.
"""
import time
import json
import hashlib
import traceback
from confluent_kafka import Producer
from kbase_workspace_client import WorkspaceClient

from src.utils.worker_group import WorkerGroup
from src.index_runner.es_writer import ESWriter
from src.utils.config import config
from src.utils import es_utils
from src.index_runner.es_indexers.main import index_obj
from src.index_runner.es_indexers.indexer_utils import (
    check_object_deleted,
    check_workspace_deleted,
    fetch_objects_in_workspace,
    is_workspace_public
)


class ESIndexer:

    @classmethod
    def init_children(cls):
        """Initialize a worker group of ESWriters, which we push work into."""
        es_writers = WorkerGroup(ESWriter, (), count=config()['workers']['num_es_writers'])
        return {'es_writers': es_writers}

    def reload_aliases(self, msg):
        """"""
        try:
            self.children['es_writers'].put(('reload_aliases', msg))
        except Exception as err:
            print('=' * 80)
            print(f"Error reloading aliases:\n{type(err)} - {err}")
            print(msg)
            print(err)
            traceback.print_exc()
            print('=' * 80)
            _log_err_to_es(self.children['es_writers'], msg, err)

    def ws_event(self, msg):
        """
        Receive a workspace event from the kafka consumer.
        msg will have fields described here: https://kbase.us/services/ws/docs/events.html
        """
        event_type = msg.get('evtype')
        ws_id = msg.get('wsid')
        if not ws_id:
            print(f'Invalid wsid in event: {ws_id}')
            return
        if not event_type:
            print(f"Missing 'evtype' in event: {msg}")
            return
        print(f'es_writer received {msg["evtype"]} for {ws_id}/{msg.get("objid", "?")}')
        try:
            if event_type in ['REINDEX', 'NEW_VERSION', 'COPY_OBJECT', 'RENAME_OBJECT']:
                print('Running indexer..')
                self._run_indexer(msg)
                print('Done running indexer..')
            elif event_type == 'REINDEX_WS':
                self._index_ws(msg)
            elif event_type == 'INDEX_NONEXISTENT_WS':
                self._index_nonexistent_ws(msg)
            elif event_type == 'INDEX_NONEXISTENT':
                self._index_nonexistent(msg)
            elif event_type == 'OBJECT_DELETE_STATE_CHANGE':
                self._run_obj_deleter(msg)
            elif event_type == 'WORKSPACE_DELETE_STATE_CHANGE':
                self._run_workspace_deleter(msg)
            elif event_type == 'CLONE_WORKSPACE':
                self._clone_workspace(msg)
            elif event_type == 'SET_GLOBAL_PERMISSION':
                self._set_global_permission(msg)
            elif event_type == 'RELOAD_ELASTIC_ALIASES':
                self.reload_aliases(msg)
            else:
                print(f"Unrecognized event {event_type}.")
                return
        except Exception as err:
            print('=' * 80)
            print(f"Error indexing:\n{type(err)} - {err}")
            print(msg)
            print(err)
            traceback.print_exc()
            print('=' * 80)
            _log_err_to_es(self.children['es_writers'], msg, err)

    def _run_indexer(self, msg):
        """
        Run the indexer for a workspace event message and produce an event for it.
        This will be threaded and backgrounded.
        """
        # index_obj returns a generator
        start = time.time()
        for result in index_obj(msg):
            if not result:
                _log_err_to_es(self.children['es_writers'], result)
                continue
            # Push to the elasticsearch write queue
            self.children['es_writers'].put((result['_action'], result))
        print(f'_run_indexer finished in {time.time() - start}s')

    def _index_ws(self, msg):
        """Index all objects in a workspace."""
        ws_client = WorkspaceClient(url=config()['kbase_endpoint'], token=config()['ws_token'])
        for (objid, ver) in ws_client.generate_all_ids_for_workspace(msg['wsid']):
            _produce({'evtype': 'REINDEX', 'wsid': msg['wsid'], 'objid': objid})

    def _index_nonexistent_ws(self, msg):
        """Index all objects in a workspace that haven't already been indexed."""
        ws_client = WorkspaceClient(url=config()['kbase_endpoint'], token=config()['ws_token'])
        for (objid, ver) in ws_client.generate_all_ids_for_workspace(msg['wsid']):
            _produce({'evtype': 'INDEX_NONEXISTENT', 'wsid': msg['wsid'], 'objid': objid})

    def _run_obj_deleter(self, msg):
        """
        Checks that the received object is deleted, since the workspace object
        delete event can refer to either delete or undelete state changes.
        """
        wsid = msg['wsid']
        objid = msg['objid']
        if not check_object_deleted(wsid, objid):
            # Object is not deleted
            print(f'object {objid} in workspace {wsid} not deleted')
            return
        self.children['es_writers'].put(('delete', {'object_id': f"{wsid}:{objid}"}))

    def _run_workspace_deleter(self, msg):
        """
        Checks that the received workspace is deleted because the
        delete event can refer to both delete or undelete state changes.
        """
        # Verify that this workspace is actually deleted
        wsid = msg['wsid']
        if not check_workspace_deleted(wsid):
            print(f'Workspace {wsid} not deleted')
            return
        self.children['es_writers'].put(('delete', {'workspace_id': str(wsid)}))

    def _clone_workspace(self, msg):
        """
        Handles the CLONE_WORKSPACE event.
        Iterates over each object in a given workspace and indexes them.
        """
        workspace_data = fetch_objects_in_workspace(msg['wsid'], include_narrative=True)
        for obj in workspace_data:
            index_msg = {
                "wsid": msg["wsid"],
                "objid": obj["obj_id"],
            }
            self._run_indexer(index_msg)

    def _set_global_permission(self, msg):
        """
        Handles the SET_GLOBAL_PERMISSION event.
        eg. this happens when making a narrative public.
        """
        # Check what the permission is on the workspace
        workspace_id = msg['wsid']
        is_public = is_workspace_public(workspace_id)
        # Push the event to the elasticsearch writer queue
        self.children['es_writers'].put(('set_global_perm', {
            'workspace_id': workspace_id,
            'is_public': is_public
        }))

    def _index_nonexistent(self, msg):
        """
        Handler for INDEX_NONEXISTENT.
        Index a document on elasticsearch only if it does not already exist there.
        Expects msg to have both 'wsid' and 'objid'.
        """
        exists = es_utils.does_doc_exist(msg['wsid'], msg['objid'])
        if not exists:
            print('Doc does not exist..')
            self._run_indexer(msg)


def _log_err_to_es(es_writers, msg, err=None):
    """Log an indexing error in an elasticsearch index."""
    # The key is a hash of the message data body
    # The index document is the error string plus the message data itself
    _id = hashlib.blake2b(json.dumps(msg).encode('utf-8')).hexdigest()
    es_writers.put(('index', {
        'index': config()['error_index_name'],
        'id': _id,
        'doc': {'error': str(err), **msg}
    }))


def _produce(data, topic=config()['topics']['admin_events']):
    producer = Producer({'bootstrap.servers': config()['kafka_server']})
    producer.produce(topic, json.dumps(data), callback=_delivery_report)
    producer.poll(60)


def _delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed:', err)
    else:
        print('Message delivered to', msg.topic())
