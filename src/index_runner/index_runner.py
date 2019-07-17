"""
Consume workspace update events from kafka and generate new index updates for es_writer.

This sends json messages to es_writer over a thread socket with a special key:
    _action - string - action name (eg. 'index', 'delete', etc)
"""
import time
import zmq
import json
import hashlib
import traceback
from dataclasses import dataclass

from utils.kafka_consumer import kafka_consumer
from utils.config import get_config
from utils import es_utils
from .indexers.main import index_obj
from .indexers.indexer_utils import (
    check_object_deleted,
    check_workspace_deleted,
    fetch_objects_in_workspace,
    is_workspace_public
)

_CONFIG = get_config()


@dataclass
class IndexRunner:
    sock_url: str  # address of socket to push work to

    def __post_init__(self):
        context = zmq.Context.instance()
        self.sock = context.socket(zmq.PUSH)  # send work to the es_writer
        self.sock.connect(self.sock_url)
        # Start the main event loop
        self._run()

    def _run(self):
        """Run the main event loop, ie. the Kafka Consumer, dispatching to self._handle_message."""
        topics = [
            _CONFIG['topics']['workspace_events'],
            _CONFIG['topics']['indexer_admin_events']
        ]
        print("index_runner started.")
        for msg in kafka_consumer(topics):
            self._handle_message(msg)
            print("Finished receiving.")

    def _handle_message(self, msg):
        """Receive a kafka message."""
        event_type = msg.get('evtype')
        ws_id = msg.get('wsid')
        if not ws_id:
            print(f'Invalid wsid in event: {ws_id}')
            return
        if not event_type:
            print(f"Missing 'evtype' in event: {msg}")
            return
        print(f'index_runner received {msg["evtype"]} for {ws_id}/{msg.get("objid", "?")}')
        try:
            if event_type in ['REINDEX', 'NEW_VERSION', 'COPY_OBJECT', 'RENAME_OBJECT']:
                print('Running indexer..')
                self._run_indexer(msg)
                print('Done running indexer..')
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
            self._log_err_to_es(msg, err)

    def _run_indexer(self, msg):
        """
        Run the indexer for a workspace event message and produce an event for it.
        This will be threaded and backgrounded.
        """
        # index_obj returns a generator
        start = time.time()
        for result in index_obj(msg):
            if not result:
                print('Empty result..')
                self._log_err_to_es(msg)
                continue
            # Push to the elasticsearch write queue
            self.sock.send_json(result)
        print(f'_run_indexer finished in {time.time() - start}s')

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
        self.sock.send_json({'_action': 'delete', 'object_id': f"{wsid}:{objid}"})

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
        self.sock.send_json({'_action': 'delete', 'workspace_id': str(wsid)})

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
        self.sock.send_json({
            '_action': 'set_global_perm',
            'workspace_id': workspace_id,
            'is_public': is_public
        })

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

    def _log_err_to_es(self, msg, err=None):
        """Log an indexing error in an elasticsearch index."""
        # The key is a hash of the message data body
        # The index document is the error string plus the message data itself
        _id = hashlib.blake2b(json.dumps(msg).encode('utf-8')).hexdigest()
        self.sock.send_json({
            '_action': 'index',
            'index': _CONFIG['error_index_name'],
            'id': _id,
            'doc': {'error': str(err), **msg}
        })
