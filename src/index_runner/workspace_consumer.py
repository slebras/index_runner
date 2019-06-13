"""
Consume workspace update events from kafka and publish new indexes.
"""
import sys
import traceback
import concurrent.futures

from .utils.kafka_consumer import kafka_consumer
from .utils.config import get_config
from .utils import es_utils
from .indexers.main import index_obj
from .indexers.indexer_utils import (
    check_object_deleted,
    check_workspace_deleted,
    fetch_objects_in_workspace,
    is_workspace_public
)

_CONFIG = get_config()


def main(es_queue):
    """
    Main consumer of Kafka messages from workspace updates, generating new indexes.
    `es_queue` is a thread queue that can be used to send data to
        `es_writer.py` for indexing or deleting documents.
    """
    topics = [
        _CONFIG['topics']['workspace_events'],
        _CONFIG['topics']['indexer_admin_events']
    ]
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=4)
    for msg_data in kafka_consumer(topics):
        pool.submit(_process_event, msg_data, es_queue)


def _process_event(msg_data, es_queue):
    """
    Process a new workspace event. This is the main switchboard for handling
    new workspace events. Dispatches to functions in the `event_type_handlers`
    dict below.
    Args:
        msg_data - json data received in the kafka event
    See the event_handler dictionaries at the bottom of this module for a
    mapping of event names to function handlers.
    """
    # Workspace events reference:
    # https://github.com/kbase/workspace_deluxe/blob/master/docsource/events.rst
    event_type = msg_data.get('evtype')
    ws_id = msg_data.get('wsid')
    if not ws_id:
        raise RuntimeError(f'Invalid wsid in event: {ws_id}')
    if not event_type:
        raise RuntimeError(f"Missing 'evtype' in event: {msg_data}")
    if event_type not in event_type_handlers:
        raise RuntimeError(f"Unrecognized event {event_type}.")
    try:
        event_type_handlers[event_type](msg_data, es_queue)
    except Exception as err:
        print(f"Error indexing:\n{type(err)} - {err}")
        traceback.print_exc()
        _log_error(msg_data, err)


def _log_error(msg_data, err=None):
    """Log an indexing error to elasticsearch and stdout."""
    print('Error writing index')
    print('-' * 80)
    print(err)
    print(msg_data)
    # TODO error log to es index
    # The key is a hash of the message data body
    # The index document is the error string plus the message data itself
    # data = {
    #     'id': hashlib.blake2b(json.dumps(msg_data).encode('utf-8')).hexdigest(),
    #     'index': _CONFIG['error_index_name'],
    #     'doc': {'error': str(err), **msg_data}
    # }
    # _PRODUCER.produce(
    #     _CONFIG['topics']['elasticsearch_updates'],
    #     json.dumps(data),
    #     'index',
    #     callback=_delivery_report
    # )
    # _PRODUCER.poll(60)


def _run_indexer(msg_data, es_queue):
    """
    Run the indexer for a workspace event message and produce an event for it.
    This will be threaded and backgrounded.
    """
    # index_obj returns a generator
    for result in index_obj(msg_data, es_queue):
        if not result:
            _log_error(msg_data)
            continue
        # Push to the elasticsearch write queue
        es_queue.put({'_action': 'index', **result})


def _run_obj_deleter(msg_data, es_queue):
    """
    checks that object that is received is deleted because the workspace object
    delete event can refer to delete or undelete state changes.

    NOTE: Gavin said there should be a check when this message is received,
          that the state of the object we're querying actually is deleted.
    """
    # verify that object is deleted
    wsid = msg_data['wsid']
    objid = msg_data['objid']
    if not check_object_deleted(wsid, objid):
        # Object is not deleted
        print(f'object {objid} in workspace {wsid} not deleted')
        return
    es_queue.put({'_action': 'delete', 'object_id': f"{wsid}:{objid}"})


def _run_workspace_deleter(msg_data, es_queue):
    """
    Checks that the received workspace is deleted because the
    delete event can refer to both delete or undelete state changes.
    """
    # Verify that this workspace is actually deleted
    wsid = msg_data['wsid']
    if not check_workspace_deleted(wsid):
        print(f'Workspace {wsid} not deleted')
        return
    es_queue.put({'workspace_id': str(wsid), '_action': 'delete'})


def _clone_workspace(msg_data, es_queue):
    """
    Handles the CLONE_WORKSPACE event
    Iterates over each object in a given workspace and indexes them.
    """
    workspace_data = fetch_objects_in_workspace(msg_data['wsid'], include_narrative=True)
    for obj in workspace_data:
        dummy_msg_data = {
            "wsid": msg_data["wsid"],
            "objid": obj["obj_id"],
        }
        _run_indexer(dummy_msg_data, es_queue)


def _set_global_permission(msg_data, es_queue):
    """
    Handles the SET_GLOBAL_PERMISSION event.
    eg. this happens when making a narrative public.
    """
    # Check what the permission is on the workspace
    workspace_id = msg_data['wsid']
    is_public = is_workspace_public(workspace_id)
    # Push the event to the elasticsearch writer queue
    es_queue.put({
        '_action': 'set_global_perm',
        'workspace_id': workspace_id,
        'is_public': is_public
    })


def _index_nonexistent(msg_data, es_queue):
    """
    Handler for INDEX_NONEXISTENT.
    Index a document on elasticsearch only if it does not already exist there.
    Expects msg_data to have both 'wsid' and 'objid'.
    """
    exists = es_utils.does_doc_exist(msg_data['wsid'], msg_data['objid'])
    if not exists:
        _run_indexer(msg_data, es_queue)


event_type_handlers = {
    # Admin events
    'REINDEX': _run_indexer,
    'INDEX_NONEXISTENT': _index_nonexistent,

    # Workspace events
    'NEW_VERSION': _run_indexer,
    'OBJECT_DELETE_STATE_CHANGE': _run_obj_deleter,
    'WORKSPACE_DELETE_STATE_CHANGE': _run_workspace_deleter,
    'COPY_OBJECT': _run_indexer,
    'RENAME_OBJECT': _run_indexer,
    'CLONE_WORKSPACE': _clone_workspace,
    'SET_GLOBAL_PERMISSION': _set_global_permission,
}


def _delivery_report(err, msg):
    """Kafka producer callback."""
    if err is not None:
        sys.stderr.write(f'Message delivery failed for "{msg.key()}" in {msg.topic()}: {err}\n')
    else:
        print(f'Message "{msg.key()}" delivered to {msg.topic()}')
