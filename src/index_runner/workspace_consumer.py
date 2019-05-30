"""
Consume workspace update events from kafka and publish new indexes.
"""
import sys
import json
from confluent_kafka import Producer

from .utils.kafka_consumer import kafka_consumer
from .utils.config import get_config
from .utils.threadify import threadify
from .indexers.main import index_obj, get_indexer_name
from .indexers.indexer_utils import check_object_deleted, check_workspace_deleted, fetch_objects_in_workspace

_CONFIG = get_config()
_PRODUCER = Producer({'bootstrap.servers': _CONFIG['kafka_server']})


def main():
    """
    Main consumer of Kafka messages from workspace updates, generating new indexes.
    """
    topics = [
        _CONFIG['topics']['workspace_events'],
        _CONFIG['topics']['indexer_admin_events']
    ]
    for msg_data in kafka_consumer(topics):
        threadify(_process_event, [msg_data])


def _process_event(msg_data):
    """
    Process a new workspace event. This is the main switchboard for handling
    new workspace events. Dispatches to functions in the `event_type_handlers`
    dict below.
    Args:
        msg_data - json data received in the kafka event
    Valid events for msg_data['evtype'] include:
        NEW_VERSION - a new version has been created for an existing object
        NEW_ALL_VERSIONS - a brand new object is created
        PUBLISH - object is made public
        DELETE_* - deletion on an object
        COPY_ACCESS_GROUP - index all objects in the workspace
        RENAME_ALL_VERSIONS - rename all versions of an object
    Admin events:
        REINDEX - reindex an object by workspace and object id
        REINDEX_MISSING - reindex an object only if it does not already have a document in ES
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
    event_type_handlers[event_type](msg_data)
    print(f"Handler finished for event {msg_data['evtype']}")


def _run_indexer(msg_data):
    """
    Run the indexer for a workspace event message and produce an event for it.
    This will be threaded and backgrounded.
    """
    # index_obj returns a generator
    result_gen = index_obj(msg_data)
    for result in result_gen:
        if not result:
            sys.stderr.write(f"Unable to index object: {msg_data}.\n")
            continue
        # Produce an event in Kafka to save the index to elasticsearch
        print('producing to', _CONFIG['topics']['elasticsearch_updates'])
        _PRODUCER.produce(
            _CONFIG['topics']['elasticsearch_updates'],
            json.dumps(result),
            'index',
            callback=_delivery_report
        )
        _PRODUCER.poll(60)


def _run_obj_deleter(msg_data):
    """
    checks that object that is received is deleted because the workspace object
    delete event can refer to delete or undelete state changes.

    NOTE: Gavin said there should be a check when this message is received,
          that the state of the object we're querying actually is deleted.
    """
    # result = delete_obj(msg_data)
    # verify that object is deleted
    wsid = msg_data['wsid']
    objid = msg_data['objid']
    if not check_object_deleted(wsid, objid):
        print(f'object {objid} in workspace {wsid} not deleted')
        return
    result = {
        'index': '_all',
        'id': f"{msg_data['wsid']}:{msg_data['objid']}"
    }
    print('producing to', _CONFIG['topics']['elasticsearch_updates'])
    _PRODUCER.produce(
        _CONFIG['topics']['elasticsearch_updates'],
        json.dumps(result),
        'delete',
        callback=_delivery_report
    )
    _PRODUCER.poll(60)


def _run_workspace_deleter(msg_data):
    """
    checks that workspace that is received is deleted because the workspace
    delete event can refer to delete or undelete state changes.

    NOTE: Gavin said there should be a check when this message is received,
          that the state of the object we're querying actually is deleted.
    """
    # 1.) Verify that this workspace is actually deleted
    # 2.) Send workspace_id as 'id' field to 'elasticsearch_updates' topic
    # NOTE: not sure if '_all' works here.
    wsid = msg_data['wsid']
    if not check_workspace_deleted(wsid):
        print(f'workspace {wsid} not deleted')
        return
    result = {
        'index': '_all',
        'id': f"{wsid}"  # not sure if we want to include ':' to end here
    }
    print('producing to', _CONFIG['topics']['elasticsearch_updates'])
    _PRODUCER.produce(
        _CONFIG['topics']['elasticsearch_updates'],
        json.dumps(result),
        'delete_workspace',
        callback=_delivery_report
    )
    _PRODUCER.poll(60)


def _clone_workspace(msg_data):
    """
    Handles CLONE_WORKSPACE event

    iterate over each object in a given workspace and index it.
    """
    workspace_data = fetch_objects_in_workspace(msg_data['wsid'], include_narrative=True)
    for obj in workspace_data:
        dummy_msg_data = {
            "wsid": msg_data["wsid"],
            "objid": obj["obj_id"],
        }
        # NOTE: for now we run in same thread, but in future we may switch this to
        #       producing a message to the indexer admin kafka topic
        _run_indexer(dummy_msg_data)


def _set_global_permission(msg_data):
    """
    Handles SET_GLOBAL_PERMISSION event
    """
    wsid = msg_data['wsid']
    objid = msg_data['objid']
    index_name = get_indexer_name(msg_data)

    result = {
        'id': f"{wsid}:{objid}",
        'index': index_name  # need to find the index
    }
    _PRODUCER.produce(
        _CONFIG['topics']['elasticsearch_updates'],
        json.dumps(result),
        'permission',
        callback=_delivery_report
    )
    _PRODUCER.poll(60)


# Handler functions for each event type ('evtype' key)
workspace_event_type_handlers = {
    'NEW_VERSION': _run_indexer,
    'REINDEX': _run_indexer,
    'OBJECT_DELETE_STATE_CHANGE': _run_obj_deleter,
    'WORKSPACE_DELETE_STATE_CHANGE': _run_workspace_deleter,
    'COPY_OBJECT': _run_indexer,
    'RENAME_OBJECT': _run_indexer,
    'CLONE_WORKSPACE': _clone_workspace,
    'SET_GLOBAL_PERMISSION': _set_global_permission,
}

event_type_handlers = {
    **workspace_event_type_handlers
}


def _delivery_report(err, msg):
    """Kafka producer callback."""
    if err is not None:
        sys.stderr.write(f'Message delivery failed for "{msg.key()}" in {msg.topic()}: {err}\n')
    else:
        print(f'Message "{msg.key()}" delivered to {msg.topic()}')
