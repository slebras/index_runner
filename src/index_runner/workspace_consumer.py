"""
Consume workspace update events from kafka and publish new indexes.
"""
import sys
import json
from confluent_kafka import Producer

from .utils.kafka_consumer import kafka_consumer
from .utils.config import get_config
from .utils.threadify import threadify
from .indexers.main import index_obj

_CONFIG = get_config()
_PRODUCER = Producer({'bootstrap.servers': _CONFIG['kafka_server']})


def main():
    """
    Main consumer of Kafka messages from workspace updates, generating new indexes.
    """
    topics = [_CONFIG['topics']['workspace_events']]
    for msg_data in kafka_consumer(topics):
        threadify(_process_event, [msg_data])


def _process_event(msg_data):
    """
    Process a new workspace event. This is the main switchboard for handling
    new workspace events. Dispatches to modules in ./event_handlers

    Args:
        msg_data - json data received in the kafka event
    Valid events for msg_data['evtype'] include:
        NEW_VERSION - a new version has been created for an existing object
        NEW_ALL_VERSIONS - a brand new object is created
        PUBLISH - object is made public
        DELETE_* - deletion on an object
        COPY_ACCESS_GROUP - index all objects in the workspace
        RENAME_ALL_VERSIONS - rename all versions of an object
        REINDEX_WORKSPACE - index all objects in the workspace
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
            return
        # Produce an event in Kafka to save the index to elasticsearch
        print('producing to', _CONFIG['topics']['elasticsearch_updates'])
        _PRODUCER.produce(
            _CONFIG['topics']['elasticsearch_updates'],
            json.dumps(result),
            'index',
            callback=_delivery_report
        )
        _PRODUCER.poll(60)


# Handler functions for each event type ('evtype' key)
event_type_handlers = {
    'NEW_VERSION': _run_indexer
}


def _delivery_report(err, msg):
    """Kafka producer callback."""
    if err is not None:
        sys.stderr.write(f'Message delivery failed for "{msg.key()}" in {msg.topic()}: {err}\n')
    else:
        print(f'Message "{msg.key()}" delivered to {msg.topic()}')
