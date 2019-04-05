"""
Main index runner daemon.
"""
import traceback
import json
from threading import Thread
from confluent_kafka import Consumer, Producer, KafkaError

from .utils.config import get_config
from .indexers.main import index_obj
# from .event_handlers.new_object_version import new_object_version


config = get_config()
producer = Producer({'bootstrap.servers': config['kafka_server']})


def main():
    """
    Main event loop for consuming messages from Kafka and generating indexes from workspace events.
    """
    consumer = Consumer({
        'bootstrap.servers': config['kafka_server'],
        'group.id': config['kafka_clientgroup'],
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([config['topics']['workspace_events']])
    print(f"Listening to {config['kafka_server']} in group {config['kafka_clientgroup']}")  # noqa
    while True:
        msg = consumer.poll(0.5)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print("Reached end of the stream.")
            else:
                print("Error:", msg.error())
            continue
        print('New message:', msg.value())
        try:
            data = json.loads(msg.value().decode('utf-8'))
        except ValueError as err:
            # JSON parsing error
            print('JSON message error:', err)
            continue
        try:
            _process_event(data, producer)
        except Exception:
            traceback.print_exc()
    consumer.close()


def _process_event(event_data, producer):
    """
    Process a new workspace event. This is the main switchboard for handling
    new workspace events. Dispatches to modules in ./event_handlers

    Args:
        event_data - json data received in the kafka event
    Valid events for event_data['evtype'] include:
        NEW_VERSION - a new version has been created for an existing object
        NEW_ALL_VERSIONS - a brand new object is created
        PUBLISH - object is made public
        DELETE_* - deletion on an object
        COPY_ACCESS_GROUP - index all objects in the workspace
        RENAME_ALL_VERSIONS - rename all versions of an object
        REINDEX_WORKSPACE - index all objects in the workspace
    """
    # Workspace events reference:
    # https://github.com/kbase/workspace_deluxe/blob/8a52097748ef31b94cdf1105766e2c35108f4c41/src/us/kbase/workspace/modules/SearchPrototypeEventHandlerFactory.java#L58  # noqa
    event_type = event_data.get('evtype')
    ws_id = event_data.get('wsid')
    if not ws_id:
        raise RuntimeError(f'Invalid wsid in event: {ws_id}')
    if not event_type:
        raise RuntimeError(f"Missing 'evtype' in event: {event_data}")
    if event_type == 'NEW_VERSION':
        print(f"New object verson event for workspace {ws_id}")
        # Generate the index and mapping for the new object
        _threadify(_run_indexer, [index_obj, event_data])
    elif event_type == 'PUBLISH':
        # publish(event_data['wsid'])
        pass
    elif event_type.startswith('DELETE_'):
        # delete(evt)
        pass
    elif event_type == 'COPY_ACCESS_GROUP':
        # _index_workspace(ws)
        pass
    elif event_type == 'RENAME_ALL_VERSIONS':
        raise NotImplementedError('RENAME_ALL_VERSIONS not yet implemented')
    elif event_type in ['REINDEX_WORKSPACE']:
        # Pseudo event
        # _index_workspace(ws)
        pass
    else:
        # TODO Kafka logger
        raise RuntimeError(f"Invalid workspace event: {event_type}")
    print(f"Handler finished for event {event_data['evtype']}")


def _run_indexer(func, event_data):
    """
    Run the indexer for a workspace evnet and produce an event for it.
    This will be threaded and backgrounded.
    """
    result = index_obj(event_data)
    # Produce an event in Kafka to save the index to elasticsearch
    print('producing to', config['topics']['save_idx'])
    producer.produce(config['topics']['save_idx'], json.dumps(result), callback=_delivery_report)
    producer.poll(60)


def _delivery_report(err, msg):
    """
    Kafka producer callback.
    """
    # TODO file logger
    if err is not None:
        print(f'Message delivery failed on {msg.topic()}: {err}')
    else:
        print(f'Message delivered to {msg.topic()}')


def _threadify(func, args):
    """
    Standardized way to start a thread from the event processor.
    """
    t = Thread(target=func, args=args)
    t.daemon = True
    t.start()


if __name__ == '__main__':
    main()
