"""
This is the entrypoint for running the app. A parent supervisor process that
launches and monitors child processes.

Architecture:
    Nodes:
        - index_runner -- consumes workspace and admin indexing events from kafka, runs indexers.
        - es_writer -- receives updates from index_runner and bulk-updates elasticsearch.
    The index_runner and es_writer run in separate workers with message queues in between.
"""
import time
import requests
import json
from confluent_kafka import Consumer, KafkaError

from src.utils.config import get_config
from src.utils.worker_group import WorkerGroup
from src.index_runner.es_indexer import ESIndexer
from src.index_runner.releng_importer import RelengImporter

_CONFIG = get_config()
_RESET_CONFIG_ITERS = 100


def main():
    """
    - Multiple processes run Kafka consumers under the same topic and client group
    - Each Kafka consumer pushes work to one or more es_indexers or releng_importers

    Work is sent from the Kafka consumer to the es_writer or releng_importer via ZMQ sockets.
    """
    # Wait for dependency services (ES and RE) to be live
    _wait_for_dependencies()
    # Initialize worker group of ESIndexer
    es_indexers = WorkerGroup(ESIndexer, (), count=_CONFIG['zmq']['num_es_indexers'])
    # Initialize a worker group of RelengImporter
    releng_importers = WorkerGroup(RelengImporter, (), count=_CONFIG['zmq']['num_re_importers'])
    # All worker groups to send kafka messages to
    receivers = [es_indexers, releng_importers]

    # Initialize and run the Kafka consumer
    consumer = _set_consumer(_CONFIG)

    iters = 0
    while True:
        msg = consumer.poll(timeout=0.5)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print('End of stream.')
            else:
                print(f"Kafka message error: {msg.error()}")
            continue
        val = msg.value().decode('utf-8')
        try:
            data = json.loads(val)
        except ValueError as err:
            print(f'JSON parsing error: {err}')
            print(f'Message content: {val}')
        for receiver in receivers:
            receiver.queue.put(('ws_event', data))
        # reload config every few iterations?
        if iters % _RESET_CONFIG_ITERS == int(_RESET_CONFIG_ITERS - 1):
            consumer = _set_consumer(get_config())
            # avoid integer overflows
            iters = 0
        iters += 1


def _set_consumer(conf):
    """"""
    consumer = Consumer({
        'bootstrap.servers': conf['kafka_server'],
        'group.id': conf['kafka_clientgroup'],
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    })
    topics = [
        conf['topics']['workspace_events'],
        conf['topics']['admin_events']
    ]
    print(f"Subscribing to: {topics}")
    print(f"Client group: {conf['kafka_clientgroup']}")
    print(f"Kafka server: {conf['kafka_server']}")
    consumer.subscribe(topics)
    return consumer


def _wait_for_dependencies():
    """Block and wait for elasticsearch."""
    timeout = 180  # in seconds
    start_time = int(time.time())
    while True:
        # Check for Elasticsearch
        try:
            requests.get(_CONFIG['elasticsearch_url']).raise_for_status()
            requests.get(_CONFIG['re_api_url'] + '/').raise_for_status()
            break
        except Exception:
            print('Waiting for dependency services...')
            time.sleep(5)
            if (int(time.time()) - start_time) > timeout:
                raise RuntimeError(f"Failed to connect to other services in {timeout}s")
    print('Services started! Now starting the app..')


if __name__ == '__main__':
    print('before main..')
    main()
