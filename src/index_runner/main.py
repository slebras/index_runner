"""
This is the entrypoint for running the app. A parent supervisor process that
launches and monitors child processes.

Architecture:
    Nodes:
        - index_runner -- consumes workspace and admin indexing events from kafka, runs indexers.
        - es_writer -- receives updates from index_runner and bulk-updates elasticsearch.
    The index_runner and es_writer run in separate workers with message queues in between.
"""
import json
import time
import requests
from confluent_kafka import Consumer, KafkaError

from src.utils.config import config
from src.utils.worker_group import WorkerGroup
from src.index_runner.es_indexer import ESIndexer
from src.index_runner.releng_importer import RelengImporter
from src.utils.service_utils import wait_for_dependencies


def main():
    """
    - Multiple processes run Kafka consumers under the same topic and client group
    - Each Kafka consumer pushes work to one or more es_indexers or releng_importers

    Work is sent from the Kafka consumer to the es_writer or releng_importer via ZMQ sockets.
    """
    # Wait for dependency services (ES and RE) to be live
    wait_for_dependencies(timeout=180)
    print('Services started! Now starting the app..')
    # Initialize worker group of ESIndexer
    es_indexers = WorkerGroup(ESIndexer, (), count=config()['workers']['num_es_indexers'])
    # Initialize a worker group of RelengImporter
    releng_importers = WorkerGroup(RelengImporter, (), count=config()['workers']['num_re_importers'])
    # All worker groups to send kafka messages to
    receivers = [es_indexers, releng_importers]

    # used to check update every minute
    last_updated_minute = int(time.time()/60)
    _CONFIG_TAG = _query_for_config_tag()

    # Initialize and run the Kafka consumer
    consumer = _set_consumer()

    while True:
        msg = consumer.poll(timeout=0.5)
        if msg is None:
            continue
        curr_min = int(time.time()/60)
        if curr_min > last_updated_minute:
            config_tag = _query_for_config_tag()
            if config_tag != _CONFIG_TAG:
                _CONFIG_TAG = config_tag
                last_updated_minute = curr_min
                # send message to es_indexers to update config.
                es_indexers.queue.put(('ws_event', {
                    'evtype': "RELOAD_ELASTIC_ALIASES",
                    "msg": f"updating to tag {_CONFIG_TAG}"
                }))
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


def _query_for_config_tag():
    """using github release api (https://developer.github.com/v3/repos/releases/) find
    out if there is new version of the config."""
    github_release_url = config()['github_release_url']
    resp = requests.get(url=github_release_url)
    if not resp.ok:
        return None
        # raise RuntimeError("not able to get github config release")
    data = resp.json()
    return data['tag_name']


def _set_consumer():
    """"""
    consumer = Consumer({
        'bootstrap.servers': config()['kafka_server'],
        'group.id': config()['kafka_clientgroup'],
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    })
    topics = [
        config()['topics']['workspace_events'],
        config()['topics']['admin_events']
    ]
    print(f"Subscribing to: {topics}")
    print(f"Client group: {config()['kafka_clientgroup']}")
    print(f"Kafka server: {config()['kafka_server']}")
    consumer.subscribe(topics)
    return consumer


if __name__ == '__main__':
    print('before main..')
    main()
