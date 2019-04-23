"""
Consume elasticsearch save events from kafka.
"""
import json
from confluent_kafka import Producer
from elasticsearch import Elasticsearch
import requests

from .utils.kafka_consumer import kafka_consumer
from .utils.config import get_config
from .utils.threadify import threadify

config = get_config()
producer = Producer({'bootstrap.servers': config['kafka_server']})

es_host = config['elasticsearch_host']
es_port = config['elasticsearch_port']

_ES_DATA_TYPE = config["elasticsearch_data_type"]

es_url = "http://" + es_host + ":" + str(es_port)
# es = elasticsearch.Elasticsearch([{'host': es_host, 'port': es_port}])

headers = {
    "Content-Type": "application/json"
}

es = Elasticsearch([{'host': es_host, 'port': es_port}])


def main():
    """
    Main event loop for consuming messages from Kafka and saving to elasticsearch.
    """
    topics = [config['topics']['elasticsearch_updates']]
    for msg_data in kafka_consumer(topics):
        threadify(_save_to_elastic, [msg_data])


def _validate_message(msg_data):
    """
    validate the input message
    """
    required_keys = ['doc', 'id', 'index']
    for key in required_keys:
        if not msg_data.get(key):
            raise RuntimeError(f"Message to Elasticsearch malformed, does not contain \'{key}\' field")


def _save_to_elastic(msg_data):
    """
    Save the indexed doc to elasticsearch.
    msg_data is a python dict of:
        doc - elasticsearch index document (json like object)
        mapping - elasticsearch type mapping
    """
    try:
        _validate_message(msg_data)
    except RuntimeError as error:
        # log the error
        msg_data['error'] = str(error)
        producer.produce(
            config['topics']['error_logs'],
            json.dumps(msg_data),
            callback=_delivery_report
        )
        producer.poll(60)
        raise error
    try:
        # save to elasticsearch index
        resp = requests.put(
            '/'.join([es_url, msg_data['index'], _ES_DATA_TYPE, msg_data['id']]),
            data=json.dumps(msg_data['doc']),
            headers=headers
        )
    except requests.exceptions.RequestException as error:
        raise error
    es.index(
        doc_type=msg_data['mapping'],
        index=msg_data['index'],
        body=msg_data['doc'],
        id=msg_data['doc']['upa']
    )
    if not resp.ok:
        # unsuccesful save to elasticsearch.
        raise RuntimeError("Error when saving to elasticsearch index %s: " % msg_data['index'] + resp.text +
                           ". Exited with status code %i" % resp.status_code)
    # log the message if we are succesful.
    producer.produce(
        config['topics']['indexer_logs'],
        json.dumps(msg_data),
        callback=_delivery_report
    )
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
