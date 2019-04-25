"""
Consume elasticsearch save events from kafka.
"""
import json
from confluent_kafka import Producer
import requests

from .utils.kafka_consumer import kafka_consumer
from .utils.config import get_config
from .utils.threadify import threadify

_CONFIG = get_config()
_PRODUCER = Producer({'bootstrap.servers': _CONFIG['kafka_server']})
_ES_HOST = _CONFIG['elasticsearch_host']
_ES_PORT = _CONFIG['elasticsearch_port']
_ES_DATA_TYPE = _CONFIG["elasticsearch_data_type"]
_ES_URL = "http://" + _ES_HOST + ":" + str(_ES_PORT)
_HEADERS = {"Content-Type": "application/json"}


def main():
    """
    Main event loop for consuming messages from Kafka and saving to elasticsearch.
    """
    topics = [_CONFIG['topics']['elasticsearch_updates']]
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
    """
    try:
        _validate_message(msg_data)
    except RuntimeError as error:
        # log the error
        msg_data['error'] = str(error)
        _PRODUCER.produce(
            _CONFIG['topics']['error_logs'],
            json.dumps(msg_data),
            callback=_delivery_report
        )
        _PRODUCER.poll(60)
        raise error
    try:
        # save to elasticsearch index
        resp = requests.put(
            '/'.join([_ES_URL, msg_data['index'], _ES_DATA_TYPE, msg_data['id']]),
            data=json.dumps(msg_data['doc']),
            headers=_HEADERS
        )
    except requests.exceptions.RequestException as error:
        raise error
    if not resp.ok:
        # unsuccesful save to elasticsearch.
        raise RuntimeError("Error when saving to elasticsearch index %s: " % msg_data['index'] + resp.text +
                           ". Exited with status code %i" % resp.status_code)
    # log the message if we are succesful.
    _PRODUCER.produce(
        _CONFIG['topics']['indexer_logs'],
        json.dumps(msg_data),
        callback=_delivery_report
    )
    _PRODUCER.poll(60)


def _delivery_report(err, msg):
    """
    Kafka producer callback.
    """
    # TODO file logger
    if err is not None:
        print(f'Message delivery failed on {msg.topic()}: {err}')
    else:
        print(f'Message delivered to {msg.topic()}')
