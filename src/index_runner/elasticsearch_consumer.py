"""
Consume elasticsearch save events from kafka.
"""
import json
from confluent_kafka import Producer
import requests

from .utils.kafka_consumer import kafka_consumer
from .utils.config import get_config
from .utils.threadify import threadify

config = get_config()
producer = Producer({'bootstrap.servers': config['kafka_server']})

es_host = config.get('elasticsearch_host')
es_port = config.get('elasticsearch_port')

es_data_type = config.get("elasticsearch_data_type")

es_url = "http://" + es_host + ":" + str(es_port)
# es = elasticsearch.Elasticsearch([{'host': es_host, 'port': es_port}])

headers = {
    "Content-Type": "application/json"
}


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
    if not msg_data.get('doc'):
        raise RuntimeError("Message to Elasticsearch malformed, does not contain \'doc\'")
    if not msg_data.get('id'):
        raise RuntimeError("Message to Elasticsearch malformed, does not contain \'id\' field")
    if not msg_data.get('index'):
        raise RuntimeError("Message to Elasticsearch malformed, does not have target elasticsearch index")


def _save_to_elastic(msg_data):
    """
    Save the indexed doc to elasticsearch.
    msg_data:
        {
            'doc': elasticsearch index document
                   - json like object
            'mapping': elasticsearch type mapping schema thing
        }
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
            '/'.join([es_url, msg_data['index'], es_data_type, msg_data['id']]),
            data=json.dumps(msg_data['doc']),
            headers=headers
        )
    except requests.exceptions.RequestException as error:
        # log the error
        msg_data['error'] = str(error)
        producer.produce(
            config['topics']['error_logs'],
            json.dumps(msg_data),
            callback=_delivery_report
        )
        producer.poll(60)
        raise error
    if not (resp.status_code == requests.codes.ok):
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
