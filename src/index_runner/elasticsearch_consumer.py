"""
Consume elasticsearch save events from kafka.
"""
import json
from confluent_kafka import Producer
from elasticsearch import Elasticsearch

from .utils.kafka_consumer import kafka_consumer
from .utils.config import get_config
from .utils.threadify import threadify

config = get_config()
producer = Producer({'bootstrap.servers': config['kafka_server']})

es_host = config.get('elasticsearch_host')
es_port = config.get('elasticsearch_port')

es = Elasticsearch([{'host': es_host, 'port':es_port}])

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
        raise RuntimeError("Message to ElasticSearch malformed, does not contain \'doc\'")
    if not msg_data.get('mapping'):
        raise RuntimeError("Message to ElasticSearch malformed, does not contain \'mapping\'")
    if not msg_data['doc'].get('upa'):
        raise RuntimeError("Message to ElasticSearch malformed, does not contain UPA")

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
        producer.produce(
            config['topics']['indexer_logs'],
            json.dumps(msg_data),
            callback=_delivery_report
        )
        producer.poll(60)
    except e:
        # log the error
        producer.produce(
            config['topics']['error_logs'],
            json.dumps(msg_data),
            callback=_delivery_report
        )
        producer.poll(60)

    es.index(
        doc_type=msg_data['mapping'],
        index=msg_data['index'],
        body=msg_data['doc'],
        id=msg_data['doc']['upa']
    )



def _delivery_report(err, msg):
    """
    Kafka producer callback.
    """
    # TODO file logger
    if err is not None:
        print(f'Message delivery failed on {msg.topic()}: {err}')
    else:
        print(f'Message delivered to {msg.topic()}')
