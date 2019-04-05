"""
Consume elasticsearch save events from kafka.
"""
import json
from confluent_kafka import Producer

from .utils.kafka_consumer import kafka_consumer
from .utils.config import get_config
from .utils.threadify import threadify

config = get_config()
producer = Producer({'bootstrap.servers': config['kafka_server']})


def main():
    """
    Main event loop for consuming messages from Kafka and saving to elasticsearch.
    """
    topics = [config['topics']['elasticsearch_updates']]
    for msg_data in kafka_consumer(topics):
        threadify(_save_to_elastic, [msg_data])


def _save_to_elastic(msg_data):
    """
    Save the indexed doc to elasticsearch.
    """
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
