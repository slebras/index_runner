"""
Convenience wrapper / generator function arounda kafka consumer for a given
topic/client group.
"""
import json
from confluent_kafka import Consumer, KafkaError

from .config import get_config


def kafka_consumer(topics):
    """Generator of kafka messages for a given set of topics."""
    config = get_config()
    consumer = Consumer({
        'bootstrap.servers': config['kafka_server'],
        'group.id': config['kafka_clientgroup'],
        'auto.offset.reset': 'earliest'
    })
    print('Subscribing to topics:', topics)
    consumer.subscribe(topics)
    while True:
        msg = consumer.poll(0.5)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                raise RuntimeError(f"Kafka message error: {msg.error()}")
        val = msg.value().decode('utf-8')
        try:
            data = json.loads(val)
            yield data
        except ValueError as err:
            print(f'JSON parsing error: {err}')
            print(f'Message content: {val}')
    consumer.close()
