"""
Convenience wrapper / generator function around a kafka consumer for a given
topic/client group.
"""
import json
from confluent_kafka import Consumer, KafkaError

from src.utils.config import get_config


def kafka_consumer(topics):
    """Generator of kafka messages for a given set of topics."""
    config = get_config()
    consumer = Consumer({
        'bootstrap.servers': config['kafka_server'],
        'group.id': config['kafka_clientgroup'],
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    })
    print(f"Subscribing to: {topics}")
    print(f"Client group: {config['kafka_clientgroup']}")
    print(f"Kafka server: {config['kafka_server']}")
    consumer.subscribe(topics)
    try:
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
                yield data
            except ValueError as err:
                print(f'JSON parsing error: {err}')
                print(f'Message content: {val}')
    finally:
        consumer.close()
