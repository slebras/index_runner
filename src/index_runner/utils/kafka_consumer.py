"""
Convenience wrapper / generator function arounda kafka consumer for a given
topic/client group.
"""
import json
from confluent_kafka import Consumer, KafkaError

from .config import get_config


def kafka_consumer(topics):
    """
    Generator of kafka messages for a given set of topics.
    """
    config = get_config()
    consumer = Consumer({
        'bootstrap.servers': config['kafka_server'],
        'group.id': config['kafka_clientgroup'],
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe(topics)
    print(f"Listening to {topics} in group {config['kafka_clientgroup']}")  # noqa
    while True:
        msg = consumer.poll(0.5)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print("Reached end of the stream.")
            else:
                print(f"Error: {msg.error()}")
            continue
        try:
            data = json.loads(msg.value().decode('utf-8'))
        except ValueError as err:
            # JSON parsing error
            print(f'JSON message error: {err}')
            continue
        print(f"New message in {topics} with data '{data}'")
        yield data
    consumer.close()
