"""
Convenience wrapper / generator function arounda kafka consumer for a given
topic/client group.
"""
import json
import time
from confluent_kafka import Consumer, KafkaError

from .config import get_config


def check_timeout(start_time, timeout):
    elapsed = time.time() - start_time
    if elapsed > timeout:
        raise TimeoutError(f"Error: Consumer waited past timeout of {timeout} seconds")


def kafka_consumer(topics, timeout=60):
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
    start_time = time.time()
    while True:
        check_timeout(start_time, timeout)
        msg = consumer.poll(0.5)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print("Reached end of the stream.")
            else:
                print("Error:", msg.error())
            continue
        print(f'New message in {topics}: {msg.value()}')
        try:
            data = json.loads(msg.value().decode('utf-8'))
        except ValueError as err:
            # JSON parsing error
            print('JSON message error:', err)
            continue
        # reset start time everytime we yield something
        start_time = time.time()
        yield data
    consumer.close()
