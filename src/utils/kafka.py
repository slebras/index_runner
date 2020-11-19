'''
Helper methods for recieving and sending messages from and to Kafka.
'''

import json
import logging

from typing import List

from confluent_kafka import Consumer, Producer

from src.utils.config import config

logger = logging.getLogger('src.utils.kafka')

_KAFKA_PRODUCE_RETRIES = 5


def init_consumer(topics: List[str]) -> Consumer:
    """
    Initialize a Kafka consumer instance
    """
    consumer = Consumer({
        'bootstrap.servers': config()['kafka_server'],
        'group.id': config()['kafka_clientgroup'],
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    })
    logger.info(f"Subscribing to: {topics}")
    logger.info(f"Client group: {config()['kafka_clientgroup']}")
    logger.info(f"Kafka server: {config()['kafka_server']}")
    consumer.subscribe(topics)
    return consumer


def close_consumer(consumer: Consumer) -> None:
    """
    This will close the network connections and sockets. It will also trigger
    a rebalance immediately rather than wait for the group coordinator to
    discover that the consumer stopped sending heartbeats and is likely dead,
    which will take longer and therefore result in a longer period of time in
    which consumers can’t consume messages from a subset of the partitions.
    """
    consumer.close()
    logger.info("Closed the Kafka consumer")


def produce(data, topic=config()['topics']['admin_events'], callback=None) -> None:
    """
    Produce a new event message on a Kafka topic and block at most 60s for it to get published.

    :param data: the data to send to Kafka. Must be JSONable.
    :param topic: the topic where the data will be sent.
    :param callback: a callable provided to the confluent Kafka Producer class.
    """
    producer = Producer({'bootstrap.servers': config()['kafka_server']})
    tries = 0
    while True:
        try:
            producer.produce(topic, json.dumps(data), callback=callback)
            producer.flush()
            break
        except BufferError:
            if tries == _KAFKA_PRODUCE_RETRIES:
                raise RuntimeError("Unable to produce a Kafka message due to BufferError")
            logger.error("Received a BufferError trying to produce a message on Kafka. Retrying..")
            tries += 1
