'''
The event loop for the index runner.
'''
from confluent_kafka import Consumer, KafkaError
from typing import Callable, Dict, Any
import json
import logging
import time
import traceback

from src.utils.config import config

Message = Dict[str, Any]

# TODO TEST unit tests


def start_loop(
        consumer: Consumer,
        message_handler: Callable[[Message], None],
        on_success: Callable[[Message], None] = lambda msg: None,
        on_failure: Callable[[Message, Exception], None] = lambda msg, e: None,
        on_config_update: Callable[[], None] = lambda: None,
        logger: logging.Logger = logging.getLogger('IR'),
        return_on_empty: bool = False,
        timeout: float = 0.5):
    """
    Run the indexer event loop.

    Args:
        consumer: A Kafka consumer which will be polled for messages.
        message_handler: a processor for messages from Kafka.
        on_success: called after message_handler has returned sucessfully and the message
            offset has been committed to Kafka. A noop by default.
        on_failure: called if the message_handler, the Kafka commit, or on_success throws an
            exception. A noop by default.
        on_config_update: called when the configuration has been updated.
        logger: a logger to use for logging events. By default a standard logger for 'IR'.
        return_on_empty: stop the loop when we receive an empty message. Helps with testing.
        timeout: how long to wait polling for the next message
    """
    # Used for re-fetching the configuration with a throttle
    last_updated_minute = int(time.time() / 60)
    # Failure count for the current offset
    fail_count = 0
    while True:
        msg = consumer.poll(timeout=timeout)
        if msg is None:
            logger.info('Message empty')
            if return_on_empty:
                return
            continue
        curr_min = int(time.time() / 60)
        if curr_min > last_updated_minute:
            # Reload the configuration
            config(force_reload=True)
            last_updated_minute = curr_min
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                logger.info('End of stream.')
            else:
                logger.error(f"Kafka message error: {msg.error()}")
            continue
        val = msg.value().decode('utf-8')
        try:
            val_json = json.loads(val)
        except ValueError as err:
            logger.error(f'JSON parsing error: {err}')
            logger.error(f'Message content: {val}')
            consumer.commit(msg)
            continue
        logger.info(f'Received event: {val_json}')
        start = time.time()
        try:
            message_handler(val_json)
        except Exception as err:
            logger.error(f'Error processing message: {err.__class__.__name__} {err}')
            logger.error(traceback.format_exc())
            # Save this error and message to a topic in Elasticsearch
            on_failure(val_json, err)
            fail_count += 1
            logger.info(f"We've had {fail_count} failures so far")
            if fail_count >= config()['max_handler_failures']:
                logger.info(f"Reached max failure count of {fail_count}. Moving on.")
                consumer.commit(msg)
                fail_count = 0
            continue
        # Move the offset for our partition
        consumer.commit(msg)
        on_success(val_json)
        fail_count = 0
        logger.info(f"Handled {val_json['evtype']} message in {time.time() - start}s")
