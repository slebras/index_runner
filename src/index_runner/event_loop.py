'''
The event loop for the index runner.
'''

import json
import logging
import requests
import time
import traceback

from typing import Callable, Dict, Any

from confluent_kafka import Consumer, KafkaError

from src.utils.config import config

Message = Dict[str, Any]

# TODO TEST unit tests


def start_loop(
        consumer: Consumer,
        message_handler: Callable[[Message], None],
        on_success: Callable[[Message], None] = lambda msg: None,
        on_failure: Callable[[Message, Exception], None] = lambda msg, e: None,
        on_config_update: Callable[[], None] = lambda: None,
        logger: logging.Logger = logging.getLogger('IR')):
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
    """
    # Used for re-fetching the configuration with a throttle
    last_updated_minute = int(time.time() / 60)
    if not config()['global_config_url']:
        config_tag = _fetch_latest_config_tag()

    while True:
        msg = consumer.poll(timeout=0.5)
        if msg is None:
            continue
        curr_min = int(time.time() / 60)
        if not config()['global_config_url'] and curr_min > last_updated_minute:
            # Check for configuration updates
            latest_config_tag = _fetch_latest_config_tag()
            last_updated_minute = curr_min
            if config_tag is not None and latest_config_tag != config_tag:
                config(force_reload=True)
                config_tag = latest_config_tag
                on_config_update()
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                logger.info('End of stream.')
            else:
                logger.error(f"Kafka message error: {msg.error()}")
            continue
        val = msg.value().decode('utf-8')
        try:
            msg = json.loads(val)
        except ValueError as err:
            logger.error(f'JSON parsing error: {err}')
            logger.error(f'Message content: {val}')
            consumer.commit()
            continue
        logger.info(f'Received event: {msg}')
        start = time.time()
        try:
            message_handler(msg)
            # Move the offset for our partition
            consumer.commit()
            on_success(msg)
            logger.info(f"Handled {msg['evtype']} message in {time.time() - start}s")
        except Exception as err:
            logger.error(f'Error processing message: {err.__class__.__name__} {err}')
            logger.error(traceback.format_exc())
            # Save this error and message to a topic in Elasticsearch
            on_failure(msg, err)


# might make sense to move this into the config module
def _fetch_latest_config_tag():
    """
    Using the Github release API, check for a new version of the config.
    https://developer.github.com/v3/repos/releases/
    """
    github_release_url = config()['github_release_url']
    if config()['github_token']:
        headers = {'Authorization': f"token {config()['github_token']}"}
    else:
        headers = {}
    try:
        resp = requests.get(url=github_release_url, headers=headers)
    except Exception as err:
        logging.error(f"Unable to fetch indexer config from github: {err}")
        # Ignore any error and continue; try the fetch again later
        return None
    if not resp.ok:
        logging.error(f"Unable to fetch indexer config from github: {resp.text}")
        return None
    data = resp.json()
    return data['tag_name']
