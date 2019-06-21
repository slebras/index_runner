"""
This is the entrypoint for running the app. A parent supervisor process that
launches and monitors child processes and threads.

Architecture:
    Nodes:
        - index_runner -- consumes workspace and admin indexing events from kafka, runs indexers.
        - es_writer -- receives updates from index_runner and bulk-updates elasticsearch.
    The index_runner and es_writer run in separate threads with a thread queue between them.

Everything in this app is IO bound, so we use threads everywhere, not processes.
"""
import time
import zmq
import zmq.devices
import requests
import subprocess  # nosec

from .index_runner import IndexRunner
from .es_writer import ESWriter
from .utils.config import get_config
from .utils.thread_group import ThreadGroup

_CONFIG = get_config()


def main():
    """
    The zmq layout is:
        index_runner--┬--PULL-->Streamer--PUSH-->es_writer
        index_runner--┤
        index_runner--┤
        index_runner--╯
    """
    _wait_for_services()
    frontend_url = f'inproc://{_CONFIG["zmq"]["socket_name"]}_front'
    backend_url = f'inproc://{_CONFIG["zmq"]["socket_name"]}_back'
    streamer = zmq.devices.ThreadDevice(zmq.STREAMER, zmq.PULL, zmq.PUSH)
    streamer.bind_in(frontend_url)
    streamer.bind_out(backend_url)
    streamer.setsockopt_in(zmq.IDENTITY, b'PULL')
    streamer.setsockopt_out(zmq.IDENTITY, b'PUSH')
    streamer.start()
    # Start the index_runner and es_writer
    # For some reason, mypy does not figure out these types correctly
    indexers = ThreadGroup(IndexRunner, (frontend_url,), count=_CONFIG['zmq']['num_indexers'])  # type: ignore
    writer = ThreadGroup(ESWriter, (backend_url,), count=1)  # type: ignore
    # Signal that the app has started to any other processes
    open('/tmp/app_started', 'a').close()  # nosec
    while True:
        # Monitor processes/threads and restart any that have crashed
        indexers.health_check()
        writer.health_check()
        time.sleep(5)


def _wait_for_services():
    """Block and wait for service dependencies (Kafka and Elasticsearch) with a timeout."""
    timeout = 180  # in seconds
    start_time = int(time.time())
    es_started = False
    kafka_started = False
    while not es_started:
        # Check for Elasticsearch
        try:
            requests.get(_CONFIG['elasticsearch_url']).raise_for_status()
            es_started = True
        except Exception:
            print('Unable to connect to elasticsearch, waiting..')
            time.sleep(5)
            if (int(time.time()) - start_time) > timeout:
                raise RuntimeError(f"Failed to connect to other services in {timeout}s")
    while not kafka_started:
        try:
            subprocess.check_output(["/usr/bin/kafkacat", "-L", "-b", _CONFIG['kafka_server']])  # nosec
            kafka_started = True
        except subprocess.CalledProcessError:
            print('Unable to connect to Kafka, waiting..')
            time.sleep(5)
            if (int(time.time()) - start_time) > timeout:
                raise RuntimeError(f"Failed to connect to other services in {timeout}s")
    print('Services started! Now starting the app..')


if __name__ == '__main__':
    main()
