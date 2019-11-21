"""
This is the entrypoint for running the app. A parent supervisor process that
launches and monitors child processes.

Architecture:
    Nodes:
        - index_runner -- consumes workspace and admin indexing events from kafka, runs indexers.
        - es_writer -- receives updates from index_runner and bulk-updates elasticsearch.
    The index_runner and es_writer run in separate workers with message queues in between.
"""
import time
import zmq
import zmq.devices

from .index_runner import IndexRunner
from .es_writer import ESWriter
from utils.config import get_config
from utils.worker_group import WorkerGroup

_CONFIG = get_config()


def main():
    """
    The zmq layout is:
        index_runner--┬--PULL-->Streamer--PUSH-->es_writer
        index_runner--┤
        index_runner--┤
        index_runner--╯
    """
    frontend_url = f'ipc:///tmp/{_CONFIG["zmq"]["socket_name"]}_front'
    backend_url = f'ipc:///tmp/{_CONFIG["zmq"]["socket_name"]}_back'
    streamer = zmq.devices.ProcessDevice(zmq.STREAMER, zmq.PULL, zmq.PUSH)
    streamer.bind_in(frontend_url)
    streamer.bind_out(backend_url)
    # The 'high water mark' options sets a maximum queue size for both the frontend and backend sockets.
    streamer.setsockopt_in(zmq.RCVHWM, _CONFIG['zmq']['queue_max'])
    streamer.setsockopt_out(zmq.SNDHWM, _CONFIG['zmq']['queue_max'])
    streamer.start()
    # Start the index_runner and es_writer
    # For some reason, mypy does not figure out these types correctly
    indexers = WorkerGroup(IndexRunner, (frontend_url,), count=_CONFIG['zmq']['num_indexers'])  # type: ignore
    writer = WorkerGroup(ESWriter, (backend_url,), count=_CONFIG['zmq']['num_es_writers'])  # type: ignore
    # Signal that the app has started to any other processes
    open('/tmp/app_started', 'a').close()  # nosec
    while True:
        # Monitor workers and restart any that have crashed
        indexers.health_check()
        writer.health_check()
        time.sleep(5)


if __name__ == '__main__':
    main()
