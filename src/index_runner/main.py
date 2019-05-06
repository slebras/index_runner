"""
Main daemon runner, launching child threads that consume and produce messages
on Kafka.
"""
import time

from . import workspace_consumer
from .utils.threadify import threadify

if __name__ == '__main__':
    print('Starting consumer threads..')
    # A list of threads, saving their functions and arguments
    thread = threadify(workspace_consumer.main, [])
    # Parent process event loop that checks our threads.
    # If a thread dies, we restart it.
    while True:
        if not thread.is_alive():
            print('Thread died, restarting...')
            thread = threadify(workspace_consumer.main, [])
        time.sleep(10)
