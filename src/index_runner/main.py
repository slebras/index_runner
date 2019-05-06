"""
Main daemon runner, launching child threads that consume and produce messages
on Kafka.
"""
import time

from . import workspace_consumer
from .utils.threadify import threadify
from .utils.set_up_indexes import set_up_indexes

if __name__ == '__main__':
    # Create indexes for elasticsearch
    set_up_indexes()
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
