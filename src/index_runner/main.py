"""
Main daemon runner, launching child threads that consume and produce messages
on Kafka.
"""
import time

from . import workspace_consumer, elasticsearch_consumer
from .utils.threadify import threadify
from .utils.set_up_indexes import set_up_indexes

if __name__ == '__main__':
    # create the indexes for elasticsearch

    set_up_indexes()
    print('Starting consumer threads..')
    # A list of threads, saving their functions and arguments
    threads = [
        {'func': workspace_consumer.main, 'thread': threadify(workspace_consumer.main, [])},
        {'func': elasticsearch_consumer.main, 'thread': threadify(elasticsearch_consumer.main, [])}
    ]
    # Parent process event loop that checks our threads.
    # If a thread dies, we restart it.
    while True:
        for thread in threads:
            if not thread['thread'].is_alive():
                print('Thread died, restarting..')
                thread['thread'] = threadify(thread['func'], thread.get('args', []))
        time.sleep(10)
