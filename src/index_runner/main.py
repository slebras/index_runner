"""
Main daemon runner, launching child threads that consume and produce messages
on Kafka.
"""
import time
import queue

from . import workspace_consumer
from . import es_writer
from .utils.threadify import threadify

if __name__ == '__main__':
    es_queue = queue.Queue()  # type: queue.Queue
    print('Starting consumer threads..')
    # A list of threads, saving their functions and arguments
    threads = [
        {
            'fn': workspace_consumer.main,
            'args': [es_queue],
            'thread': threadify(workspace_consumer.main, [es_queue])
        },
        {
            'fn': es_writer.main,
            'args': [es_queue],
            'thread': threadify(es_writer.main, [es_queue])
        }
    ]
    # Parent process event loop that checks our threads.
    # If a thread dies, we restart it.
    while True:
        for t in threads:
            if not t['thread'].is_alive():
                print(f'Thread for {t["fn"]} died, restarting..')
                t['thread'] = threadify(t['fn'], t['args'])
        time.sleep(10)
