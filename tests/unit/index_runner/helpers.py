from typing import List
import queue
import time


class MockMessage:
    """Mock Kafka message returned in the mock consumer"""

    def __init__(self, msg: str = '{}'):
        self.msg = msg

    def error(self):
        return None

    def value(self):
        return self.msg.encode()


class MockConsumer:
    """Mock Kafka consumer for testing the event loop without needing Kafka itself."""

    def __init__(self, topics: List[str]):
        self.msg_queue = queue.Queue()
        # Track the last message polled
        self.msg_popped = None

    def poll(self, timeout: float):
        if self.msg_popped is not None:
            # Last message not committed; resend it
            return self.msg_popped
        if self.msg_queue.empty():
            # No new messages; wait `timeout` seconds
            time.sleep(timeout)
            return None
        else:
            # Send the next message
            self.msg_popped = self.msg_queue.get()
            return self.msg_popped

    def commit(self, msg):
        """Our fake 'commit' is to clear out the "msg_popped" field, so we do
        not resend that message and move on to the rest of the queue."""
        self.msg_popped = None

    def produce_test(self, msg: str):
        """Push a message to the queue for testing purposes. Most likely you
        want to call this before starting the event loop"""
        self.msg_queue.put(MockMessage())
