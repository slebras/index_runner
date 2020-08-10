from src.utils.logger import init_logger
import logging

from src.utils.config import config
from src.index_runner.event_loop import start_loop
from tests.unit.index_runner.helpers import MockConsumer

logger = logging.getLogger('IR')
init_logger(logger)


def test_retry_count():
    """Test that our handler function that always throws will get called the
    right amount of times before getting skipped."""
    call_count = 0
    consumer = MockConsumer([])

    def handler_raise(message):
        nonlocal call_count
        call_count += 1
        raise RuntimeError('Test error')
    consumer.produce_test('{}')
    start_loop(consumer, handler_raise, return_on_empty=True)
    assert call_count == config()['max_handler_failures']
