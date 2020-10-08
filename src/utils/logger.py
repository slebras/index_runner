'''
Logging setup for the indexer.
'''

import logging
import logging.handlers  # required or logging.handlers is inaccessible. Pretty goofy
import os
import sys


def init_logger(logger: logging.Logger):
    """
    Initialize log settings. Mutates the `logger` object. Writes to stdout.
    """
    # Make the urllib3 logging less noisy
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    # Set the log level
    level = os.environ.get('LOGLEVEL', 'DEBUG').upper()
    logger.setLevel(level)
    logger.propagate = False  # Don't print duplicate messages
    logging.basicConfig(level=level)
    # Create the formatter
    fmt = "%(asctime)s %(levelname)-8s %(message)s (%(filename)s:%(lineno)s)"
    time_fmt = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(fmt, time_fmt)
    # Stdout
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)
    logger.addHandler(stdout_handler)
    print(f'Logger and level: {logger}')
    return logger


logger = init_logger(logging.getLogger('IR'))
