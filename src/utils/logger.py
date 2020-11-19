'''
Logging setup for the indexer.
'''

import logging
import logging.handlers  # required or logging.handlers is inaccessible. Pretty goofy
import os
import sys


def init_logger(logger: logging.Logger):
    """
    Initialize log settings. Mutates the `logger` object.
    Write to stdout and to a local rotating file.
    Logs to tmp/app.log
    """
    # Set the log level
    level = os.environ.get('LOGLEVEL', 'DEBUG').upper()
    logger.setLevel(level)
    logger.propagate = False  # Don't print duplicate messages
    logging.basicConfig(level=level)
    # Create the formatter
    fmt = "%(asctime)s %(levelname)-8s %(message)s (%(filename)s:%(lineno)s)"
    time_fmt = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(fmt, time_fmt)
    # File handler
    os.makedirs('tmp', exist_ok=True)
    # 1mb max log file with 2 backups
    log_path = 'tmp/app.log'
    file_handler = logging.handlers.RotatingFileHandler(log_path, maxBytes=1048576, backupCount=2)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    # Stdout
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)
    logger.addHandler(stdout_handler)
    logger.info(f'Logger and level: {logger}')
    logger.info(f'Logging to file: {log_path}')
