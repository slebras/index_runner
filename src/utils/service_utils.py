"""
Utilites for dealing with the various services the index runner depends on.
"""
import time
import requests
import logging
from src.utils.config import config

logging.getLogger(__name__)


def wait_for_dependencies(elasticsearch=True, re_api=True, timeout=60):
    """
    Block and wait for elasticsearch and / or the relation engine API.

    elasticsearch - True (the default) to block on elasticsearch.
    re_api - True (the default) to block on the relation engine API.
    timeout - the maximum time to wait for all services to come up.
    """
    start = int(time.time())
    if elasticsearch:
        es_url = config()['elasticsearch_url'] + '/_cluster/health'
        params = {'wait_for_status': 'yellow', 'timeout': '60s'}
        _wait_for_service(es_url, 'elasticsearch', start, timeout, params=params)
    if re_api:
        _wait_for_service(config()['re_api_url'] + '/', 'relation engine api', start, timeout)


def _wait_for_service(url, name, start_time, timeout, params=None):
    while True:
        try:
            logging.info(f'Waiting for {name} service...')
            requests.get(url, params=params).raise_for_status()
            break
        except Exception:
            time.sleep(5)
            if (int(time.time()) - start_time) > timeout:
                raise RuntimeError(f"Failed to connect to all services in {timeout}s. Timed out on {name}.")
