"""
Utilites for dealing with the various services the index runner depends on.
"""
import time
import requests
from src.utils.config import config


def wait_for_dependencies(elasticsearch=True, re_api=True, timeout=60):
    """
    Block and wait for elasticsearch and / or the relation engine API.

    elasticsearch - True (the default) to block on elasticsearch.
    re_api - True (the default) to block on the relation engine API.
    timeout - the maximum time to wait for all services to come up.
    """
    start = int(time.time())
    if elasticsearch:
        _wait_for_service(config()['elasticsearch_url'], 'elasticsearch', start, timeout)
    if re_api:
        _wait_for_service(config()['re_api_url'] + '/', 'relation engine api', start, timeout)


def _wait_for_service(url, name, start_time, timeout):
    while True:
        try:
            requests.get(url).raise_for_status()
            break
        except Exception:
            print(f'Waiting for {name} service...')
            time.sleep(5)
            if (int(time.time()) - start_time) > timeout:
                raise RuntimeError(
                    f"Failed to connect to all services in {timeout}s. Timed out on {name}.")
