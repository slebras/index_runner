"""
Used in run_tests.sh to wait for various service dependencies (arango, elastic)
to come online before we start the tests.
"""
import time
import os
from src.utils.config import config

if __name__ == '__main__':
    tmp_ready_path = config()['proc_ready_path']
    timeout = 180
    start = time.time()
    while True:
        if os.path.exists(tmp_ready_path):
            print('Daemon is started! Continuing.')
            break
        if time.time() - start > timeout:
            raise RuntimeError('Timed out waiting for the daemon to start')
        print('Still waiting for index runner to start...')
        time.sleep(3)
