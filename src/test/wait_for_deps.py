"""
Wait for the daemon to finish booting before running the tests.
Called by scripts/run_tests.sh
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
