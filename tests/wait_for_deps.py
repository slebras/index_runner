"""
Wait for the daemon to finish booting before running the tests.
Called by scripts/run_tests.sh

We wait for the /tmp/IR_READY file to be present within the docker container.
This is created from src/index_runner/main.py
"""
import time
import subprocess
from src.utils.config import config

if __name__ == '__main__':
    tmp_ready_path = config()['proc_ready_path']
    cmd = ["docker-compose", "exec", "app", "ls", tmp_ready_path]
    timeout = 180
    start = time.time()
    while True:
        try:
            output = subprocess.check_output(cmd).decode()
        except subprocess.CalledProcessError:
            output = ""
        if tmp_ready_path in output:
            print('Daemon is started! Continuing.')
            break
        if time.time() - start > timeout:
            raise RuntimeError('Timed out waiting for the daemon to start')
        print('Still waiting for index runner to start...')
        time.sleep(3)
