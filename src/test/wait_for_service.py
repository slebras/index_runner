"""
Block until the API is running.
"""
import time
import os
from index_runner.utils.config import get_config

_CONFIG = get_config()
service_up = False
timeout = 60
start_time = int(time.time())

while not service_up:
    print("Waiting app to start..")
    if os.path.exists(f'/tmp/{_CONFIG["zmq"]["socket_name"]}_front'):  # nosec
        service_up = True
    else:
        time.sleep(5)

print('Service is up!')
