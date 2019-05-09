import sys
import json
import os
from confluent_kafka import Producer

kafka_url = os.environ.get('KAFKA_SERVER', 'kafka')
_PRODUCER = Producer({'bootstrap.servers': kafka_url})
_TOPIC = 'workspaceevents'


def main(input_path, token):
    with open(input_path) as infd:
        for line in infd:
            j = json.loads(line)
            event = {
                'evtype': 'REINDEX',
                'wsid': j['wsid'],
                'objid': j['objid']
            }
            _PRODUCER.produce(
                _TOPIC,
                json.dumps(event),
                callback=_delivery_report
            )


def _delivery_report(err, msg):
    if err is not None:
        sys.stderr.write(f'Message delivery failed for {msg.key()} in {msg.topic()}: {err}\n')
    else:
        print(f'Message "{msg.key()}" delivered to {msg.topic()}')
