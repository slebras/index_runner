"""
Tests for the index_runner.py worker
"""
import unittest
import zmq
import json
from confluent_kafka import Producer
from index_runner.index_runner import IndexRunner
from utils.worker_group import WorkerGroup
from utils.config import get_config

_CONFIG = get_config()


class TestIndexRunner(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        sock_url = "ipc:///tmp/test_index_runner"
        cls.sock = zmq.Context().socket(zmq.PULL)
        cls.sock.bind(sock_url)
        cls.ir = WorkerGroup(fn=IndexRunner, args=(sock_url,), count=1)

    @classmethod
    def tearDownClass(cls):
        cls.ir.kill()

    def test_run_indexer_valid(self):
        _produce({
            "wsid": 41347,
            "evtype": "NEW_VERSION",
            "objid": 5
        })
        print('Trying to receive from sock..')
        rep = self.sock.recv_json()
        self.assertEqual(rep['_action'], 'index')
        self.assertEqual(rep['doc']['narrative_title'], 'Test Narrative Name')
        self.assertEqual(rep['index'], 'narrative:1')
        self.assertEqual(rep['id'], 'WS::41347:5')


# -- Utils


def _produce(data, topic=_CONFIG['topics']['workspace_events']):
    print(f"Producing to: {_CONFIG['kafka_server']}")
    producer = Producer({'bootstrap.servers': _CONFIG['kafka_server']})
    producer.produce(topic, json.dumps(data), callback=_delivery_report)
    producer.poll(60)


def _delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message "{msg.value()}" delivered to topic "{msg.topic()}"')
