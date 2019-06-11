import unittest
import json
import time
import requests
from confluent_kafka import Producer, Consumer, KafkaError

from index_runner.utils.config import get_config

_CONFIG = get_config()
_HEADERS = {"Content-Type": "application/json"}
_TEST_EVENTS = {
    # This object will not be found in the listObjects method
    'narrative_save_nonexistent': {
        "wsid": 41347,
        "ver": 16,
        "perm": None,
        "evtype": "NEW_VERSION",
        "objid": 5,
        "time": 1554408508419,
        "objtype": "KBaseNarrative.Narrative-4.0",
        "permusers": [],
        "user": "username"
    },
    'new_object_version': {
        "wsid": 41347,
        "ver": 1,
        "perm": None,
        "evtype": "NEW_VERSION",
        "objid": 6,
        "time": 1554404277444,
        "objtype": "KBaseGenomes.Genome-14.2",
        "permusers": [],
        "user": "username"
    },
    'deleted_object': {
        "wsid": 41347,
        "ver": None,
        "perm": None,
        "evtype": "OBJECT_DELETE_STATE_CHANGE",
        "objid": 5,
        "time": 1554408311320,
        "objtype": None,
        "permusers": [],
        "user": "username"
    },
    'set_global_permission': {
        "wsid": 41347,
        "evtype": "SET_GLOBAL_PERMISSION",
        "time": 1559165387436,
        "user": "username"
    }
}


class TestIntegration(unittest.TestCase):

    maxDiff = None

    def test_object_save_and_delete(self):
        """
        Test a NEW_VERSION event plus an OBJECT_DELETE_STATE_CHANGE.
        """
        print(f"Producing NEW_VERSION event to {_CONFIG['topics']['workspace_events']}")
        _produce(_TEST_EVENTS['narrative_save_nonexistent'])
        time.sleep(30)
        _id = 'WS::41347:5'
        doc = _get_doc(_id)
        self.assertEqual(doc['_id'], _id)
        print(f"Producing OBJECT_DELETE_STATE_CHANGE event to {_CONFIG['topics']['workspace_events']}")
        _produce(_TEST_EVENTS['deleted_object'])
        time.sleep(30)
        with self.assertRaises(RuntimeError):
            _get_doc(_id)
        print("Producing a SET_GLOBAL_PERMISSION event.")
        _produce(_TEST_EVENTS['set_global_permission'])
        time.sleep(30)

    def test_index_nonexistent(self):
        _produce({
            'evtype': 'INDEX_NONEXISTENT',
            'wsid': 41347,
            'objid': 5
        })

    def test_es_error_logging(self):
        """
        Test an event that throws an error while trying to index. We check that
        an error gets logged to ES.
        """
        print('Producing an invalid NEW_VERSION event..')
        _produce({
            'evtype': 'NEW_VERSION',
            'wsid': 1,
            # Invalid message; missing data
        })
        time.sleep(30)
        # _id = 'xyz'  # todo
        # doc = _get_doc(_id)
        # self.assertEqual(doc['_id'], _id)


def _consume_last(topic, key, timeout=120):
    """Consume the most recent message from the topic stream."""
    consumer = Consumer({
        'bootstrap.servers': _CONFIG['kafka_server'],
        'group.id': 'test_only',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([topic])
    start_time = time.time()
    while True:
        # get time elapsed in seconds
        elapsed = time.time() - start_time
        if elapsed > timeout:
            # close consumer before throwing error
            consumer.close()
            raise TimeoutError(f"Error: Consumer waited past timeout of {timeout} seconds")
        msg = consumer.poll(0.5)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print('End of stream.')
            else:
                print(f"Error: {msg.error()}")
            continue
        if msg.key() != key:
            continue
        # We got our message
        consumer.close()
        return json.loads(msg.value())


def _produce(data, topic=_CONFIG['topics']['workspace_events']):
    producer = Producer({'bootstrap.servers': _CONFIG['kafka_server']})
    producer.produce(topic, json.dumps(data), callback=_delivery_report)
    producer.poll(60)


def _get_doc(_id):
    """Fetch a document from elastic based on ID."""
    prefix = _CONFIG['elasticsearch_index_prefix']
    url = f"{_CONFIG['elasticsearch_url']}/{prefix}.*/_search?size=1"
    resp = requests.post(
        url,
        data=json.dumps({
            'query': {'term': {'_id': _id}}
        }),
        headers={'Content-Type': 'application/json'}
    )
    if not resp.ok:
        raise RuntimeError(resp.text)
    respj = resp.json()
    if not respj['hits']['total']:
        raise RuntimeError(f"Document {_id} not found.")
    return respj['hits']['hits'][0]


def _delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed:', err)
    else:
        print('Message delivered to', msg.topic())
