import unittest
import json
import time
import requests
from confluent_kafka import Producer

from src.utils.config import config
from src.utils.re_client import get_doc
from src.utils.service_utils import wait_for_dependencies

_TEST_EVENT = {
   "wsid": 33192,
   "ver": 2,
   "perm": None,
   "evtype": "NEW_VERSION",
   "objid": 23,
   "time": 1578439639664,
   "objtype": "KBaseGenomeAnnotations.Assembly-6.0",
   "permusers": [],
   "user": "jayrbolton"
}  # type: dict


class TestIntegration(unittest.TestCase):
    """
    Integration test to confirm that the pieces of the system are successfully interconnected.
    We can produce a message to a kafka topic and something gets saved to ES and Arango.

    To test more detailed functionality of indexers/importers, write separate
    unit tests. Including them here will be too slow.
    """
    maxDiff = None

    @classmethod
    def setUpClass(cls):
        wait_for_dependencies()

    def test_integration(self):
        # Produce an event on Kafka
        _produce(_TEST_EVENT)
        wsid = _TEST_EVENT['wsid']  # type: ignore
        objid = _TEST_EVENT['objid']  # type: ignore
        es_id = f"WS::{wsid}:{objid}"  # type: ignore
        re_key = f"{wsid}:{objid}"
        es_doc = _get_es_doc_blocking(es_id)
        re_doc = _wait_for_re_doc('ws_object', re_key)
        self.assertEqual(es_doc['_id'], es_id)
        self.assertEqual(re_doc['workspace_id'], wsid)
        self.assertEqual(re_doc['object_id'], objid)


# -- Test utils

def _delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to: {msg.topic()}')


def _produce(data, topic=config()['topics']['workspace_events']):
    producer = Producer({'bootstrap.servers': config()['kafka_server']})
    producer.produce(topic, json.dumps(data), callback=_delivery_report)
    producer.poll(60)
    print(f"Produced {data}")


def _get_es_doc_blocking(_id, timeout=180):
    """Fetch a doc on ES, waiting for it to become available, with a timeout."""
    start_time = int(time.time())
    while True:
        print(f'Waiting for doc {_id}')
        result = _get_es_doc(_id)
        if result:
            return result
        if (int(time.time()) - start_time) > timeout:
            raise RuntimeError(f"Document {_id} not found in {timeout}s.")
        time.sleep(5)


def _get_es_doc(_id):
    """Fetch a document from elastic based on ID."""
    prefix = config()['elasticsearch_index_prefix']
    url = f"{config()['elasticsearch_url']}/{prefix}.*/_search?size=1"
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
    if not respj['hits']['total']['value']:
        return None
        # raise RuntimeError(f"Document {_id} not found.")
    return respj['hits']['hits'][0]


def _get_es_aliases():
    """Fetch aliases from elasticsearch"""
    url = f"{config()['elasticsearch_url']}/_aliases"
    resp = requests.get(url)
    if not resp.ok:
        raise RuntimeError(resp.text)
    respj = resp.json()
    if len(respj) < 1:
        return None
    aliases = {}  # type: dict
    # we remove prefix from index and alias names (should all be "search2.")
    for index in respj:
        for al in respj[index]['aliases'].keys():
            al = al.split('.')[1]
            if al in aliases:
                aliases[al].append(index.split('.')[1])
            else:
                aliases[al] = [index.split('.')[1]]
    return aliases


def _wait_for_re_doc(coll, key, timeout=180):
    """Fetch a doc with the RE API, waiting for it to become available with a 30s timeout."""
    start_time = time.time()
    while True:
        print(f'Waiting for doc {coll}/{key}')
        results = get_doc(coll, key)
        if results['count'] > 0:
            break
        else:
            if int(time.time() - start_time) > timeout:
                raise RuntimeError('Timed out trying to fetch', key)
            time.sleep(1)
    return results['results'][0]
