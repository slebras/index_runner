import unittest
import json
import time
import requests
import logging
from confluent_kafka import Producer

from src.utils.config import config
from src.utils.re_client import get_doc, get_edge

logger = logging.getLogger('IR')

_TEST_EVENTS = {
    # This object will not be found in the listObjects method
    "new_object_version": {
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
    "deleted_object": {
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
    "set_global_permission": {
        "wsid": 41347,
        "evtype": "SET_GLOBAL_PERMISSION",
        "time": 1559165387436,
        "user": "username"
    }
}


class TestIntegration(unittest.TestCase):
    """
    Integration test to confirm that the pieces of the system are successfully interconnected.
    We can produce a message to a kafka topic, it gets picked up by
    index_runner, it gets pushed to es_writer, and then it gets saved to ES.
    """
    maxDiff = None

    def test_basic(self):
        """
        Test the full workflow from kafka to ES and RE.
        This just validates that things are connected.
        """
        # Produce an event on Kafka
        ev = _TEST_EVENTS['new_object_version']
        _produce(ev)
        _id = f"WS::{ev['wsid']}:{ev['objid']}"  # type: ignore
        # Fetch the doc from Elasticsearch
        doc = _get_es_doc_blocking(_id)
        self.assertEqual(doc['_id'], _id)
        # Relation engine tests...
        # Check for ws_object
        obj_doc = _wait_for_re_doc('ws_object', '41347:5')
        self.assertEqual(obj_doc['workspace_id'], 41347)
        self.assertEqual(obj_doc['object_id'], 5)
        self.assertEqual(obj_doc['deleted'], False)
        hsh = "0e8d1a5090be7c4e9ccf6d37c09d0eab"
        # Check for ws_object_hash
        hash_doc = _wait_for_re_doc('ws_object_hash', hsh)
        self.assertEqual(hash_doc['type'], 'MD5')
        # Check for ws_object_version
        ver_doc = _wait_for_re_doc('ws_object_version', '41347:5:1')
        self.assertEqual(ver_doc['workspace_id'], 41347)
        self.assertEqual(ver_doc['object_id'], 5)
        self.assertEqual(ver_doc['version'], 1)
        self.assertEqual(ver_doc['name'], "Narrative.1553621013004")
        self.assertEqual(ver_doc['hash'], "0e8d1a5090be7c4e9ccf6d37c09d0eab")
        self.assertEqual(ver_doc['size'], 26938)
        self.assertEqual(ver_doc['epoch'], 1554408999000)
        self.assertEqual(ver_doc['deleted'], False)
        # Check for ws_copied_from
        copy_edge = _wait_for_re_edge(
            'ws_copied_from',  # collection
            'ws_object_version/41347:5:1',  # from
            'ws_object_version/1:2:3'  # to
        )
        self.assertTrue(copy_edge)
        # Check for ws_version_of
        ver_edge = _wait_for_re_edge(
            'ws_version_of',  # collection
            'ws_object_version/41347:5:1',  # from
            'ws_object/41347:5'  # to
        )
        self.assertTrue(ver_edge)
        # Check for ws_workspace_contains_obj
        contains_edge = _wait_for_re_edge(
            'ws_workspace_contains_obj',  # collection
            'ws_workspace/41347',  # from
            'ws_object/41347:5'  # to
        )
        self.assertTrue(contains_edge)
        # Check for ws_obj_created_with_method edge
        created_with_edge = _wait_for_re_edge(
            'ws_obj_created_with_method',  # collection
            'ws_object_version/41347:5:1',  # from
            'ws_method_version/narrative:3.10.0:UNKNOWN'  # to
        )
        self.assertEqual(created_with_edge['method_params'], None)
        # Check for ws_obj_created_with_module edge
        module_edge = _wait_for_re_edge(
            'ws_obj_created_with_module',  # collection
            'ws_object_version/41347:5:1',  # from
            'ws_module_version/narrative:3.10.0'  # to
        )
        self.assertTrue(module_edge)
        # Check for ws_obj_instance_of_type
        type_edge = _wait_for_re_edge(
            'ws_obj_instance_of_type',  # collection
            'ws_object_version/41347:5:1',  # from
            'ws_type_version/KBaseNarrative.Narrative-4.0'  # to
        )
        self.assertTrue(type_edge)
        # Check for the ws_owner_of edge
        owner_edge = _wait_for_re_edge(
            'ws_owner_of',  # collection
            'ws_user/username',  # from
            'ws_object_version/41347:5:1',  # to
        )
        self.assertTrue(owner_edge)
        # Check for the ws_refers_to edges
        referral_edge1 = _wait_for_re_edge(
            'ws_refers_to',  # collection
            'ws_object_version/41347:5:1',  # from
            'ws_object_version/1:1:1',  # to
        )
        self.assertTrue(referral_edge1)
        referral_edge2 = _wait_for_re_edge(
            'ws_refers_to',  # collection
            'ws_object_version/41347:5:1',  # from
            'ws_object_version/2:2:2',  # to
        )
        self.assertTrue(referral_edge2)
        # Check for the ws_prov_descendant_of edges
        prov_edge1 = _wait_for_re_edge(
            'ws_prov_descendant_of',  # collection
            'ws_object_version/41347:5:1',  # from
            'ws_object_version/1:1:1',  # to
        )
        self.assertTrue(prov_edge1)
        prov_edge2 = _wait_for_re_edge(
            'ws_prov_descendant_of',  # collection
            'ws_object_version/41347:5:1',  # from
            'ws_object_version/2:2:2',  # to
        )
        self.assertTrue(prov_edge2)
        del_ev = _TEST_EVENTS['deleted_object']
        _produce(del_ev)
        del_id = f"WS::{del_ev['wsid']}:{del_ev['objid']}"  # type: ignore
        _check_deleted_es_doc_blocking(del_id)

    def test_import_nonexistent_existing(self):
        """Test an IMPORT_NONEXISTENT event."""
        _produce({'evtype': 'INDEX_NONEXISTENT', 'wsid': 41347, 'objid': 6, 'ver': 1})
        admin_topic = config()['topics']['admin_events']
        obj_doc1 = _wait_for_re_doc('ws_object', '41347:6')
        self.assertEqual(obj_doc1['object_id'], 6)
        _produce({'evtype': 'INDEX_NONEXISTENT', 'wsid': 41347, 'objid': 5, 'ver': 1}, admin_topic)
        obj_doc2 = _wait_for_re_doc('ws_object', '41347:6')
        self.assertEqual(obj_doc1['_rev'], obj_doc2['_rev'])

    def test_reload_aliases(self):
        """test the RELOAD_ELASTIC_ALIASES event. TODO: fill out test."""
        _produce({'evtype': "RELOAD_ELASTIC_ALIASES", 'wsid': 1, 'objid': 1, 'ver': 1})
        es_aliases = _get_es_aliases_blocking()
        config_aliases = config()['global']['aliases']
        for alias in config_aliases:
            self.assertTrue(alias in es_aliases)
            for index in config_aliases[alias]:
                self.assertTrue(index in es_aliases[alias])


# -- Test utils

def _delivery_report(err, msg):
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.info(f'Message delivered to: {msg.topic()}')


def _produce(data, topic=config()['topics']['workspace_events']):
    producer = Producer({'bootstrap.servers': config()['kafka_server']})
    producer.produce(topic, json.dumps(data), callback=_delivery_report)
    producer.poll(60)


def _check_deleted_es_doc_blocking(_id, timeout=180):
    """
    Helper for testing deletion. Inverse function of _get_es_doc_blocking
    Try to fetch a doc on ES, returning successfully once the doc does not exist.
    """
    start_time = int(time.time())
    while True:
        result = _get_es_doc(_id)
        logger.info('result is', result)
        if not result:
            return None
        if (int(time.time()) - start_time) > timeout:
            raise RuntimeError(f"Document {_id} was not deleted in {timeout}s.")
        print(f'Waiting for document {_id} to be deleted')
        time.sleep(5)


def _get_es_doc_blocking(_id, timeout=180):
    """Fetch a doc on ES, waiting for it to become available, with a timeout."""
    start_time = int(time.time())
    while True:
        result = _get_es_doc(_id)
        if result:
            return result
        if (int(time.time()) - start_time) > timeout:
            raise RuntimeError(f"Document {_id} not found in {timeout}s.")
        time.sleep(5)


def _get_es_aliases_blocking(timeout=180):
    start_time = int(time.time())
    while True:
        result = _get_es_aliases()
        if result:
            return result
        if (int(time.time()) - start_time) > timeout:
            raise RuntimeError(f"Aliases not found in {timeout}s.")
        time.sleep(5)


def _get_es_doc(_id):
    """Fetch a document from elastic based on ID."""
    prefix = config()['elasticsearch_index_prefix']
    logger.info(f'Fetching {_id} from index {prefix}.*')
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
    """fetch aliases from elasticsearch"""
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


def _wait_for_re_edge(coll, from_key, to_key):
    """Fetch an edge with the RE API, waiting for it to become available with 30s timeout."""
    timeout = int(time.time()) + 30
    while True:
        results = get_edge(coll, from_key, to_key)
        if results['count'] > 0:
            break
        else:
            if int(time.time()) > timeout:
                raise RuntimeError('Timed out trying to fetch', from_key, to_key)
            time.sleep(1)
    return results['results'][0]


def _wait_for_re_doc(coll, key, timeout=180):
    """Fetch a doc with the RE API, waiting for it to become available with a 30s timeout."""
    start_time = time.time()
    while True:
        results = get_doc(coll, key)
        if results['count'] > 0:
            break
        else:
            if int(time.time() - start_time) > timeout:
                raise RuntimeError('Timed out trying to fetch', key)
            time.sleep(1)
    return results['results'][0]
