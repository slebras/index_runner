import unittest

import src.test.helpers as helpers

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
}

_TEST_DEL_EVENT = {
   "wsid": 33192,
   "ver": 2,
   "perm": None,
   "evtype": 'OBJECT_DELETE_STATE_CHANGE',
   "objid": 23,
   "time": 1578439639664,
   "objtype": "KBaseGenomeAnnotations.Assembly-6.0",
   "permusers": [],
   "user": "jayrbolton"
}


class TestIntegration(unittest.TestCase):
    """
    Integration test to confirm that the pieces of the system are successfully interconnected.
    We can produce a message to a kafka topic and something gets saved to ES and Arango.

    To test more detailed functionality of indexers/importers, write separate
    unit tests. Including them here will be too slow.
    """
    maxDiff = None

    def test_integration(self):
        # Produce an event on Kafka
        helpers.produce(_TEST_EVENT)
        wsid = _TEST_EVENT['wsid']  # type: ignore
        objid = _TEST_EVENT['objid']  # type: ignore
        es_id = f"WS::{wsid}:{objid}"  # type: ignore
        re_key = f"{wsid}:{objid}"
        es_doc = helpers.get_es_doc_blocking(es_id)
        re_doc = helpers.wait_for_re_doc('ws_object', re_key)
        self.assertEqual(es_doc['_id'], es_id)
        self.assertEqual(re_doc['workspace_id'], wsid)
        self.assertEqual(re_doc['object_id'], objid)
        log_doc = helpers.get_es_doc_blocking(_TEST_EVENT['time'])
        self.assertEqual(log_doc['_source'], _TEST_EVENT)
