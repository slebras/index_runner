import unittest

import tests.helpers as helpers

_TEST_EVENT = {
   "wsid": 55825,
   "ver": 15,
   "perm": None,
   "evtype": "NEW_VERSION",
   "objid": 2,
   "time": 101,
   "user": "jayrbolton"
}


class TestIntegration(unittest.TestCase):
    """
    Integration test to confirm that the pieces of the system are successfully
    interconnected. We produce a message to a kafka topic and confirm something
    gets saved to ES and Arango.

    To test more detailed functionality of indexers/importers, write separate
    unit tests. Including them here will be too slow.
    """
    maxDiff = None

    def test_integration(self):
        # Produce the new object event on Kafka
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
