import os
import json
import unittest

# from index_runner.indexers.genome import delete_genome
from index_runner.indexers.main import generic_deleter
# from index_runner.indexers.pangenome import delete_pangenome
from index_runner.utils.config import get_config

_CONFIG = get_config()

_TEST_EVENTS = {
    'assembly_save': {
        "wsid": 39794,
        "ver": 1,
        "perm": None,
        "evtype": "NEW_VERSION",
        "objid": 4,
        "time": 1554408311320,
        "objtype": "KBaseGenomeAnnotations.Assembly‑6.0",
        "permusers": [],
        "user": "username"
    },
    'genome_save': {
        "wsid": 39794,
        "ver": 1,
        "perm": None,
        "evtype": "NEW_VERSION",
        "objid": 4,
        "time": 1554408311320,
        "objtype": "KBaseGenomes.Genome‑15.1",
        "permusers": [],
        "user": "username"
    },
    'pangenome_save': {
        'wsid': 39794,
        'ver': 1,
        'perm': None,
        'evtype': "NEW_VERSION",
        "objid": 8,
        "time": 1554408311320,
        "objtype": "KBaseGenomes.Pangenome-4.2",
        "permusers": [],
        "user": "username"
    }
}


class TestDeleters(unittest.TestCase):

    maxDiff = None

    def _default_obj_test(self, event_data_str, indexer, check_against):
        print(f'Testing {event_data_str} indexer...')
        event_data = _TEST_EVENTS[event_data_str]
        json_data_path = f"{event_data_str}_{event_data['wsid']}_{event_data['objid']}.json"
        dir_path = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(dir_path, 'test_data', json_data_path)) as fd:
            test_data = json.load(fd)
        for idx, msg_data in enumerate(indexer(test_data['obj'])):
            self.assertEqual(msg_data['id'], check_against[idx])

    def test_default_deleter(self):
        check_against = ["39794:4"]
        self._default_obj_test('assembly_save', generic_deleter, check_against)

    # def test_genome_deleter(self):
    #     # The genome `check_against` data is really big, so we keep it in an external file
    #     dir_path = os.path.dirname(os.path.realpath(__file__))
    #     with open(os.path.join(dir_path, 'test_data/genome_check_against_deleter.json')) as fd:
    #         check_against = json.load(fd)
    #     self._default_obj_test('genome_save', delete_genome, check_against)

    # def test_pangenome_deleter(self):
    #     # The pangenome `check_against` data is really big, so we keep it in an external file
    #     dir_path = os.path.dirname(os.path.realpath(__file__))
    #     with open(os.path.join(dir_path, 'test_data/pangenome_check_against_deleter.json')) as fd:
    #         check_against = json.load(fd)
    #     self._default_obj_test('pangenome_save', delete_pangenome, check_against)
