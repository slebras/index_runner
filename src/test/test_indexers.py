import os
import json
import unittest

from index_runner.utils.config import get_config
from index_runner.indexers.reads import index_reads
from index_runner.indexers.genome import index_genome
from index_runner.indexers.assembly import index_assembly

_CONFIG = get_config()

_TEST_EVENTS = {
    'reads_save': {
        "wsid": 15,
        "ver": 1,
        "perm": None,
        "evtype": "NEW_VERSION",
        "objid": 44,
        "time": 1554408311320,
        "objtype": "KBaseFile.PairedEndLibrary‑2.0",
        "permusers": [],
        "user": "pranjan"
    },
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
        "time": 1554408311320,
        "objtype": "KBaseGenomes.Pangenome-4.2",
        "permusers": [],
        "user": "username"
    }
}


class TestIndexers(unittest.TestCase):

    maxDiff = None

    def _default_obj_test(self, event_data_str, indexer, check_against):
        print(f'Testing {event_data_str} indexer...')
        event_data = _TEST_EVENTS[event_data_str]
        json_data_path = f"{event_data_str}_{event_data['wsid']}_{event_data['objid']}.json"
        dir_path = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(dir_path, 'test_data', json_data_path)) as fd:
            test_data = json.load(fd)
        for idx, msg_data in enumerate(indexer(test_data['obj'], test_data['ws_info'], test_data['obj'])):
            self.assertEqual(msg_data['doc'], check_against[idx])

    @unittest.skip('x')
    def test_reads_indexer(self):
        check_against = [{
            'phred_type': None,
            'gc_content': None,
            'mean_quality_score': None,
            'mean_read_length': None,
            'sequencing_tech': "Illumina",
            'reads_type': "KBaseFile.PairedEndLibrary",
            'reads_type_version': "2.0",
            'size': 36510129,
            'interleaved': True,
            'single_genome': True
        }]
        self._default_obj_test('reads_save', index_reads, check_against)

    @unittest.skip('x')
    def test_assembly_indexer(self):
        check_against = [{
            'assembly_name': None,
            'mean_contig_length': 50195.5,
            'percent_complete_contigs': None,
            'percent_circle_contigs': None,
            'assembly_type': 'KBaseGenomeAnnotations.Assembly',
            'assembly_type_version': '6.0',
            'assembly_id': '3300029893_12.fa_assembly',
            'gc_content': 0.41488,
            'size': 2208602,
            'num_contigs': 44,
            'taxon_ref': None,
            'external_origination_date': None,
            'external_source_id': None,
            'external_source': None
        }]
        self._default_obj_test('assembly_save', index_assembly, check_against)

    def test_genome_indexer(self):
        # The genome `check_against` data is really big, so we keep it in an external file
        dir_path = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(dir_path, 'test_data/genome_check_against.json')) as fd:
            check_against = json.load(fd)
        self._default_obj_test('genome_save', index_genome, check_against)
