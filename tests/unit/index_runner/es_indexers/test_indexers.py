import os
import json
import unittest
import requests
import tempfile
import shutil

from src.utils.config import config

from src.index_runner.es_indexers.reads import index_reads
from src.index_runner.es_indexers.genome import index_genome
from src.index_runner.es_indexers.assembly import index_assembly
from src.index_runner.es_indexers.pangenome import index_pangenome
from src.index_runner.es_indexers.taxon import index_taxon
from src.index_runner.es_indexers.tree import index_tree
from src.index_runner.es_indexers.annotated_metagenome_assembly import _index_ama
from src.index_runner.es_indexers.from_sdk import index_from_sdk

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
        "objid": 8,
        "time": 1554408311320,
        "objtype": "KBaseGenomes.Pangenome-4.2",
        "permusers": [],
        "user": "username"
    },
    'tree_save': {
        'wsid': 39794,
        'ver': 1,
        'perm': None,
        'evtype': "NEW_VERSION",
        "objid": 10,
        "time": 1554408311320,
        "objtype": "KBaseTrees.Tree-1.0",
        "permusers": [],
        "user": "username"
    },
    'taxon_save': {
        'wsid': 39794,
        'ver': 1,
        'perm': None,
        'evtype': "NEW_VERSION",
        "objid": 9,
        "time": 1554408311320,
        "objtype": "KBaseGenomeAnnotations.Taxon-1.0",
        "permusers": [],
        "user": "username"
    },
    'genomeset_save': {
        'wsid': 22385,
        'ver': 1,
        'perm': None,
        'evtype': "NEW_VERSION",
        'objid': 82,
        'time': 1554408311320,
        'objtype': "KBaseSets.GenomeSet-2.1",
        'permusers': [],
        'user': "username"
    },
    'annotated_metagenome_assembly_save': {
        'wsid': 39794,
        'ver': 1,
        'perm': None,
        'evtype': "NEW_VERSION",
        'objid': 22,
        'time': 1554408311320,
        'objtype': "KBaseMetagenomes.AnnotatedMetagenomeAssembly-1.0",
        'permusers': [],
        'user': "username"
    },
}  # type: dict

_DIR = os.path.dirname(os.path.realpath(__file__))


class TestIndexers(unittest.TestCase):

    maxDiff = None

    def _default_obj_test(self, event_data_str, indexer, check_against):
        print(f'Testing {event_data_str} indexer...')
        event_data = _TEST_EVENTS[event_data_str]
        json_data_path = f"{event_data_str}_{event_data['wsid']}_{event_data['objid']}.json"
        with open(os.path.join(_DIR, 'test_data', json_data_path)) as fd:
            test_data = json.load(fd)
        # print('[')
        for (idx, msg_data) in enumerate(indexer(test_data['obj'], test_data['ws_info'], test_data['obj'])):
            # print(json.dumps(msg_data), ',')
            self.assertEqual(msg_data, check_against[idx])
        # print(']')

    @unittest.skip('TODO')
    def test_from_sdk(self):
        check_against = [{
            '_action': 'index',
            'index': "genomeset_1",
            'id': "WS::22385:82",
            'doc': {
                "genomes": [
                    {"label": "gen1", "genome_ref": "22385/65/1"},
                    {"label": "gen2", "genome_ref": "22385/68/1"},
                    {"label": "gen3", "genome_ref": "22385/70/1"},
                    {"label": "gen4", "genome_ref": "22385/74/1"},
                    {"label": "gen5", "genome_ref": "22385/76/1"},
                    {"label": "gen6", "genome_ref": "22385/80/1"}
                ],
                "description": "Listeria monocytogenes Roary Test"
            }
        }]
        self._default_obj_test('genomeset_save', index_from_sdk, check_against)

    def test_genome_indexer(self):
        # The genome `check_against` data is really big, so we keep it in an external file
        with open(os.path.join(_DIR, 'test_data/genome_check_against.json')) as fd:
            check_against = json.load(fd)
        self._default_obj_test('genome_save', index_genome, check_against)

    def test_pangenome_indexer(self):
        # The pangenome `check_against` data is really big, so we keep it in an external file
        with open(os.path.join(_DIR, 'test_data/pangenome_check_against.json')) as fd:
            check_against = json.load(fd)
        self._default_obj_test('pangenome_save', index_pangenome, check_against)

    def test_tree_indexer(self):
        check_against = [{
            '_action': 'index',
            'index': 'tree_1',
            'id': 'WS::39794:10',
            'doc': {
                "tree_name": None,
                "type": "SpeciesTree",
                "labels": [{
                    "node_id": "user1",
                    "label": "Rhodobacter CACIA 14H1"
                }]
            }
        }]
        self._default_obj_test('tree_save', index_tree, check_against)

    def test_taxon_indexer(self):
        check_against = [{
            '_action': 'index',
            'index': 'taxon_2',
            'id': 'WS::39794:9',
            'doc': {
                "scientific_name": "Escherichia coli",
                "scientific_lineage": ("cellular organisms; Bacteria; Proteobacteria; Gammaproteobacteria; "
                                       "Enterobacterales; Enterobacteriaceae; Escherichia"),
                "domain": "Bacteria",
                "kingdom": None,
                "parent_taxon_ref": "12518/10/5",
                "genetic_code": 11,
                "aliases": [
                    "\"Bacillus coli\" Migula 1895", "\"Bacterium coli commune\" Escherich 1885",
                    "\"Bacterium coli\" (Migula 1895) Lehmann and Neumann 1896", "ATCC 11775",
                    "Bacillus coli", "Bacterium coli", "Bacterium coli commune", "CCUG 24",
                    "CCUG 29300", "CIP 54.8", "DSM 30083", "E. coli", "Enterococcus coli", "Escherchia coli",
                    "Escherichia coli (Migula 1895) Castellani and Chalmers 1919", "Escherichia sp. MAR",
                    "Escherichia/Shigella coli", "Eschericia coli", "JCM 1649", "LMG 2092", "NBRC 102203",
                    "NCCB 54008", "NCTC 9001", "bacterium 10a", "bacterium E3"
                ]
            }
        }]
        self._default_obj_test('taxon_save', index_taxon, check_against)
