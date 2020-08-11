import os
import json
import unittest
import tempfile
import shutil

from src.index_runner.es_indexers.reads import index_reads
from src.index_runner.es_indexers.genome import index_genome
from src.index_runner.es_indexers.assembly import index_assembly
from src.index_runner.es_indexers.pangenome import index_pangenome
from src.index_runner.es_indexers.taxon import index_taxon
from src.index_runner.es_indexers.tree import index_tree
from src.index_runner.es_indexers.annotated_metagenome_assembly import _index_ama
from src.index_runner.es_indexers.sample_set import index_sample_set
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
    'sample_set_save': {
        'wsid': 39794,
        'ver': 1,
        'perm': None,
        'evtype': "NEW_VERSION",
        'objid': 40,
        'time': 1554408311320,
        'objtype': "KBaseSets.SampleSet-1.0",
        'permusers': [],
        'user': "username"
    }
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

    def test_sample_set_indexer(self):
        check_against = [
            {
                "_action": "index",
                "doc": {
                    "description": "Pamela's Sesar sample data",
                    "sample_ids": ["1"],
                    "sample_names": ["PB-Low-5"],
                    "sample_versions": [1]
                },
                "index": "sample_set_1",
                "id": "WS::39794:40"
            },
            {
                "_action": "index",
                "doc": {
                    "description": "Pamela's Sesar sample data",
                    "sample_ids": ["1"],
                    "sample_names": ["PB-Low-5"],
                    "sample_versions": [1]
                },
                "index": "sample_set_version_1",
                "id": "WSVER::39794:40:1"
            },
            {
                "_action": "index",
                "doc": {
                    "save_date": 1591823661642,
                    "sample_version": 1,
                    "name": "PB-Low-5",
                    "parent_id": "WS::39794:40",
                    "latitude": "33.3375;degrees",
                    "collection_date": "2019-06-26 00:00:00;day",
                    "longitude": "81.71861111;degrees",
                    "material": "Soil",
                    "current_archive_contact": "pweisenhorn@anl.gov",
                    "location_description": "Savannah River Site",
                    "collection_method": "Coring > Syringe",
                    "primary_physiographic_feature": "Hollow",
                    "field_program_cruise": "Argonne Wetlands Hydrobiogeochemistry SFA",
                    "coordinate_precision?": "30",
                    "navigation_type": "GPS",
                    "relation_type": "grouped",
                    "name_of_physiographic_feature": "Tims Branch watershed",
                    "current_archive": "Argonne National Lab",
                    "locality_description": "Pine Backwater",
                    "purpose": "Microbial Characterization 1",
                    "collector_chief_scientist": "Pamela Weisenhorn",
                    "related_identifiers": "IEAWH0002",
                    "id": "IEAWH0001",
                    "node_id": "IEAWH0001"
                },
                "index": "sample_1",
                "id": "SMP::8948a78f-9b3b-477b-8502-fc6fc952a394:1"
            }
        ]
        self._default_obj_test('sample_set_save', index_sample_set, check_against)

    def test_reads_indexer(self):
        check_against = [{
            '_action': 'index',
            'id': 'WS::15:44',
            'index': 'reads_2',
            'doc': {
                'phred_type': None,
                'gc_content': None,
                'mean_quality_score': None,
                'mean_read_length': None,
                'sequencing_tech': "Illumina",
                'size': 36510129,
                'interleaved': True,
                'single_genome': True
            }
        }]
        self._default_obj_test('reads_save', index_reads, check_against)

    def test_assembly_indexer(self):
        check_against = [{
            '_action': 'index',
            'id': 'WS::39794:4',
            'index': 'assembly_2',
            'doc': {
                'assembly_name': None,
                'mean_contig_length': 50195.5,
                'percent_complete_contigs': None,
                'percent_circle_contigs': None,
                'assembly_id': '3300029893_12.fa_assembly',
                'gc_content': 0.41488,
                'size': 2208602,
                'num_contigs': 44,
                'taxon_ref': None,
                'external_origination_date': None,
                'external_source_id': None,
                'external_source': None
            }
        }]
        self._default_obj_test('assembly_save', index_assembly, check_against)

    def test_annotated_metagenome_assembly_indexer(self):
        # the annotated_meganome_assembly 'check_against' data is really big, so we keep it in an external file
        event_data_str = "annotated_metagenome_assembly_save"
        with open(os.path.join(_DIR, 'test_data/annotated_metagenome_assembly_check_against.json')) as fd:
            check_against = json.load(fd)
        print(f'Testing {event_data_str} indexer...')
        event_data = _TEST_EVENTS[event_data_str]
        json_data_path = f"{event_data_str}_{event_data['wsid']}_{event_data['objid']}.json"
        with open(os.path.join(_DIR, 'test_data', json_data_path)) as fd:
            test_data = json.load(fd)

        features_test_file = os.path.join(_DIR, "test_data", "features.json.gz")
        info = test_data['obj']['info']
        workspace_id = info[6]
        object_id = info[0]
        version = info[4]
        ama_index = f"WS::{workspace_id}:{object_id}"
        ver_ama_index = f"WSVER::{workspace_id}:{object_id}:{version}"

        try:
            tmp_dir = tempfile.mkdtemp()
            features_test_file_copy_path = os.path.join(tmp_dir, "features.json.gz")
            shutil.copy(features_test_file, features_test_file_copy_path)
            for (idx, msg_data) in enumerate(_index_ama(features_test_file_copy_path, test_data['obj']['data'],
                                                        ama_index, ver_ama_index, tmp_dir)):
                self.assertEqual(msg_data, check_against[idx])

        finally:
            shutil.rmtree(tmp_dir)

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
