import unittest
import json
import time
from confluent_kafka import Producer, Consumer, KafkaError

from index_runner.utils.config import get_config
from index_runner.indexers.main import _default_fields
from index_runner.indexers.reads import index_reads
from index_runner.indexers.assembly import index_assembly

from kbase_workspace_client import WorkspaceClient
from kbase_workspace_client.exceptions import WorkspaceResponseError

_CONFIG = get_config()
_HEADERS = {"Content-Type": "application/json"}
_TEST_EVENTS = {
    'new_object': {
    },
    'narrative_save': {
        "wsid": 41347,
        "ver": 16,
        "perm": None,
        "evtype": "NEW_VERSION",
        "objid": 1,
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
    'reads_save': {
        "wsid": 15,
        "ver": 1,
        "perm": None,
        "evtype": "NEW_VERSION",
        "objid": 44,
        "time": 1554408311320,
        "objtype": "KBaseFile.PairedEndLibrary‑2.0",
        "permusers": [],
        "user": "username"
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
}


def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed:', err)
    else:
        print('Message delivered to', msg.topic(), msg.partition())


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


class TestTypes(unittest.TestCase):

    def _default_obj_test(self, event_data_str, indexer, check_against):
        print(f'Testing {event_data_str} indexer...')
        event_data = _TEST_EVENTS[event_data_str]
        upa = "/".join([
            str(event_data['wsid']),  # type: ignore
            str(event_data['objid']),  # type: ignore
            str(event_data['ver'])  # type: ignore
        ])
        ws_client = WorkspaceClient(url=_CONFIG['workspace_url'], token=_CONFIG['ws_token'])
        try:
            obj_data = ws_client.admin_req('getObjects', {
                'objects': [{'ref': upa}]
            })
        except WorkspaceResponseError as err:
            print('Workspace response error:', err.resp_data)
            raise err
        try:
            ws_info = ws_client.admin_req('getWorkspaceInfo', {
                'id': event_data['wsid']  # type: ignore
            })
        except WorkspaceResponseError as err:
            print('Workspace response error:', err.resp_data)
            raise err
        obj_data_v1 = obj_data
        msg_data = indexer(obj_data, ws_info, obj_data_v1)
        msg_data['doc'].update(_default_fields(obj_data, ws_info, obj_data_v1))
        print('..objects formatted for index, verifying output...')
        self.assertEqual(msg_data['doc'], check_against)

    def test_reads_indexer(self):
        check_against = {
            'phred_type': None,
            'gc_content': None,
            'mean_quality_score': None,
            'mean_read_length': None,
            'sequencing_tech': "Illumina",
            'reads_type': "KBaseFile.PairedEndLibrary",
            'reads_type_version': "2.0",
            'size': 36510129,
            'interleaved': True,
            'single_genome': True,
            'obj_name': "rhodobacter.art.q20.int.PE.reads",
            'guid': "15:44",
            'creator': 'username',
            'access_group': 15,
            'version': 1,
            'timestamp': 1475672651086,
            'creation_date': "2016-10-05T17:11:40+0000",
            'is_public': True,
            'obj_id': 44,
            "shared_users": ['username'],
            'copied': None
        }
        self._default_obj_test('reads_save', index_reads, check_against)

    def test_assembly_indexer(self):

        check_against = {
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
            'external_source': None,
            'creator': 'slebras',
            'access_group': 39794,
            'obj_name': '3300029893_12',
            'shared_users': ['username'],
            'guid': '39794:4',
            'timestamp': 1548197802938,
            'creation_date': '2019-05-03T17:31:47+0000',
            'is_public': True,
            'version': 1,
            'obj_id': 4,
            'copied': '39795/6/1'
        }
        self._default_obj_test('assembly_save', index_assembly, check_against)


class TestIntegration(unittest.TestCase):

    def test_narrative_update_event(self):
        print('producing to', _CONFIG['topics']['workspace_events'])
        producer = Producer({'bootstrap.servers': _CONFIG['kafka_server']})
        producer.produce(
            _CONFIG['topics']['workspace_events'],
            json.dumps(_TEST_EVENTS['narrative_save']),
            callback=delivery_report
        )
        producer.poll(60)
        print('..finished producing, now consuming. This may take a couple minutes as the workers restart...')
        msg_data = _consume_last(_CONFIG['topics']['elasticsearch_updates'], b'index')
        check_against = {
            "narrative_title": "Test Narrative Name",
            'obj_id': 1,
            'version': 16,
            'obj_name': "Narrative.1553621013004",
            'data_objects': [
                {
                    'name': 'Rhodobacter_CACIA_14H1',
                    'obj_type': 'KBaseGenomes.Genome-7.0'
                }, {
                    'name': '_Nostoc_azollae__0708',
                    'obj_type': 'KBaseGenomes.Genome-14.2'
                }, {
                    'name': 'Acetobacter_aceti_NBRC_14818',
                    'obj_type': 'KBaseGenomes.Genome-14.1'
                }, {
                    'name': '_H9',
                    'obj_type': 'KBaseBiochem.Media-1.0'
                }, {
                    'name': 'KBase_derived_Rhodobacter_CACIA_14H1.gff_genome.assembly',
                    'obj_type': 'KBaseGenomeAnnotations.Assembly-6.0'
                }, {
                    'name': 'KBase_derived_Rhodobacter_CACIA_14H1.gff_genome',
                    'obj_type': 'KBaseGenomes.Genome-15.1'
                }
            ],
            "cells": [
                {
                    'desc': 'Testing\n',
                    'cell_type': 'markdown'
                }, {
                    'desc': "print('nope')",
                    'cell_type': 'code_cell'
                }, {
                    'desc': 'Import GFF3/FASTA file as Genome from Staging Area',
                    'cell_type': 'kbase_app'
                }
            ],
            "creator": "username",
            "guid": "41347:1",
            "shared_users": ['username'],
            "total_cells": 3,
            "access_group": 41347,
            "is_public": True,
            "timestamp": 1554408998887,
            'creation_date': '2019-03-26T17:23:33+0000',
            'copied': '1/2/3'
        }
        self.assertEqual(msg_data['doc'], check_against)
