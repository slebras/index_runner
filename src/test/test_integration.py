import unittest
import json
import time
from confluent_kafka import Producer, Consumer, KafkaError

from index_runner.utils.config import get_config

_CONFIG = get_config()
_HEADERS = {"Content-Type": "application/json"}
_TEST_EVENTS = {
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
    'set_global_permission': {
        "wsid": 41347,
        "evtype": "SET_GLOBAL_PERMISSION",
        "time": 1559165387436,
        "user": "username"
    }
}


class TestIntegration(unittest.TestCase):

    maxDiff = None

    def test_narrative_update_event(self):
        print('producing to', _CONFIG['topics']['workspace_events'])
        producer = Producer({'bootstrap.servers': _CONFIG['kafka_server']})
        producer.produce(
            _CONFIG['topics']['workspace_events'],
            json.dumps(_TEST_EVENTS['narrative_save']),
            callback=_delivery_report
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
            'copied': '1/2/3',
            'tags': [],
            'obj_type_name': 'Narrative',
            'obj_type_version': '4.0',
            'obj_type_module': 'KBaseNarrative'
        }
        self.assertEqual(msg_data['doc'], check_against)

    def test_genome_delete_event(self):
        print('producing to', _CONFIG['topics']['indexer_admin_events'])
        producer = Producer({'bootstrap.servers': _CONFIG['kafka_server']})
        producer.produce(
            _CONFIG['topics']['indexer_admin_events'],
            json.dumps(_TEST_EVENTS['deleted_object']),
            callback=_delivery_report
        )
        producer.poll(60)
        print('..finished producing OBJECT_DELETE_STATE_CHANGE event. Now consuming...')
        msg_data = _consume_last(_CONFIG['topics']['elasticsearch_updates'], b'delete')
        check_against = "41347:5"
        self.assertEqual(msg_data['id'], check_against)

    def test_global_permission_change(self):
        """
        Test a SET_GLOBAL_PERMISSION event
        """
        print('producing to', _CONFIG['topics']['workspace_events'])
        producer = Producer({'bootstrap.servers': _CONFIG['kafka_server']})
        producer.produce(
            _CONFIG['topics']['workspace_events'],
            json.dumps(_TEST_EVENTS['set_global_permission']),
            callback=_delivery_report
        )
        producer.poll(60)
        print('..finished producing SET_GLOBAL_PERMISSION event. Now consuming..')
        msg_data = _consume_last(_CONFIG['topics']['elasticsearch_updates'], b'make_public')
        print('MSG_DATA!', msg_data)


def _delivery_report(err, msg):
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
