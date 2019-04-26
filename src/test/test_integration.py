import unittest
import json
import time
import requests
from confluent_kafka import Producer, Consumer, KafkaError

from index_runner.utils.config import get_config

_CONFIG = get_config()
_ES_DATA_TYPE = _CONFIG['elasticsearch_data_type']
_ES_URL = "http://" + _CONFIG['elasticsearch_host'] + ":" + str(_CONFIG['elasticsearch_port'])
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
    }
}


def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed:', err)
    else:
        print('Message delivered to', msg.topic(), msg.partition())


def consume_last(topic, timeout=120):
    """Consume the most recent message from the topic stream."""
    consumer = Consumer({
        'bootstrap.servers': _CONFIG['kafka_server'],
        'group.id': 'test_only',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([topic])
    # partition = TopicPartition(_CONFIG['topics']['elasticsearch_updates'], 0)
    # consumer.seek(0)
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
        # We got a message
        consumer.close()
        return json.loads(msg.value())


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
        msg_data = consume_last(_CONFIG['topics']['elasticsearch_updates'])
        check_against = {
            "name": "Test Narrative Name",
            "upa": "41347:1:16",
            "data_objects": [
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
            "shared_users": ['username'],
            "total_cells": 3,
            "access_group": 41347,
            "public": True,
            "timestamp": 1554408998887,
            "creation_date": '2019-04-04T20:16:39+0000'
        }
        self.assertEqual(msg_data['doc'], check_against)
        # Dumb way to wait for the elasticsearch document to save to the index
        time.sleep(15)
        # Make a request to elastic to fetch our new index document
        url = "/".join([_ES_URL, msg_data['index'], _ES_DATA_TYPE, msg_data['id']])
        resp = requests.get(url, headers=_HEADERS)
        if not resp.ok:
            raise RuntimeError("Failed to get id %s from index %s,"
                               " results in error: " % (msg_data['id'], msg_data['index']) + resp.text +
                               ". Exited with status code %i" % resp.status_code)
        resp_data = resp.json()
        if resp_data.get('error'):
            raise RuntimeError("Failed to get id %s from index %s,"
                               " results in error: " % (msg_data['id'], msg_data['index'])
                               + str(resp_data['error']['root_cause']))
        self.assertEqual(resp_data['_source'], check_against)
