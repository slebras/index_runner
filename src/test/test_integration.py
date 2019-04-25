import unittest
import json
import time
import requests
from confluent_kafka import Producer, Consumer, KafkaError

from index_runner.utils.config import get_config

config = get_config()

# load elasticsearch options
es_host = config['elasticsearch_host']
es_port = config['elasticsearch_port']
es_data_type = config['elasticsearch_data_type']

es_url = "http://" + es_host + ":" + str(es_port)

headers = {
    "Content-Type": "application/json"
}

test_events = {
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
        'bootstrap.servers': config['kafka_server'],
        'group.id': 'test_only',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([topic])
    # partition = TopicPartition(config['topics']['elasticsearch_updates'], 0)
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

    def test_consumer_timout(self):
        print('producing to', config['topics']['workspace_events'])
        producer = Producer({
            'bootstrap.servers': config['kafka_server']
        })
        producer.produce(
            config['topics']['workspace_events'],
            json.dumps(test_events['narrative_save']),
            callback=delivery_report
        )
        producer.poll(60)
        print('..finished producing, now consuming. This may take a couple minutes as the workers restart...')
        # We expect this to fail with timeout of 1 second.
        with self.assertRaises(Exception) as context:
            consume_last(config['topics']['elasticsearch_updates'], timeout=1)

        self.assertIn("Consumer waited past timeout", str(context.exception))

    def test_narrative_update_event(self):
        print('producing to', config['topics']['workspace_events'])
        producer = Producer({
            'bootstrap.servers': config['kafka_server']
        })
        producer.produce(
            config['topics']['workspace_events'],
            json.dumps(test_events['narrative_save']),
            callback=delivery_report
        )
        producer.poll(60)
        print('..finished producing, now consuming. This may take a couple minutes as the workers restart...')
        msg_data = consume_last(config['topics']['elasticsearch_updates'])

        # TODO: update missing fields "accgrp", "shared_users", "creation_date", and "public"
        check_against = {
            "name": "Test Narrative Name",
            "upa": "41347:1:16",
            "data_objects": [
                {
                    'name': 'Rhodobacter_CACIA_14H1',
                    'type': 'KBaseGenomes.Genome-7.0'
                }, {
                    'name': '_Nostoc_azollae__0708',
                    'type': 'KBaseGenomes.Genome-14.2'
                }, {
                    'name': 'Acetobacter_aceti_NBRC_14818',
                    'type': 'KBaseGenomes.Genome-14.1'
                }, {
                    'name': '_H9',
                    'type': 'KBaseBiochem.Media-1.0'
                }, {
                    'name': 'KBase_derived_Rhodobacter_CACIA_14H1.gff_genome.assembly',
                    'type': 'KBaseGenomeAnnotations.Assembly-6.0'
                }, {
                    'name': 'KBase_derived_Rhodobacter_CACIA_14H1.gff_genome',
                    'type': 'KBaseGenomes.Genome-15.1'
                }
            ],
            "cells": [
                {
                    'description': 'Testing\n',
                    'type': 'markdown'
                }, {
                    'description': "print('nope')",
                    'type': 'code_cell'
                }, {
                    'description': 'Import GFF3/FASTA file as Genome from Staging Area',
                    'type': 'kbase_app'
                }
            ],
            "creator": "username",
            "shared_users": ['username'],
            "total_cells": 3,
            "access_group": 41347,
            "public": True,
            "islast": True,
            "shared": False,
            "timestamp": 1554408998887,
            "guid": "WS:41347/1/16",
            "creation_date": '2019-04-04T20:16:39+0000',
        }

        self.assertEqual(msg_data['doc'], check_against)

        log_data = consume_last(config['topics']['indexer_logs'])
        print(log_data)

        try:
            resp = requests.get(
                "/".join([es_url, msg_data['index'], es_data_type, msg_data['id']]),
                headers=headers
            )
        except requests.exceptions.RequestException as error:
            raise error

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
        # TODO test for msg on indexer_logs topic
        # TODO test for doc on elasticsearch
