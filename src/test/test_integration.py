import unittest
import json
from confluent_kafka import Producer, Consumer, KafkaError

from index_runner.utils.config import get_config


config = get_config()

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


def consume_last(topic):
    """Consume the most recent message from the topic stream."""
    consumer = Consumer({
        'bootstrap.servers': config['kafka_server'],
        'group.id': 'test_only',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([topic])
    # partition = TopicPartition(config['topics']['elasticsearch_updates'], 0)
    # consumer.seek(0)
    while True:
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
        # b'{"mapping": , "doc": }'

        self.assertEqual(msg_data['mapping'], {
            "name": {"type": "text"},
            "markdown_text": {"type": "text"},
            "app_names": {"type": "text"},
            "creator": {"type": "text"},
            "total_cells": {"type": "short"},
            "epoch": {"type": "date"}
        })

        self.assertEqual(msg_data['doc'], {
            "name": "wat",
            "markdown_text": ["Testing"],
            "app_names": [
                "kb_uploadmethods/import_gff_fasta_as_genome_from_staging",
                "kb_uploadmethods/import_gff_fasta_as_genome_from_staging"
            ],
            "creator": "jayrbolton",
            "total_cells": 5,
            "epoch": 1554408998887
        })
