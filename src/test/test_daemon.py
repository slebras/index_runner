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


def consume_idx_events():
    """Consume the most recent message from the topic stream."""
    consumer = Consumer({
        'bootstrap.servers': config['kafka_server'],
        'group.id': 'test_only',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([config['topics']['elasticsearch_updates']])
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
        print('new msg!', msg.value())
    consumer.close()


def produce_events():
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
    print('..produced')


if __name__ == '__main__':
    produce_events()
    consume_idx_events()
