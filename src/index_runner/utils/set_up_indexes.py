import sys
import json
from confluent_kafka import Producer

from . import ws_type
from .config import get_config


_HEADERS = {"Content-Type": "application/json"}
_CONFIG = get_config()
# Universal type mappings for every doc in every index.

_GLOBAL_MAPPINGS = _CONFIG['global']['global_mappings']['ws_object']
_MAPPINGS = _CONFIG['global']['mappings']
_ALIASES = _CONFIG['global']['aliases']


def set_up_indexes():
    print("Setting up indices...")
    producer = Producer({'bootstrap.servers': _CONFIG['kafka_server']})
    for index, mapping in _MAPPINGS.items():
        global_mappings = {}  # type: dict
        for g_map in mapping['global_mappings']:
            global_mappings.update(_GLOBAL_MAPPINGS[g_map])
        producer.produce(
            _CONFIG['topics']['elasticsearch_updates'],
            json.dumps({
                'name': index,
                'alias': _ALIASES[index],
                'props': {**mapping['properties'], **global_mappings}  # type: ignore
            }),
            'init_index',
            callback=_delivery_report
        )
        producer.poll(60)


def set_up_generic_index(full_type_name):
    """
    Set up an index for a generic type for which we don't have a specific
    indexer function.
    """
    producer = Producer({'bootstrap.servers': _CONFIG['kafka_server']})
    (module_name, type_name, type_ver) = ws_type.get_pieces(full_type_name)
    index_name = type_name.lower()
    producer.produce(
        _CONFIG['topics']['elasticsearch_updates'],
        json.dumps({
            'name': index_name + ':0',
            'alias': index_name,
            'props': _GLOBAL_MAPPINGS['ws_object']
        }),
        'init_index',
        callback=_delivery_report
    )
    producer.poll(60)


def _delivery_report(err, msg):
    if err is not None:
        sys.stderr.write(f'Message delivery failed for {msg.key()} in {msg.topic()}: {err}\n')
    else:
        print(f'Message "{msg.key()}" delivered to {msg.topic()}')
