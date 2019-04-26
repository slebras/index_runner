import requests
import json
from .config import get_config

config = get_config()

_ES_HOST = config['elasticsearch_host']
_ES_PORT = config['elasticsearch_port']
_ES_DATA_TYPE = config["elasticsearch_data_type"]
_ES_URL = "http://" + _ES_HOST + ":" + str(_ES_PORT)
_ES_INDEX_PREFIX = config.get('elasticsearch_index_prefix')
_HEADERS = {"Content-Type": "application/json"}
_GLOBAL_MAPPINGS = {
    'timestamp': {'type': 'date'},
    'creation_date': {'type': 'date'}
}
_MAPPINGS = {
    "kbasenarrative.narrative-4.0": {
        'name': {'type': 'text'},
        'upa': {'type': 'keyword'},
        'data_objects': {
            'type': 'nested',
            'properties': {
                'name': {'type': 'keyword'},
                'obj_type': {'type': 'keyword'}
            }
        },
        'cells': {
            'type': 'object',
            'properties': {
                'desc': {'type': 'text'},
                'cell_type': {'type': 'keyword'}
            }
        },
        'creator': {'type': 'text'},
        'shared_users': {'type': 'text'},
        'total_cells': {'type': 'short'},
        'access_group': {'type': 'integer'},
        'public': {'type': 'boolean'},
        'islast': {'type': 'boolean'},
        'shared': {'type': 'boolean'}
    }
}


def _create_index(index, mapping):
    """
    Create an index on Elasticsearch using our mapping definitions above.
    """
    request_body = {
        "mappings": {
            _ES_DATA_TYPE: {
                "properties": {**mapping, **_GLOBAL_MAPPINGS}
            }
        },
        "settings": {
            "index": {
                "number_of_shards": 10,
                "number_of_replicas": 2
            }
        }
    }
    resp = requests.put(
        '/'.join([_ES_URL, index]),
        data=json.dumps(request_body),
        headers=_HEADERS
    )
    if not resp.ok:
        raise RuntimeError(f"Error while creating new index {index}: {resp.text}")


def set_up_indexes():
    print("setting up indices...")
    try:
        resp = requests.get(_ES_URL + "/_aliases")
    except requests.exceptions.RequestException as error:
        raise error
    if not resp.ok:
        raise RuntimeError("Error while querying for indices: " + resp.text +
                           ". Exited with status code %i" % resp.status_code)
    indexes_data = resp.json()
    indexes = indexes_data.keys()
    for index, mapping in _MAPPINGS.items():
        index = _ES_INDEX_PREFIX + '.' + index + "_1"
        if index in indexes:
            print("index %s already created" % index)
            continue
        print("creating new index %s" % index)
        # create index here using the mapping stored above.
        _create_index(index, mapping)
    print("all indices loaded...")
