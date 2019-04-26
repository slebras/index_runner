import requests
import json
from .config import get_config

config = get_config()

_ES_HOST = config['elasticsearch_host']
_ES_PORT = config['elasticsearch_port']
_ES_DATA_TYPE = config.get('elasticsearch_data_type', 'data')
_ES_URL = "http://" + _ES_HOST + ":" + str(_ES_PORT)
_ES_INDEX_PREFIX = config.get('elasticsearch_index_prefix')
_HEADERS = {"Content-Type": "application/json"}
_GLOBAL_MAPPINGS = {
    'timestamp': {'type': 'date'},
    'creation_date': {'type': 'date'}
}
_MAPPINGS = {
    'narrative:1': {
        'alias': 'narrative',
        'properties': {
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
            'creator': {'type': 'keyword'},
            'shared_users': {'type': 'keyword'},
            'total_cells': {'type': 'short'},
            'access_group': {'type': 'integer'},
            'is_public': {'type': 'boolean'}
        }
    }
}


def _create_index(index_name, mapping):
    """
    Create an index on Elasticsearch using our mapping definitions above.
    """
    request_body = {
        "mappings": {
            _ES_DATA_TYPE: {
                "properties": {**mapping['properties'], **_GLOBAL_MAPPINGS}
            }
        },
        "settings": {
            "index": {
                "number_of_shards": 10,
                "number_of_replicas": 2
            }
        }
    }
    url = _ES_URL + '/' + index_name
    resp = requests.put(url, data=json.dumps(request_body), headers=_HEADERS)
    if not resp.ok:
        raise RuntimeError(f"Error while creating new index {index_name}:\n{resp.text}")


def _create_alias(alias_name, index_name):
    """
    Create an alias from `alias_name` to the  `index_name`.
    """
    body = {
        'actions': [{'add': {'index': index_name, 'alias': alias_name}}]
    }
    url = _ES_URL + '/_aliases'
    resp = requests.post(url, data=json.dumps(body), headers=_HEADERS)
    if not resp.ok:
        raise RuntimeError(f"Error creating alias '{alias_name}':\n{resp.text}")


def set_up_indexes():
    print("setting up indices...")
    # Fetch all alias names
    aliases_resp = requests.get(_ES_URL + "/_aliases?format=json")
    if not aliases_resp.ok:
        raise RuntimeError('\n'.join([
            f"Error querying aliases: {aliases_resp.text}",
            f"Exited with status code {aliases_resp.status_code}"
        ]))
    # This will be a dict where the keys are index names. Each value is a dict
    # with a key for "aliases".
    indices = aliases_resp.json()
    for index, mapping in _MAPPINGS.items():
        index_name = f"{_ES_INDEX_PREFIX}.{index}"
        alias_name = f"{_ES_INDEX_PREFIX}.{mapping['alias']}"
        if index_name not in indices:
            print(f"Creating new index '{index_name}' and alias '{alias_name}'.")
            _create_index(index_name, mapping)
            _create_alias(alias_name, index_name)
            continue
        print(f"Index '{index_name}' already created.")
        # Index is created, but let's check for aliases
        aliases = indices[index_name].get('aliases', {})
        if alias_name not in aliases:
            print(f"Creating alias '{alias_name}'.")
            _create_alias(alias_name, index_name)
        else:
            print(f"Alias '{alias_name}' already created.")
    print("all indices loaded...")
