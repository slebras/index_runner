"""Elasticsearch API client utilities."""
import json
import requests

from .config import get_config

# Initialize configuration data
_CONFIG = get_config()
_PREFIX = _CONFIG['elasticsearch_index_prefix']
_ES_URL = "http://" + _CONFIG['elasticsearch_host'] + ":" + str(_CONFIG['elasticsearch_port'])


def does_doc_exist(wsid, objid):
    """Check if a document exists on elasticsearch based on workspace and object id."""
    _id = f"{_PREFIX}::{wsid}:{objid}"
    resp = requests.post(
        _ES_URL + f"/{_PREFIX}.*",
        data=json.dumps({'query': {'term': {'_id': _id}}}),
        params={'size': 0}
    )
    if not resp.ok:
        raise RuntimeError(f"Unexpected elasticsearch server error:\n{resp.text}")
    total = resp.json()['hits']['total']
    return total > 0
