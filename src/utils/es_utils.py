"""Elasticsearch API client utilities."""
import json
import requests

from src.utils.config import config

# Initialize configuration data
_PREFIX = config()['elasticsearch_index_prefix']
_ES_URL = "http://" + config()['elasticsearch_host'] + ":" + str(config()['elasticsearch_port'])


def check_doc_existence(wsid, objid):
    """Check if a document exists on elasticsearch based on workspace and object id."""
    _id = f"WS::{wsid}:{objid}"
    resp = requests.post(
        _ES_URL + f"/{_PREFIX}.*/_search",
        data=json.dumps({'query': {'term': {'_id': _id}}}),
        params={'size': 0},
        headers={'Content-Type': 'application/json'}
    )
    if not resp.ok:
        raise RuntimeError(f"Unexpected elasticsearch server error:\n{resp.text}")
    resp_json = resp.json()
    total = resp_json['hits']['total']['value']
    return total > 0


def _get_document(index_name, document_id):
    """ Get document (if it exists) in index from Elasticsearch, otherwise return None
    document - document id often in following form:
        "NAMESPACE::workspace_id:object_id(:object_version)" () - optional depending on index
        for further reference look at individual object type indexers.
    """
    es_url = f"{_ES_URL}/{_PREFIX}.{index_name}/_doc/{document_id}"
    resp = requests.get(url=es_url)
    # don't check status, will error if Document not found.
    respj = resp.json()
    if respj.get('error'):
        raise Exception(f"Query to {es_url} resulted in error - {respj['error']}")
    if respj['found']:
        return respj['_source']
    else:
        return None
