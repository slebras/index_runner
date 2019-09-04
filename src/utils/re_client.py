"""
Relation engine API client functions.
"""
import json
import requests

from src.utils.config import get_config

_CONFIG = get_config()


def stored_query(name, params):
    """Run a stored query."""
    resp = requests.post(
        _CONFIG['re_api_url'] + '/api/v1/query_results',
        params={'stored_query': name},
        data=json.dumps(params),
    )
    if not resp.ok:
        raise RuntimeError(resp.text)
    return resp.json()


def get_doc(coll, key):
    """Fetch a doc in a collection by key."""
    resp = requests.post(
        _CONFIG['re_api_url'] + '/api/v1/query_results',
        data=json.dumps({
            'query': "for v in @@coll filter v._key == @key limit 1 return v",
            '@coll': coll,
            'key': key
        }),
        headers={'Authorization': _CONFIG['re_api_token']}
    )
    if not resp.ok:
        raise RuntimeError(resp.text)
    return resp.json()


def check_doc_existence(_id):
    """Check if a doc exists in RE already by full ID."""
    (coll, key) = _id.split('/')
    query = """
    for d in @@coll filter d._key == @key limit 1 return 1
    """
    resp = requests.post(
        _CONFIG['re_api_url'] + '/api/v1/query_results',
        data=json.dumps({
            'query': query,
            '@coll': coll,
            'key': key
        }),
        headers={'Authorization': _CONFIG['re_api_token']}
    )
    if not resp.ok:
        raise RuntimeError(resp.text)
    return resp.json()['count'] > 0


def get_edge(coll, from_key, to_key):
    """Fetch an edge by from and to keys."""
    query = """
    for v in @@coll
        filter v._from == @from AND v._to == @to
        limit 1
        return v
    """
    resp = requests.post(
        _CONFIG['re_api_url'] + '/api/v1/query_results',
        data=json.dumps({
            'query': query,
            '@coll': coll,
            'from': from_key,
            'to': to_key
        }),
        headers={'Authorization': _CONFIG['re_api_token']}
    )
    if not resp.ok:
        raise RuntimeError(resp.text)
    return resp.json()


def save(coll_name, docs):
    """
    Bulk-save documents to the relation engine database
    API docs: https://github.com/kbase/relation_engine_api
    Args:
        coll_name - collection name
        docs - list of dicts to save into the collection as json documents
    """
    url = _CONFIG['re_api_url'] + '/api/v1/documents'
    # convert the docs into a string, where each obj is separated by a linebreak
    payload = '\n'.join([json.dumps(d) for d in docs])
    params = {'collection': coll_name, 'on_duplicate': 'update'}
    resp = requests.put(
        url,
        data=payload,
        params=params,
        headers={'Authorization': _CONFIG['ws_token']}
    )
    if not resp.ok:
        raise RuntimeError(f'Error response from RE API: {resp.text}')
    return resp.json()
