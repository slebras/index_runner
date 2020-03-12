"""
Relation engine API client functions.
"""
import json
import re
import requests
import logging

from src.utils.config import config

# see https://www.arangodb.com/2018/07/time-traveling-with-graph-databases/
# in unix epoch ms this is 2255/6/5
MAX_ADB_INTEGER = 2**53 - 1

_ADB_KEY_DISALLOWED_CHARS_REGEX = re.compile(r"[^a-zA-Z0-9_\-:\.@\(\)\+,=;\$!\*'%]")

logger = logging.getLogger('IR')


# should this live somewhere else?
def clean_key(key):
    """
    Swaps disallowed characters for an ArangoDB '_key' with an underscore.
    See https://www.arangodb.com/docs/stable/data-modeling-naming-conventions-document-keys.html
    """
    return _ADB_KEY_DISALLOWED_CHARS_REGEX.sub('_', key)


def stored_query(name, params):
    """Run a stored query."""
    resp = requests.post(
        config()['re_api_url'] + '/api/v1/query_results',
        params={'stored_query': name},
        data=json.dumps(params),
    )
    if not resp.ok:
        raise RuntimeError(resp.text)
    return resp.json()


def clear_collection(collection_name):
    """
    Remove all the documents in a collection without affecting indexes.

    collection_name - the collection to clear.
    """
    resp = requests.post(
        config()['re_api_url'] + '/api/v1/query_results',
        data=json.dumps({
            'query': 'FOR d in @@col REMOVE(d) IN @@col',
            '@col': collection_name
        }),
        headers={'Authorization': config()['re_api_token']}
    )
    if not resp.ok:
        raise RuntimeError(resp.text)


def get_all_documents(collection_name):
    """
    Returns all the documents in a collection. Using this on a large collection is not advised.

    collection_name - the collection from which documents will be returned.
    """
    resp = requests.post(
        config()['re_api_url'] + '/api/v1/query_results',
        data=json.dumps({
            'query': 'FOR d in @@col RETURN d',
            '@col': collection_name
        }),
        headers={'Authorization': config()['re_api_token']}
    )
    if not resp.ok:
        raise RuntimeError(resp.text)
    return resp.json()


def get_doc(coll, key):
    """Fetch a doc in a collection by key."""
    resp = requests.post(
        config()['re_api_url'] + '/api/v1/query_results',
        data=json.dumps({
            'query': "for v in @@coll filter v._key == @key limit 1 return v",
            '@coll': coll,
            'key': key
        }),
        headers={'Authorization': config()['re_api_token']}
    )
    if not resp.ok:
        raise RuntimeError(resp.text)
    return resp.json()


def check_doc_existence(wsid, objid):
    """Check if a doc exists in RE already by full ID."""
    coll = 'ws_object'
    key = f"{wsid}:{objid}"
    query = """
    for d in @@coll filter d._key == @key limit 1 return 1
    """
    resp = requests.post(
        config()['re_api_url'] + '/api/v1/query_results',
        data=json.dumps({
            'query': query,
            '@coll': coll,
            'key': key
        }),
        headers={'Authorization': config()['re_api_token']}
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
        config()['re_api_url'] + '/api/v1/query_results',
        data=json.dumps({
            'query': query,
            '@coll': coll,
            'from': from_key,
            'to': to_key
        }),
        headers={'Authorization': config()['re_api_token']}
    )
    if not resp.ok:
        raise RuntimeError(resp.text)
    return resp.json()


def save(coll_name, docs, on_duplicate='update'):
    """
    Bulk-save documents to the relation engine database
    API docs: https://github.com/kbase/relation_engine_api
    Args:
        coll_name - collection name
        docs - single dict or list of dicts to save into the collection as json documents
        on_duplicate - what to do on a unique key collision. One of 'update', 'replace' 'ignore',
            'error'.
    """
    if isinstance(docs, dict):
        docs = [docs]
    url = config()['re_api_url'] + '/api/v1/documents'
    # convert the docs into a string, where each obj is separated by a linebreak
    payload = '\n'.join([json.dumps(d) for d in docs])
    params = {'collection': coll_name, 'on_duplicate': on_duplicate}
    params['display_errors'] = '1'
    resp = requests.put(
        url,
        data=payload,
        params=params,
        headers={'Authorization': config()['re_api_token']}
    )
    if not resp.ok:
        raise RuntimeError(f'Error response from RE API: {resp.text}')
    return resp.json()


def execute_query(query, params=None):
    """
    Execute an arbitrary query in the database.
    NOTE: be sure to guard against AQL injection when using this function.
    """
    if not params:
        params = {}
    params['query'] = query
    url = config()['re_api_url'] + '/api/v1/query_results'
    resp = requests.post(
        url,
        data=json.dumps(params),
        headers={'Authorization': config()['re_api_token']}
    )
    if not resp.ok:
        raise RuntimeError(resp.text)
    return resp.json()
