"""
Takes workspace kafka event data and generates new Elasticsearch index upates
(creations, updates, deletes, etc)
"""
import json
import requests
import time
from enum import Enum

from src.utils.logger import logger
from src.utils.config import config
from src.utils.ws_utils import get_type_pieces
from src.index_runner.es_indexers.main import index_obj
from src.index_runner.es_indexers.indexer_utils import (
    check_object_deleted,
    check_workspace_deleted,
    is_workspace_public
)

_PREFIX = config()['elasticsearch_index_prefix']
_ES_URL = config()['elasticsearch_url']
_IDX = _PREFIX + ".*"
_GLOBAL_MAPPINGS = config()['global']['global_mappings']
_HEADERS = {"Content-Type": "application/json"}
_MAPPINGS = config()['global']['mappings']
_DEFAULT_SEARCH_ALIAS = 'default_search'


def init_indexes():
    """
    Initialize Elasticsearch indexes using the global configuration file.
    """
    for index, mapping in _MAPPINGS.items():
        global_mappings = {}  # type: dict
        if mapping.get('global_mappings'):
            for g_map in mapping['global_mappings']:
                global_mappings.update(_GLOBAL_MAPPINGS[g_map])
        _init_index(index, {**mapping['properties'], **global_mappings})


def reload_aliases():
    """
    Create aliases on elasticsearch from the global configuration file.
    """
    # FIXME Currently, this function only adds new indexes to aliases.
    # In the future, we want to remove aliases that exist on elastic
    # but not in the config.
    if config()['global'].get('aliases'):
        group_aliases = config()['global']['aliases']
        for alias_name in group_aliases:
            try:
                _create_alias(
                    f"{_PREFIX}.{alias_name}",
                    [f"{_PREFIX }.{name}" for name in group_aliases[alias_name]]
                )
            except RuntimeError as err:
                names = group_aliases[alias_name]
                raise RuntimeError(f"Failed creating alias name: {alias_name}, "
                                   f"for indices: {names}..\nerror: {err}")
        logger.info("Reloaded elasticsearch aliases")


def run_indexer(obj, ws_info, msg):
    start = time.time()
    batch_writes = []
    for data in index_obj(obj, ws_info, msg):
        action = data['_action']
        if action == 'index':
            batch_writes.append(data)
            if len(batch_writes) >= config()['es_batch_writes']:
                count = len(batch_writes)
                _write_to_elastic(batch_writes)
                logger.info(f'Indexing of {count} docs on ES took {time.time() - start}s')
                start = time.time()
                batch_writes = []
        elif action == 'init_generic_index':
            _init_generic_index(data)
    if batch_writes:
        count = len(batch_writes)
        _write_to_elastic(batch_writes)
        logger.info(f'Indexing of {count} docs on ES took {time.time() - start}s')


def delete_obj(msg):
    """
    Checks that the received object is deleted, since the workspace object
    delete event can refer to either delete or undelete state changes.
    """
    wsid = msg['wsid']
    objid = msg['objid']
    if not check_object_deleted(wsid, objid):
        # Object is not deleted
        logger.info(f'Object {objid} in workspace {wsid} is not deleted')
        return
    # Perform the deletion
    query = {
        'bool': {
            'must': [
                {'term': {'access_group': wsid}},
                {'term': {'obj_id': objid}}
            ]
        }
    }
    json_body = json.dumps({'query': query})
    # Perform the delete_by_query using the elasticsearch http api.
    resp = requests.post(
        f"{_ES_URL}/{_IDX}/_delete_by_query",
        params={'conflicts': 'proceed'},
        data=json_body,
        headers={"Content-Type": "application/json"}
    )
    if not resp.ok:
        # Unsuccesful request to elasticsearch.
        raise RuntimeError(f"Error deleting object on elasticsearch:\n{resp.text}")
    logger.info(f"Deleted elasticsearch documents associated with obj {wsid}/{objid}")


def delete_ws(msg):
    """
    Delete everything under a workspace.
    First checks that the received workspace is deleted because the
    delete event can refer to both delete or undelete state changes.
    """
    # Verify that this workspace is actually deleted
    wsid = msg['wsid']
    if not check_workspace_deleted(wsid):
        logger.info(f'Workspace {wsid} not deleted')
        return
    # Delete everything with the given workspace ID
    query = {'term': {'access_group': wsid}}
    json_body = json.dumps({'query': query})
    # Perform the delete_by_query using the elasticsearch http api.
    resp = requests.post(
        f"{_ES_URL}/{_IDX}/_delete_by_query",
        params={'conflicts': 'proceed'},
        data=json_body,
        headers={"Content-Type": "application/json"}
    )
    if not resp.ok:
        # Unsuccesful request to elasticsearch.
        raise RuntimeError(f"Error deleting workspace on elasticsearch:\n{resp.text}")


def set_perms(msg):
    """
    Set the `is_public` field for a workspace. Handles the SET_GLOBAL_PERMISSION event.
    eg. this happens when making a narrative public.
    """
    # Fetch the permission for the workspace
    wsid = msg['wsid']
    is_public = is_workspace_public(wsid)
    is_public_str = 'true' if is_public else 'false'
    _update_by_query(
        {'term': {'access_group': wsid}},
        f"ctx._source.is_public={is_public_str}",
        config()
    )


# -- Utils

def _init_generic_index(msg):
    """
    Initialize an index from a workspace object indexed by the generic indexer.
    For example, when the generic indexer gets a type like Module.Type-4.0,
    then we create an index called "search2.type_0".
    Message fields:
        full_type_name - string - eg. "Module.Type-X.Y"
    """
    (_, type_name, type_ver) = get_type_pieces(msg['full_type_name'])
    index_name = type_name.lower() + '_0'
    mappings = {**_GLOBAL_MAPPINGS['ws_auth'], **_GLOBAL_MAPPINGS['ws_object']}
    _init_index(index_name, mappings)
    # Update the 'default_search' alias to include this index
    _create_alias(_DEFAULT_SEARCH_ALIAS, f"{_PREFIX}.{index_name}")


def _write_to_elastic(data):
    """
    Bulk save a list of documents to an index.
    Each entry in the list has {doc, id, index}
        doc - document data (for indexing events)
        id - document id
        index - index name
        delete - bool (for delete events)
    """
    # Construct the post body for the bulk index
    json_body = ''
    while data:
        datum = data.pop()
        idx = f"{_PREFIX}.{datum['index']}"
        json_body += json.dumps({
            'index': {
                '_index': idx,
                '_id': datum['id']
            }
        })
        json_body += '\n'
        json_body += json.dumps(datum['doc'])
        json_body += '\n'
    # Save the documents using the elasticsearch http api
    resp = requests.post(f"{_ES_URL}/_bulk", data=json_body, headers={"Content-Type": "application/json"})
    if not resp.ok:
        # Unsuccessful save to elasticsearch.
        raise RuntimeError(f"Error saving to elasticsearch:\n{resp.text}")


def _update_by_query(query, script, config):
    url = f"{_ES_URL}/{_IDX}/_update_by_query"
    resp = requests.post(
        url,
        params={
            'conflicts': 'proceed',
            'wait_for_completion': 'true',
            'refresh': 'true',
        },
        data=json.dumps({
            'query': query,
            'script': {'inline': script, 'lang': 'painless'}
        }),
        headers={'Content-Type': 'application/json'}
    )
    if not resp.ok:
        raise RuntimeError(f'Error updating by query:\n{resp.text}')


def _create_alias(alias_name, index_names):
    """
    Create an alias from `alias_name` to the  `index_names`.
    NOTE: index_names can be a string or list of strings
    """
    body = {'actions': [{'add': {'indices': index_names, 'alias': alias_name}}]}
    url = _ES_URL + '/_aliases'
    resp = requests.post(url, data=json.dumps(body), headers=_HEADERS)
    if not resp.ok:
        raise RuntimeError(f"Error creating alias '{alias_name}':\n{resp.text}")
    return Status.CREATED


def _create_index(index_name):
    """
    Create an index on Elasticsearch with a given name.
    """
    request_body = {
        "settings": {
            "index": {
                "number_of_shards": config()['generic_shard_count'],
                "number_of_replicas": config()['generic_replica_count'],
            }
        }
    }
    url = _ES_URL + '/' + index_name
    resp = requests.put(url, data=json.dumps(request_body), headers=_HEADERS)
    if not resp.ok:
        err_type = resp.json()['error']['type']
        if err_type == 'resource_already_exists_exception':
            return Status.EXISTS
        else:
            raise RuntimeError(f"Error while creating new index {index_name}:\n{resp.text}")
    else:
        return Status.CREATED  # created


def _put_mapping(index_name, mapping):
    """
    Create or update the type mapping for a given index.
    """
    # type_name = config()['global']['es_type_global_name']
    url = f"{_ES_URL}/{index_name}/_mapping"
    resp = requests.put(
        url,
        data=json.dumps({'properties': mapping}),
        headers=_HEADERS
    )
    if not resp.ok:
        raise RuntimeError(f"Error updating mapping for index {index_name}:\n{resp.text}")
    return Status.UPDATED


def _init_index(name, props):
    """
    Initialize an index on elasticsearch if it doesn't already exist.
    Message fields:
        name - index name
        props - property type mappings
    """
    index_name = f"{_PREFIX}.{name}"
    status = _create_index(index_name)
    if status == Status.CREATED:
        logger.info(f"index {index_name} created.")
    elif status == Status.EXISTS:
        logger.info(f"index {index_name} already exists.")
    # Update the type mapping
    _put_mapping(index_name, props)


class Status(Enum):
    """Simple enum for ES update statuses."""
    UPDATED = 0
    CREATED = 1
    EXISTS = 2
