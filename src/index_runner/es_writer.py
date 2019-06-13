"""
Elasticsearch data writer.
"""
import time
import json
import requests

from .utils.logger import log
from .utils.config import get_config
from .utils.ws_utils import get_obj_ids_from_ws, get_type_pieces

# How many documents we accumulate before writing in bulk
_WRITE_THRESHOLD = 1000

# Initialize configuration data
_CONFIG = get_config()
_ES_URL = _CONFIG['elasticsearch_url']
_PREFIX = _CONFIG['elasticsearch_index_prefix']
_IDX = _PREFIX + ".*"
_HEADERS = {"Content-Type": "application/json"}
_GLOBAL_MAPPINGS = _CONFIG['global']['global_mappings']
_MAPPINGS = _CONFIG['global']['mappings']
_ALIASES = _CONFIG['global']['aliases']


def main(queue):
    """
    Main event loop. `queue` is a thread queue that will have update/delete
    actions that we aggregate and perform in bulk in the handler functions
    below.
    """
    # Accumulators for bulk http actions on ES
    state = {
        'batch_writes': [],
        'batch_deletes': []
    }  # type: dict
    # Set up ES indices for any that don't exist
    for index, mapping in _MAPPINGS.items():
        global_mappings = {}  # type: dict
        for g_map in mapping['global_mappings']:
            global_mappings.update(_GLOBAL_MAPPINGS[g_map])
        _handle_init_index({
            'name': index,
            'alias': _ALIASES.get(index),
            'props': {**mapping['properties'], **global_mappings}
        }, state)
    # Main event loop
    while True:
        while queue.qsize():
            msg = queue.get()
            if not msg.get('_action') or (msg['_action'] not in _MSG_HANDLERS):
                raise RuntimeError(f"Invalid message to elasticsearch writer: {msg}")
            _MSG_HANDLERS[msg['_action']](msg, state)  # type: ignore
        if state['batch_writes']:
            _write_to_elastic(state['batch_writes'])
        if state['batch_deletes']:
            _delete_from_elastic(state['batch_deletes'])
        time.sleep(1)


def _handle_delete(msg, state):
    """Push a delete action to the batch_deletes list."""
    state['batch_deletes'].append(msg)
    if len(state['batch_deletes']) >= _WRITE_THRESHOLD:
        _delete_from_elastic(state['batch_deletes'])  # Empties batch_deletes


def _handle_index(msg, state):
    """Push an update action to the batch_writes list."""
    state['batch_writes'].append(msg)
    if len(state['batch_writes']) >= _WRITE_THRESHOLD:
        _write_to_elastic(state['batch_writes'])  # Empties batch_writes


def _handle_set_global_perm(msg, state):
    """
    Make all objects in a certain workspace either all public or all private.
    """
    workspace_id = int(msg['workspace_id'])
    is_public_str = 'true' if msg.get('is_public') else 'false'
    _update_by_query(
        {'term': {'access_group': workspace_id}},
        f"ctx._source.is_public={is_public_str}",
        _CONFIG
    )


def _handle_init_index(msg, state):
    """
    Initialize an index on elasticsearch if it doesn't already exist.
    Message fields:
        name - index name
        alias - optional - index alias
        props - property type mappings
    """
    index_name = f"{_PREFIX}.{msg['name']}"
    _create_index(index_name)
    # Update the type mapping
    _put_mapping(index_name, msg['props'])
    # Create the alias
    if msg.get('alias'):
        alias_name = f"{_PREFIX}.{msg['alias']}"
        _create_alias(alias_name, index_name)


def _handle_init_generic_index(msg, state):
    """
    Initialize an index from a workspace object indexed by the generic indexer.
    For example, when the generic indexer gets a type like Module.Type-4.0,
    then we create an index called "search2.type:0".
    Message fields:
        full_type_name - string - eg. "Module.Type-X.Y"
    """
    (module_name, type_name, type_ver) = get_type_pieces(msg['full_type_name'])
    index_name = type_name.lower()
    _handle_init_index({
        'name': index_name + ':0',
        'props': _GLOBAL_MAPPINGS['ws_object']
    }, state)


_MSG_HANDLERS = {
    'delete': _handle_delete,
    'index': _handle_index,
    'init_index': _handle_init_index,
    'init_generic_index': _handle_init_generic_index,
    'set_global_perm': _handle_set_global_perm,
}


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
    print(f'Created alias from {alias_name} to {index_name}')


def _create_index(index_name):
    """
    Create an index on Elasticsearch with a given name.
    """
    request_body = {
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
        err_type = resp.json()['error']['type']
        if err_type == 'index_already_exists_exception':
            print(f"Index '{index_name}' already exists.")
        else:
            raise RuntimeError(f"Error while creating new index {index_name}:\n{resp.text}")


def _put_mapping(index_name, mapping):
    """
    Create or update the type mapping for a given index.
    """
    type_name = _CONFIG['global']['es_type_global_name']
    url = f"{_ES_URL}/{index_name}/_mapping/{type_name}"
    resp = requests.put(url, data=json.dumps({'properties': mapping}), headers=_HEADERS)
    if not resp.ok:
        raise RuntimeError(f"Error updating mapping for index {index_name}:\n{resp.text}")
    print('Updated mapping', resp.text)


def _delete_from_elastic(batch_deletes):
    """
    Given a list of messages with field 'id' (in form 'workspace_id' or 'workspace_id:object_id'),
    constructs a bulk delete_by_query request for elasticsearch.
    Mutates `data`, emptying the list.
    """
    # Construct the post body for the bulk index
    should_body = []
    # Make sure we don't use same id more than once.
    id_set = set()
    while batch_deletes:
        msg = batch_deletes.pop()
        if msg.get('workspace_id'):
            wsid = msg['workspace_id']
            for obj_id in get_obj_ids_from_ws(wsid):
                id_set.add(f"WS::{wsid}:{obj_id}")
        else:
            id_set.add(f"WS::{msg['object_id']}")
    for _id in id_set:
        should_body.append({'term': {'_id': _id}})
    json_body = json.dumps({'query': {'bool': {'should': should_body}}})
    # Perform the delete_by_query using the elasticsearch http api.
    resp = requests.post(
        f"{_ES_URL}/{_IDX}/_delete_by_query",
        data=json_body,
        headers={"Content-Type": "application/json"}
    )
    if not resp.ok:
        # Unsuccesful save to elasticsearch.
        raise RuntimeError(f"Error saving to elasticsearch:\n{resp.text}")
    log('info', f"Elasticsearch delete by query successful.")


def _write_to_elastic(data):
    """
    Bulk save a list of documents to an index.
    Each entry in the list has {doc, id, index}
        doc - document data (for indexing events)
        id - document id
        index - index name
        delete - bool (for delete events)
    """
    es_type = _CONFIG['global']['es_type_global_name']
    # Construct the post body for the bulk index
    json_body = ''
    log('info', f"Bulk saving {len(data)} documents..")
    while data:
        datum = data.pop()
        json_body += json.dumps({
            'index': {
                '_index': f"{_PREFIX}.{datum['index']}",
                '_type': es_type,
                '_id': datum['id']
            }
        })
        json_body += '\n'
        json_body += json.dumps(datum['doc'])
        json_body += '\n'
    # Save the documents using the elasticsearch http api
    resp = requests.post(f"{_ES_URL}/_bulk", data=json_body, headers={"Content-Type": "application/json"})
    if not resp.ok:
        # Unsuccesful save to elasticsearch.
        raise RuntimeError(f"Error saving to elasticsearch:\n{resp.text}")
    log('info', f"Elasticsearch bulk save successful.")


def _update_by_query(query, script, config):
    url = f"{_ES_URL}/{_IDX}/_update_by_query"
    resp = requests.post(
        url,
        params={
            'conflicts': 'proceed',
            'wait_for_completion': True,
            'refresh': True
        },
        data=json.dumps({
            'query': query,
            'script': {'inline': script, 'lang': 'painless'}
        }),
        headers={'Content-Type': 'application/json'}
    )
    if not resp.ok:
        raise RuntimeError(f'Error updating by query:\n{resp.text}')
