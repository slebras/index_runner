"""
Elasticsearch data writer.
"""
import json
import requests

from .utils.logger import log
from .utils.config import get_config
from .utils.ws_utils import get_obj_ids_from_ws

# How many documents we accumulate before writing in bulk
_WRITE_THRESHOLD = 1000

# Initialize configuration data
_CONFIG = get_config()
_ES_URL = f"http://{_CONFIG['elasticsearch_host']}:{_CONFIG['elasticsearch_port']}"
_PREFIX = _CONFIG['elasticsearch_index_prefix']
_IDX = _PREFIX + ".*"


def main(queue):
    """
    Main event loop. `queue` is a thread queue that will have update/delete
    actions that we aggregate and perform in bulk in the handler functions
    below.
    """
    batch_writes = []  # type: list
    batch_deletes = []  # type: list
    while True:
        if queue.qsize():
            msg = queue.get()
            if msg['_action'] not in _message_handlers:
                raise RuntimeError(f"Invalid message to elasticsearch writer: {msg}")
            _message_handlers(msg, batch_writes, batch_deletes)  # type: ignore


def _handle_delete(msg, batch_writes, batch_deletes):
    """Push a delete action to the batch_deletes list."""
    batch_deletes.append(msg)
    if len(batch_deletes) >= _WRITE_THRESHOLD:
        _delete_from_elastic(batch_deletes)  # Empties batch_deletes


def _handle_index(msg, batch_writes, batch_deletes):
    """Push an update action to the batch_writes list."""
    batch_writes.append(msg)
    if len(batch_writes) >= _WRITE_THRESHOLD:
        _write_to_elastic(batch_writes)  # Empties batch_writes


def _handle_set_global_perm(msg, batch_writes, batch_deletes):
    """ """
    workspace_id = int(msg['workspace_id'])
    is_public_str = 'true' if msg.get('is_public') else 'false'
    _update_by_query(
        {'term': {'access_group': workspace_id}},
        f"ctx._source.is_public={is_public_str}",
        _CONFIG
    )


_message_handlers = {
    'delete': _handle_delete,
    'index': _handle_index,
    'set_global_perm': _handle_set_global_perm,
}


def _delete_from_elastic(data):
    """
    Given a list of messages with field 'id' (in form 'workspace_id' or 'workspace_id:object_id'),
    constructs a bulk delete_by_query request for elasticsearch.
    Mutates `data`, emptying the list.
    """
    # Construct the post body for the bulk index
    should_body = []
    # Make sure we don't use same id more than once.
    id_set = set()
    while data:
        msg = data.pop()
        if msg.get('workspace_id'):
            wsid = msg['workspace_id']
            for obj_id in get_obj_ids_from_ws(wsid):
                id_set.add(f"{wsid}:{obj_id}")
        else:
            id_set.add(f"{_PREFIX}::{msg['object_id']}")
    for id_ in id_set:
        prefix_body = {'prefix': {'guid': id_}}
        should_body.append(prefix_body)
    json_body = json.dumps({'query': {'bool': {'should': should_body}}})
    es_url = "http://" + _CONFIG['elasticsearch_host'] + ":" + str(_CONFIG['elasticsearch_port'])
    # Perform the delete_by_query using the elasticsearch http api.
    resp = requests.post(
        f"{es_url}/{_IDX}/_delete_by_query",
        data=json_body,
        headers={"Content-Type": "application/json"}
    )
    if not resp.ok:
        # Unsuccesful save to elasticsearch.
        raise RuntimeError(f"Error saving to elasticsearch:\n{resp.text}")
    _kafka_log({'_action': 'delete', 
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
    es_url = "http://" + _CONFIG['elasticsearch_host'] + ":" + str(_CONFIG['elasticsearch_port'])
    log('info', f"Bulk saving {len(data)} documents..")
    # Save the documents using the elasticsearch http api
    resp = requests.post(f"{es_url}/_bulk", data=json_body, headers={"Content-Type": "application/json"})
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
            'refesh': True
        },
        data=json.dumps({
            'query': query,
            'script': {'inline': script, 'lang': 'painless'}
        }),
        headers={'Content-Type': 'application/json'}
    )
    if not resp.ok:
        raise RuntimeError(f'Error updating by query:\n{resp.text}')
