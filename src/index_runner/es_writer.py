"""
Elasticsearch data writer.

Receives messages from index_runner.
"""
import json
import requests
import time
from enum import Enum
from kbase_workspace_client import WorkspaceClient

from src.utils.config import config
from src.utils.ws_utils import get_type_pieces

# Initialize configuration data
_ES_URL = config()['elasticsearch_url']
_PREFIX = config()['elasticsearch_index_prefix']
_WS_CLIENT = WorkspaceClient(url=config()['kbase_endpoint'], token=config()['ws_token'])
_IDX = _PREFIX + ".*"
_HEADERS = {"Content-Type": "application/json"}
_GLOBAL_MAPPINGS = config()['global']['global_mappings']
_MAPPINGS = config()['global']['mappings']
_ALIASES = config()['global']['aliases']


class ESWriter:
    # Minimum amount of ES updates before we perform a batch operation
    batch_min = 10000

    def __init__(self):
        """Initialize the indices, aliases, and type mappings on ES."""
        print("Initializing all ES indices and mappings from the global config:")
        self.batch_writes = []  # type: list
        self.batch_deletes = []  # type: list
        for index, mapping in _MAPPINGS.items():
            global_mappings = {}  # type: dict
            if mapping.get('global_mappings'):
                for g_map in mapping['global_mappings']:
                    global_mappings.update(_GLOBAL_MAPPINGS[g_map])
            self.init_index({
                'name': index,
                'alias': _ALIASES.get(index),
                'props': {**mapping['properties'], **global_mappings}
            })

    def on_queue_empty(self):
        """
        Runs on a 5s timeout receiving a message.
        Perform our built-up batch operations while we're idle.
        """
        self._perform_batch_writes(min_length=1)
        self._perform_batch_deletes(min_length=1)

    def delete(self, data):
        """
        Handle a delete action.
        Data is a dict with fields for 'workspace_id' (int) and 'object_id' (int)
        """
        self.batch_deletes.append(data)
        self._perform_batch_deletes(min_length=self.batch_min)

    def index(self, data):
        """
        Handle an index action.
        Data is a dict with fields for 'index' (index name), 'id' (ES id), and 'doc' (ES data)
        """
        self.batch_writes.append(data)
        self._perform_batch_writes(min_length=self.batch_min)

    def _perform_batch_writes(self, min_length=1):
        """
        Perform all the batch writes and empty the batch_writes list.
        Runs after 5s of inactivity or if the batch ops reach a min length.
        """
        write_len = len(self.batch_writes)
        if write_len >= min_length:
            _write_to_elastic(self.batch_writes)
            self.batch_writes = []
            print(f"es_writer wrote {write_len} documents to elasticsearch.")

    def _perform_batch_deletes(self, min_length=1):
        """
        Perform all the batch deletes and empty the batch_deletes list.
        Runs after 5s of inactivity or if the batch ops reach a min length.
        """
        # Only perform batch ops at most once every `self.batch_interval` seconds
        delete_len = len(self.batch_deletes)
        if delete_len >= min_length:
            _delete_from_elastic(self.batch_deletes)
            self.batch_deletes = []
            print(f"es_writer deleted {delete_len} documents from elasticsearch.")

    def init_index(self, msg):
        """
        Initialize an index on elasticsearch if it doesn't already exist.
        Message fields:
            name - index name
            alias - optional - index alias
            props - property type mappings
        """
        # index_name = f"{_PREFIX}.{msg['namespace']}.{msg['name']}"  # namespace
        index_name = f"{_PREFIX}.{msg['name']}"
        status = _create_index(index_name)
        if status == Status.CREATED:
            print(f"es_writer Index {index_name} created.")
        elif status == Status.EXISTS:
            print(f"es_writer Index {index_name} already exists.")
        # Update the type mapping
        _put_mapping(index_name, msg['props'])
        # Create the alias
        if msg.get('alias'):
            # alias_name = f"{_PREFIX}.{msg['namespace']}.{msg['alias']}"
            alias_name = f"{_PREFIX}.{msg['alias']}"
            status = _create_alias(alias_name, index_name)
            print(f"es_writer Alias {alias_name} for index {index_name} created.")

    def set_global_perm(self, msg):
        """
        Make all objects in a certain workspace either all public or all private.
        """
        workspace_id = int(msg['workspace_id'])
        is_public_str = 'true' if msg.get('is_public') else 'false'
        _update_by_query(
            {'term': {'access_group': workspace_id}},
            f"ctx._source.is_public={is_public_str}",
            config()
        )

    def init_generic_index(self, msg):
        """
        Initialize an index from a workspace object indexed by the generic indexer.
        For example, when the generic indexer gets a type like Module.Type-4.0,
        then we create an index called "search2.type:0".
        Message fields:
            full_type_name - string - eg. "Module.Type-X.Y"
        """
        (module_name, type_name, type_ver) = get_type_pieces(msg['full_type_name'])
        index_name = type_name.lower()
        self.init_index({
            'name': index_name + ':0',
            'props': _GLOBAL_MAPPINGS['ws_object']
        })


# -- Utils

def _create_alias(alias_name, index_name):
    """
    Create an alias from `alias_name` to the  `index_name`.
    """
    body = {'actions': [{'add': {'index': index_name, 'alias': alias_name}}]}
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
            return Status.EXISTS
        else:
            raise RuntimeError(f"Error while creating new index {index_name}:\n{resp.text}")
    else:
        return Status.CREATED  # created


def _put_mapping(index_name, mapping):
    """
    Create or update the type mapping for a given index.
    """
    type_name = config()['global']['es_type_global_name']
    url = f"{_ES_URL}/{index_name}/_mapping/{type_name}"
    resp = requests.put(url, data=json.dumps({'properties': mapping}), headers=_HEADERS)
    if not resp.ok:
        raise RuntimeError(f"Error updating mapping for index {index_name}:\n{resp.text}")
    return Status.UPDATED


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
            # TODO: update to check/work for multiple versions.
            for (obj_id, ver) in _WS_CLIENT.generate_all_ids_for_workspace(wsid):
                id_set.add(f"WS::{wsid}:{obj_id}")
        else:
            id_set.add(f"WS::{msg['object_id']}")
    for _id in id_set:
        should_body.append({'term': {'_id': _id}})
    json_body = json.dumps({'query': {'bool': {'should': should_body}}})
    # Perform the delete_by_query using the elasticsearch http api.
    resp = requests.post(
        f"{_ES_URL}/{_IDX}/_delete_by_query",
        params={'conflicts': 'proceed'},
        data=json_body,
        headers={"Content-Type": "application/json"}
    )
    if not resp.ok:
        # Unsuccesful save to elasticsearch.
        raise RuntimeError(f"Error saving to elasticsearch:\n{resp.text}")


def _write_to_elastic(data):
    """
    Bulk save a list of documents to an index.
    Each entry in the list has {doc, id, index}
        doc - document data (for indexing events)
        id - document id
        index - index name
        delete - bool (for delete events)
    """
    start = time.time()
    es_type = config()['global']['es_type_global_name']
    # Construct the post body for the bulk index
    json_body = ''
    while data:
        datum = data.pop()
        json_body += json.dumps({
            'index': {
                # '_index': f"{_PREFIX}.{datum['namespace']}.{datum['index']}",
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
    print(f'write_to_elastic took {time.time() - start}s')


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


class Status(Enum):
    """Simple enum for ES update statuses."""
    UPDATED = 0
    CREATED = 1
    EXISTS = 2
