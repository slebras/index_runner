"""
Elasticsearch data writer.

Receives messages from index_runner.
"""
import zmq
import json
import requests
from dataclasses import dataclass, field
from enum import Enum

from .utils.config import get_config
from .utils.ws_utils import get_obj_ids_from_ws, get_type_pieces

# How many documents we accumulate before writing in bulk
_BULK_MAX = 10000

# Initialize configuration data
_CONFIG = get_config()
_ES_URL = _CONFIG['elasticsearch_url']
_PREFIX = _CONFIG['elasticsearch_index_prefix']
_IDX = _PREFIX + ".*"
_HEADERS = {"Content-Type": "application/json"}
_GLOBAL_MAPPINGS = _CONFIG['global']['global_mappings']
_MAPPINGS = _CONFIG['global']['mappings']
_ALIASES = _CONFIG['global']['aliases']


@dataclass
class ESWriter:
    queue_uri: str  # inproc address of the zmq queue device
    batch_writes: list = field(default_factory=list)  # accumulator of documents to write to ES
    batch_deletes: list = field(default_factory=list)  # accumulator of document IDs to delete from ES
    bulk_max = 10000

    def __post_init__(self):
        context = zmq.Context.instance()
        self.sock = context.socket(zmq.REP)  # Socket for sending replies to index_runner
        self.sock.connect(self.queue_uri)
        print("Initializing all ES indices and mappings from the global config")
        for index, mapping in _MAPPINGS.items():
            global_mappings = {}  # type: dict
            for g_map in mapping['global_mappings']:
                global_mappings.update(_GLOBAL_MAPPINGS[g_map])
            self._init_index({
                'name': index,
                'alias': _ALIASES.get(index),
                'props': {**mapping['properties'], **global_mappings}
            })
        # Start the event loop
        self._run()

    def _run(self):
        """
        Run the event loop, receiving messages over self.sock
        """
        poller = zmq.Poller()
        poller.register(self.sock, zmq.POLLIN)
        # We use a zmq poller so that we can receive messages with a timeout
        # If we timeout and don't receive a message for a while, then we do some work.
        # We also do some work if we hit the _BULK_MAX threshold.
        poller = zmq.Poller()
        poller.register(self.sock, zmq.POLLIN)
        print('poller', poller)
        # Main event loop
        while True:
            polled = poller.poll(30000)  # timeout at 30 seconds
            print('polled', polled, self.sock)
            if self.sock in dict(polled):
                msg = self.sock.recv_json()
                try:
                    self._recv(msg)
                finally:
                    print('es_writer sending back..')
                    self.sock.send(b'')
            else:
                # Timed out waiting for a message.
                # Make bulk updates and clear out the accumulators on timeout.
                self._perform_batch_ops()

    def _recv(self, msg):
        """
        Receive a JSON message over self.sock.
        Message "action" name should go in msg._action.
        """
        print('es_writer received', msg)
        if not msg.get('_action'):
            raise RuntimeError(f"Message to elasticsearch writer missing `_action` field: {msg}")
        action = msg['_action']
        if action == 'delete':
            self.batch_deletes.append(msg)
        elif action == 'index':
            self.batch_writes.append(msg)
        elif action == 'init_index':
            self._init_index(msg)
        elif action == 'init_generic_index':
            self._init_generic_index(msg)
        elif action == 'set_global_perm':
            self._set_global_perm(msg)
        self._perform_batch_ops()

    def _perform_batch_ops(self):
        """Perform all the batch writes and deletes and empty the lists."""
        write_len = len(self.batch_writes)
        delete_len = len(self.batch_deletes)
        if write_len:
            _write_to_elastic(self.batch_writes)
            self.batch_writes = []
            print(f"es_writer Wrote {write_len} documents to elasticsearch.")
        if delete_len:
            _delete_from_elastic(self.batch_deletes)
            self.batch_deletes = []
            print(f"es_writer Deleted {delete_len} documents from elasticsearch.")

    def _init_index(self, msg):
        """
        Initialize an index on elasticsearch if it doesn't already exist.
        Message fields:
            name - index name
            alias - optional - index alias
            props - property type mappings
        """
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
            alias_name = f"{_PREFIX}.{msg['alias']}"
            status = _create_alias(alias_name, index_name)
            print(f"es_writer Alias {alias_name} for index {index_name} created.")

    def _set_global_perm(self, msg):
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

    def _init_generic_index(self, msg):
        """
        Initialize an index from a workspace object indexed by the generic indexer.
        For example, when the generic indexer gets a type like Module.Type-4.0,
        then we create an index called "search2.type:0".
        Message fields:
            full_type_name - string - eg. "Module.Type-X.Y"
        """
        (module_name, type_name, type_ver) = get_type_pieces(msg['full_type_name'])
        index_name = type_name.lower()
        self._init_index({
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
    type_name = _CONFIG['global']['es_type_global_name']
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
    # Save the documents using the elasticsearch http api
    resp = requests.post(f"{_ES_URL}/_bulk", data=json_body, headers={"Content-Type": "application/json"})
    if not resp.ok:
        # Unsuccesful save to elasticsearch.
        raise RuntimeError(f"Error saving to elasticsearch:\n{resp.text}")


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
