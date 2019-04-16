"""
Indexer logic based on type
"""
from kbase_workspace_client import WorkspaceClient
from kbase_workspace_client.exceptions import WorkspaceResponseError

from ..utils.config import get_config
from .narrative import index_narrative


def index_obj(msg_data):
    """
    For a newly created object, generate the index document for it and push to
    the elasticsearch topic on Kafka.

    Args:
        msg_data - json event data received from the kafka workspace events stream
    """
    # Fetch the object data from the workspace API
    upa = _get_upa_from_msg_data(msg_data)
    config = get_config()
    es_index_prefix = config.get('elasticsearch_index_prefix')
    ws_url = config['workspace_url']
    ws_client = WorkspaceClient(url=ws_url, token=config['ws_token'])
    upa = _get_upa_from_msg_data(msg_data)
    try:
        obj_data = ws_client.admin_req('getObjects', {
            'objects': [{'ref': upa}]
        })
    except WorkspaceResponseError as err:
        print('Workspace response error:', err.resp_data)
        raise err
    try:
        ws_info = ws_client.admin_req('getWorkspaceInfo', {
            'ws_id': msg_data['wsid']
        })
    except WorkspaceResponseError as err:
        print('Workspace response error:', err.resp_data)
        raise err
    # Dispatch to a specific type handler to produce the search document
    (type_module_name, type_version) = msg_data['objtype'].split('-')
    (type_module, type_name) = type_module_name.split('.')
    indexer = _find_indexer(type_module, type_name, type_version)
    return indexer(obj_data, ws_info, es_index_prefix)


def _find_indexer(type_module, type_name, type_version):
    """
    Find the indexer function for the given object type within the indexer_directory list.
    """
    for entry in indexer_directory:
        module_match = ('module' not in entry) or entry['module'] == type_module
        name_match = ('type' not in entry) or entry['type'] == type_name
        ver_match = ('version' not in entry) or entry['version'] == type_version
        if module_match and name_match and ver_match:
            return entry['indexer']
    return default_indexer


def default_indexer(obj_data, ws_info, index_prefix):
    """
    Default indexer fallback, when we don't find a type-specific handler.
    """
    return {'schema': {}, 'data': obj_data}


def _add_default_fields(data):
    """
    function to add recurring fields
    """
    return {
        "timestamp": data['epoch'],
        "guid": "WS:" + '/'.join([str(data['info'][6]), str(data['info'][0]), str(data['info'][4])]),

    }


# Directory of all indexer functions.
# Higher up in the list gets higher precedence.
indexer_directory = [
    {'module': 'KBaseNarrative', 'type': 'Narrative', 'indexer': index_narrative}
]


def _get_upa_from_msg_data(event_data):
    """Get the UPA workspace reference from a Kafka workspace event payload."""
    ws_id = event_data.get('wsid')
    if not ws_id:
        raise RuntimeError(f'Event data missing the "wsid" field for workspace ID: {event_data}')
    obj_id = event_data.get('objid')
    if not ws_id:
        raise RuntimeError(f'Event data missing the "objid" field for object ID: {event_data}')
    obj_ver = event_data.get('ver')
    if not ws_id:
        raise RuntimeError(f'Event data missing the "ver" field for object ID: {event_data}')
    return f"{ws_id}/{obj_id}/{obj_ver}"
