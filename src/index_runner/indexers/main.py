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
            'id': msg_data['wsid']
        })
    except WorkspaceResponseError as err:
        print('Workspace response error:', err.resp_data)
        raise err
    # get upa for first version of current object
    v1_upa = '/'.join(upa.split("/")[:-1]) + "/1"
    # we get the info of the first object to get the origin creation date
    # of the object
    try:
        obj_data_v1 = ws_client.admin_req('getObjects', {
            'objects': [{'ref': v1_upa}],
            'no_data': 1
        })
    except WorkspaceResponseError as err:
        print('Workspace response error:', err.resp_data)
        raise err
    # Dispatch to a specific type handler to produce the search document
    (type_module_name, type_version) = msg_data['objtype'].split('-')
    (type_module, type_name) = type_module_name.split('.')
    indexer = _find_indexer(type_module, type_name, type_version)
    if not indexer:
        # No indexer found for this type
        return
    indexer_ret = indexer(obj_data, ws_info, obj_data_v1)
    # Merge in default data into the document
    indexer_ret['doc'].update(_default_fields(obj_data, obj_data_v1))
    return indexer_ret


def _find_indexer(type_module, type_name, type_version):
    """
    Find the indexer function for the given object type within the indexer_directory list.
    """
    for entry in _INDEXER_DIRECTORY:
        module_match = ('module' not in entry) or entry['module'] == type_module
        name_match = ('type' not in entry) or entry['type'] == type_name
        ver_match = ('version' not in entry) or entry['version'] == type_version
        if module_match and name_match and ver_match:
            return entry['indexer']
    # No indexer found for this type
    return None


def _default_fields(obj_data, obj_data_v1):
    """
    Add fields that should be present in any document on elasticsearch.
    """
    data = obj_data['data'][0]
    v1_info = obj_data['data'][0]['info']
    return {
        "creator": data["creator"],
        "access_group": data['info'][6]
        "timestamp": data['epoch'],
        "creation_date": v1_info[3]
    }


# Directory of all indexer functions.
# Higher up in the list gets higher precedence.
_INDEXER_DIRECTORY = [
    {'module': 'KBaseNarrative', 'type': 'Narrative', 'indexer': index_narrative}
]


def _get_upa_from_msg_data(msg_data):
    """Get the UPA workspace reference from a Kafka workspace event payload."""
    ws_id = msg_data.get('wsid')
    if not ws_id:
        raise RuntimeError(f'Event data missing the "wsid" field for workspace ID: {msg_data}')
    obj_id = msg_data.get('objid')
    if not ws_id:
        raise RuntimeError(f'Event data missing the "objid" field for object ID: {msg_data}')
    obj_ver = msg_data.get('ver')
    if not ws_id:
        raise RuntimeError(f'Event data missing the "ver" field for object ID: {msg_data}')
    return f"{ws_id}/{obj_id}/{obj_ver}"
