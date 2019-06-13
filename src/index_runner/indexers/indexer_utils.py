from kbase_workspace_client import WorkspaceClient
from kbase_workspace_client.exceptions import WorkspaceResponseError

from ..utils.config import get_config
from ..utils.ws_utils import get_type_pieces

_REF_DATA_WORKSPACES = []  # type: list
_CONFIG = get_config()


def check_object_deleted(ws_id, obj_id):
    """
    We check an object is deleted by listing the object in a workspace and
    making sure the object we are looking for is missing.

    We want to do this because the DELETE event can correspond to more than
    just an object deletion, so we want to make sure the object is deleted
    """
    ws_url = _CONFIG['workspace_url']
    ws_client = WorkspaceClient(url=ws_url, token=_CONFIG['ws_token'])
    try:
        narr_data_obj_info = ws_client.admin_req("listObjects", {
            'ids': [ws_id]
        })
    except WorkspaceResponseError as err:
        print("Workspace response error: ", err.resp_data)
        # NOTE: not sure if we want to raise err here, worth thinking about
        raise err
    # make sure obj_id is not in list of object ids of workspace (this means its deleted)
    if obj_id not in [obj[0] for obj in narr_data_obj_info]:
        return True
    else:
        return False


def is_workspace_public(ws_id):
    """
    Check if a workspace is public, returning bool.
    """
    ws_url = _CONFIG['workspace_url']
    ws_client = WorkspaceClient(url=ws_url, token=_CONFIG['ws_token'])
    ws_info = ws_client.admin_req('getWorkspaceInfo', {'id': ws_id})
    global_read = ws_info[6]
    return global_read != 'n'


def check_workspace_deleted(ws_id):
    """
    Since the DELETE_WORKSPACE event can correspond to workspace undeletion as well as deletion,
    we make sure that the workspace is deleted. This is done by making sure we get an excpetion
    with the word 'delete' in the error body.
    """
    ws_url = _CONFIG['workspace_url']
    ws_client = WorkspaceClient(url=ws_url, token=_CONFIG['ws_token'])
    try:
        ws_client.admin_req("getWorkspaceInfo", {
            'id': ws_id
        })
    except WorkspaceResponseError as err:
        if 'delete' in err.text:
            return True
    return False


def get_shared_users(ws_id):
    """
    Get the list of users that have read, write, or author access to a workspace object.
    Args:
        ws_id - workspace id of requested workspace object
    """
    ws_url = _CONFIG['workspace_url']
    ws_client = WorkspaceClient(url=ws_url, token=_CONFIG['ws_token'])
    try:
        obj_perm = ws_client.admin_req("getPermissionsMass", {
            'workspaces': [{'id': ws_id}]
        })['perms'][0]
    except WorkspaceResponseError as err:
        print("Workspace response error: ", err.resp_data)
        raise err
    shared_users = []
    for username, user_perms in obj_perm.items():
        if user_perms in ['a', 'r', 'w'] and username != '*':
            shared_users.append(username)
    return shared_users


def fetch_objects_in_workspace(ws_id, include_narrative=False):
    """
    Get a list of dicts with keys 'type' and 'name' corresponding to all data
    objects in the requested workspace.
    Args:
        ws_id - a workspace id
    """
    ws_url = _CONFIG['workspace_url']
    ws_client = WorkspaceClient(url=ws_url, token=_CONFIG['ws_token'])
    try:
        narr_data_obj_info = ws_client.admin_req("listObjects", {
            "ids": [ws_id]
        })
    except WorkspaceResponseError as err:
        print("Workspace response error: ", err.resp_data)
        raise err
    if include_narrative:
        narrative_data = [
            {"obj_id": obj[0], "name": obj[1], "obj_type": obj[2], "ver": obj[4]}
            for obj in narr_data_obj_info
        ]
    else:
        narrative_data = [
            {"name": obj[1], "obj_type": obj[2]}
            for obj in narr_data_obj_info
            if 'KBaseNarrative' not in str(obj[2])
        ]
    return narrative_data


def default_fields(obj_data, ws_info, obj_data_v1):
    """
    Produce data for fields that are present in any workspace object document on elasticsearch.
    """
    ws_id = obj_data['info'][6]
    obj_id = obj_data['info'][0]
    version = obj_data['info'][4]
    v1_info = obj_data_v1['info']
    is_public = ws_info[6] == 'r'
    shared_users = get_shared_users(ws_id)
    copy_ref = obj_data.get('copied')
    obj_type = obj_data['info'][2]
    (type_module, type_name, type_version) = get_type_pieces(obj_type)
    tags = []
    if ws_id in _REF_DATA_WORKSPACES:
        tags.append("refdata")
    return {
        "creator": obj_data["creator"],
        "access_group": ws_id,
        "obj_name": obj_data['info'][1],
        "shared_users": shared_users,
        "timestamp": obj_data['epoch'],
        "creation_date": v1_info[3],
        "is_public": is_public,
        "version": version,
        "obj_id": obj_id,
        "copied": copy_ref,
        "tags": tags,
        "obj_type_version": type_version,
        "obj_type_module": type_module,
        "obj_type_name": type_name
    }


def get_upa_from_msg_data(msg_data):
    """Get the UPA workspace reference from a Kafka workspace event payload."""
    ws_id = msg_data.get('wsid')
    if not ws_id:
        raise RuntimeError(f'Event data missing the "wsid" field for workspace ID: {msg_data}')
    obj_id = msg_data.get('objid')
    if not obj_id:
        raise RuntimeError(f'Event data missing the "objid" field for object ID: {msg_data}')
    return f"{ws_id}/{obj_id}"


def mean(array):
    """
    get mean of list, returns None if length is less than 1
    """
    if not array:
        return None
    return float(sum(array))/float(len(array))
