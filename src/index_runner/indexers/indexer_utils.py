from kbase_workspace_client import WorkspaceClient
from kbase_workspace_client.exceptions import WorkspaceResponseError

from ..utils.config import get_config
from ..utils import ws_type

_REF_DATA_WORKSPACES = []  # type: list


def mean(array):
    """
    get mean of list, returns None if length is less than 1
    """
    if not array:
        return None
    return float(sum(array))/float(len(array))


def check_object_deleted(ws_id, obj_id):
    """
    We check an object is deleted by
    """
    config = get_config()
    ws_url = config['workspace_url']
    ws_client = WorkspaceClient(url=ws_url, token=config['ws_token'])
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


def check_workspace_deleted(ws_id):
    """
    """
    config = get_config()
    ws_url = config['workspace_url']
    ws_client = WorkspaceClient(url=ws_url, token=config['ws_token'])
    try:
        ws_client.ws_client.admin_req("getWorkspaceInfo", {
            'id': ws_id
        })
    except WorkspaceResponseError as err:
        if 'delete' in err.text:
            # we want this
            return True
    return False


def get_shared_users(ws_id):
    """
    Get the list of users that have read, write, or author access to a workspace object.

    Args:
        ws_id - workspace id of requested workspace object
    """
    config = get_config()
    ws_url = config['workspace_url']
    ws_client = WorkspaceClient(url=ws_url, token=config['ws_token'])
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
    config = get_config()
    ws_url = config['workspace_url']
    ws_client = WorkspaceClient(url=ws_url, token=config['ws_token'])
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
    Add fields that should be present in any document on elasticsearch.
    """
    ws_id = obj_data['info'][6]
    obj_id = obj_data['info'][0]
    version = obj_data['info'][4]
    v1_info = obj_data_v1['info']
    is_public = ws_info[6] == 'r'
    shared_users = get_shared_users(ws_id)
    copy_ref = obj_data.get('copied')
    obj_type = obj_data['info'][2]
    (type_module, type_name, type_version) = ws_type.get_pieces(obj_type)
    tags = []
    if ws_id in _REF_DATA_WORKSPACES:
        tags.append("refdata")

    return {
        "creator": obj_data["creator"],
        "access_group": ws_id,
        "obj_name": obj_data['info'][1],
        "shared_users": shared_users,
        "guid": ":".join([str(ws_id), str(obj_id)]),
        "timestamp": obj_data['epoch'],
        "creation_date": v1_info[3],
        "is_public": is_public,
        "version": version,
        "obj_id": obj_id,
        "copied": copy_ref,
        "tags": tags,
        "obj_type_version": type_version,
        "obj_type_module": type_module,
        "obj_type_name": type_name,
    }
