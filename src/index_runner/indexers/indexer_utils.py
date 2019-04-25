from kbase_workspace_client import WorkspaceClient
from kbase_workspace_client.exceptions import WorkspaceResponseError

from ..utils.config import get_config


def get_shared_users(ws_id):
    """
    returns list of users that have read, write, or author access to a workspace object

    inputs:
        ws_id: workspace id of requested workspace object
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
        if user_perms in ['a', 'r', 'w']:
            shared_users.append(username)

    return shared_users


def get_narrative_data(ws_id):
    """
    returns list of dicts with keys 'type' and 'name' corrosponding to all data objects in
    the requested workspace.

    inputs:
        ws_id: a workspace id
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

    narrative_data = [
        {"name": obj[1], "type": obj[2]}
        for obj in narr_data_obj_info
        if 'KBaseNarrative' not in str(obj[2])
    ]

    return narrative_data
