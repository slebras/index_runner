from kbase_workspace_client import WorkspaceClient
from kbase_workspace_client.exceptions import WorkspaceResponseError

from ..utils.config import get_config


def get_shared_users(data):
    """
    returns list of users that have read, write, or author access to a workspace object

    inputs:
        data: data from workspace_client.getObjects(...)['data'][idx]

    """
    config = get_config()
    ws_url = config['workspace_url']
    ws_client = WorkspaceClient(url=ws_url, token=config['ws_token'])

    try:
        obj_perm = ws_client.admin_req("getPermissionsMass", {
            'workspaces': [{'id': data['info'][6]}]
        })['perms'][0]
    except WorkspaceResponseError as err:
        print("Workspace response error: ", err.resp_data)
        raise err

    shared_users = []
    for username, user_perms in obj_perm.items():
        if user_perms in ['a', 'r', 'w']:
            shared_users.append(username)

    return shared_users
