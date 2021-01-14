from kbase_workspace_client.exceptions import WorkspaceResponseError
import logging

from src.utils.config import config
from src.utils.logger import logger
from src.utils.ws_utils import get_type_pieces, log_error

_REF_DATA_WORKSPACES = []  # type: list


def check_object_deleted(ws_id, obj_id):
    """
    We check an object is deleted by listing the object in a workspace and
    making sure the object we are looking for is missing.

    We want to do this because the DELETE event can correspond to more than
    just an object deletion, so we want to make sure the object is deleted
    """
    try:
        narr_data_obj_info = config()['ws_client'].admin_req("listObjects", {'ids': [ws_id]})
    except WorkspaceResponseError as err:
        log_error(err, logging.WARNING)
        narr_data_obj_info = []
    # Make sure obj_id is not in list of object ids (this means it is deleted)
    obj_ids = [obj[0] for obj in narr_data_obj_info]
    return obj_id not in obj_ids


def is_workspace_public(ws_id):
    """
    Check if a workspace is public, returning bool.
    """
    ws_info = config()['ws_client'].admin_req('getWorkspaceInfo', {'id': ws_id})
    global_read = ws_info[6]
    return global_read != 'n'


def check_workspace_deleted(ws_id):
    """
    Since the DELETE_WORKSPACE event can correspond to workspace undeletion as well as deletion,
    we make sure that the workspace is deleted. This is done by making sure we get an excpetion
    with the word 'delete' in the error body.
    """
    try:
        config()['ws_client'].admin_req("getWorkspaceInfo", {
            'id': ws_id
        })
    except WorkspaceResponseError as err:
        if 'delete' in err.resp_text:
            return True
    return False


def get_shared_users(ws_id):
    """
    Get the list of users that have read, write, or author access to a workspace object.
    Args:
        ws_id - workspace id of requested workspace object
    """
    try:
        obj_perm = config()['ws_client'].admin_req("getPermissionsMass", {
            'workspaces': [{'id': ws_id}]
        })['perms'][0]
    except WorkspaceResponseError as err:
        logger.error("Workspace response error: ", err.resp_data)
        raise err
    shared_users = []
    for username, user_perms in obj_perm.items():
        if user_perms in ['a', 'r', 'w'] and username != '*':
            shared_users.append(username)
    return shared_users


def _get_tags(ws_info):
    """Get the tags relevant to search from the ws_info metadata"""
    metadata = ws_info[-1]
    if metadata.get('searchtags'):
        if isinstance(metadata['searchtags'], list):
            return metadata['searchtags']
        else:
            return [metadata['searchtags']]
    else:
        return []


def merge_default_fields(indexer_ret, defaults):
    """
    Merge default fields into existing default fields (if they exist)
    All compound values are wrapped in lists without unique values
    Singletons are not wrapped in lists, while null vals are None
    """
    if 'doc' not in indexer_ret:
        raise ValueError(f"indexer return data should have 'doc' dictionary {indexer_ret}")
    # Merge each list inside each dict
    for key, default_val in defaults.items():
        val = indexer_ret['doc'].get(key)
        val_is_list = isinstance(val, list)
        def_is_list = isinstance(default_val, list)
        if val is None:
            val = default_val
        elif val_is_list and def_is_list:
            val = _remove_dupes(val + default_val)
        elif val_is_list and default_val not in val:
            val.append(default_val)
        elif def_is_list and val not in default_val:
            default_val.append(val)
            val = default_val
        elif not val_is_list and not def_is_list and val != default_val:
            val = [val, default_val]
        indexer_ret['doc'][key] = val
    # Remove duplicates
    for key, val in indexer_ret['doc'].items():
        if isinstance(val, list):
            indexer_ret['doc'][key] = _remove_dupes(val)
    return indexer_ret


def _remove_dupes(ls: list) -> list:
    """Remove duplicate values from a list."""
    try:
        # Try the more efficient method first
        return list(set(ls))
    except TypeError:
        # Fall back to a slower method
        ret = []
        for v in ls:
            if v not in ret:
                ret.append(v)
        return ret


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
    tags = _get_tags(ws_info)
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


def handle_id_to_file(handle_id, dest_path):
    """given handle id, download associated file from shock."""
    shock_id = config()['ws_client'].handle_to_shock(handle_id)
    config()['ws_client'].download_shock_file(shock_id, dest_path)


def mean(array):
    """
    get mean of list, returns None if length is less than 1
    """
    if not array:
        return None
    return float(sum(array))/float(len(array))
