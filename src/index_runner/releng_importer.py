"""
Relation Engine (ArangoDB) data importer.

Writes data to arangodb from workspace update events.
"""
import logging
import time

from src.utils.config import config
from src.index_runner.releng.import_obj import import_object
from src.index_runner.releng.del_obj import delete_object

logger = logging.getLogger('IR')

# Initialize configuration data


def run_importer(obj, ws_info, msg):
    start = time.time()
    import_object(obj, ws_info)
    logger.info(f"Imported an object into RE in {time.time() - start}s.")


def delete_obj(msg):
    """
    Handle an object deletion event (OBJECT_DELETE_STATE_CHANGE)
    Delete everything that was created for this object. This is the inverse
    operation of the import_obj action.
    """
    obj_ref = f"{msg['wsid']}/{msg['objid']}"
    if msg.get("ver"):
        obj_ref += f"/{msg['ver']}"
    obj_info = config()['ws_client'].admin_req('getObjectInfo', {
        'objects': [{'ref': obj_ref}]
    })['infos'][0]
    delete_object(obj_info)


def delete_ws(msg):
    """Handle a workspace deletion event (WORKSPACE_DELETE_STATE_CHANGE)."""
    logger.info('_delete_ws TODO')  # TODO
    # raise NotImplementedError()


def set_perms(msg):
    """Set permissions for an entire workspace (SET_GLOBAL_PERMISSION)."""
    logger.info('_set_global_perms TODO')  # TODO
    # raise NotImplementedError()
