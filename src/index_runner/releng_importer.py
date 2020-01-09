"""
Relation Engine (ArangoDB) data importer.

Writes data to arangodb from workspace update events.
"""
import logging
import time

from src.index_runner.releng.import_obj import import_object

logger = logging.getLogger('IR')

# Initialize configuration data


def import_worker(work_queue):
    while True:
        (obj, ws_info, msg) = work_queue.get()
        start = time.time()
        import_object(obj, ws_info)
        logger.info(f"Imported an object into RE in {time.time() - start}s.")
        work_queue.task_done()


def delete_obj(msg):
    """Handle an object deletion event (OBJECT_DELETE_STATE_CHANGE)"""
    logger.info('_delete_obj TODO')  # TODO
    # raise NotImplementedError()


def delete_ws(msg):
    """Handle a workspace deletion event (WORKSPACE_DELETE_STATE_CHANGE)."""
    logger.info('_delete_ws TODO')  # TODO
    # raise NotImplementedError()


def set_perms(msg):
    """Set permissions for an entire workspace (SET_GLOBAL_PERMISSION)."""
    logger.info('_set_global_perms TODO')  # TODO
    # raise NotImplementedError()
