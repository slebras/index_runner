"""
Main entrypoint for the app and the Kafka topic consumer.
Sends work to the es_indexer or the releng_importer.
Generally handles every message synchronously. Duplicate the service to get more parallelism.
"""
import logging
import json
import time
import os
import atexit
import signal
import hashlib
from kbase_workspace_client import WorkspaceClient
from kbase_workspace_client.exceptions import WorkspaceResponseError

from src.index_runner import event_loop
import src.utils.kafka as kafka
from src.utils.logger import init_logger
import src.utils.es_utils as es_utils
import src.utils.re_client as re_client
import src.index_runner.es_indexer as es_indexer
import src.index_runner.releng_importer as releng_importer
from src.utils.config import config
from src.utils.service_utils import wait_for_dependencies

logger = logging.getLogger('IR')
ws_client = WorkspaceClient(url=config()['kbase_endpoint'], token=config()['ws_token'])


def _handle_msg(msg):
    event_type = msg.get('evtype')
    if not event_type:
        msg = f"Missing 'evtype' in event: {msg}"
        logger.error(msg)
        raise RuntimeError(msg)
    objtype = msg.get('objtype')
    if objtype is not None and isinstance(objtype, str) and len(objtype) > 0:
        # Check the type against the configured whitelist or blacklist, if present
        whitelist = config()['allow_types']
        blacklist = config()['skip_types']
        if whitelist is not None and objtype not in whitelist:
            logger.warning(f"Type {msg['objtype']} is not in ALLOW_TYPES, skipping")
            return
        if blacklist is not None and objtype in blacklist:
            logger.warning(f"Type {msg['objtype']} is in SKIP_TYPES, skipping")
            return
    if event_type in ['REINDEX', 'NEW_VERSION', 'COPY_OBJECT', 'RENAME_OBJECT']:
        obj = _fetch_obj_data(msg)
        ws_info = _fetch_ws_info(msg)
        if not config()['skip_releng']:
            releng_importer.run_importer(obj, ws_info, msg)
        es_indexer.run_indexer(obj, ws_info, msg)
    elif event_type == 'REINDEX_WS' or event_type == 'CLONE_WORKSPACE':
        # Reindex all objects in a workspace, overwriting existing data
        for objinfo in ws_client.generate_obj_infos(msg['wsid'], admin=True):
            objid = objinfo[0]
            kafka.produce({'evtype': 'REINDEX', 'wsid': msg['wsid'], 'objid': objid},
                          callback=_delivery_report)
    elif event_type == 'INDEX_NONEXISTENT_WS':
        # Reindex all objects in a workspace without overwriting any existing data
        for objinfo in ws_client.generate_obj_infos(msg['wsid'], admin=True):
            objid = objinfo[0]
            kafka.produce({'evtype': 'INDEX_NONEXISTENT', 'wsid': msg['wsid'], 'objid': objid},
                          callback=_delivery_report)
    elif event_type == 'INDEX_NONEXISTENT':
        # Import to RE if we are not skipping RE and also it does not exist in the db
        re_required = not config()['skip_releng'] and not re_client.check_doc_existence(msg['wsid'], msg['objid'])
        # Index in elasticsearch if it does not exist there by ID
        es_required = not es_utils.check_doc_existence(msg['wsid'], msg['objid'])
        if not re_required and not es_required:
            # Skip any indexing/importing of this object
            return
        # We need to either index or import the object
        obj = _fetch_obj_data(msg)
        ws_info = _fetch_ws_info(msg)
        if re_required:
            releng_importer.run_importer(obj, ws_info, msg)
        if es_required:
            es_indexer.run_indexer(obj, ws_info, msg)
    elif event_type == 'OBJECT_DELETE_STATE_CHANGE':
        # Delete the object on RE and ES. Synchronous for now.
        es_indexer.delete_obj(msg)
        if not config()['skip_releng']:
            releng_importer.delete_obj(msg)
    elif event_type == 'WORKSPACE_DELETE_STATE_CHANGE':
        # Delete everything in RE and ES under this workspace
        es_indexer.delete_ws(msg)
        if not config()['skip_releng']:
            releng_importer.delete_ws(msg)
    elif event_type == 'SET_GLOBAL_PERMISSION':
        # Set the `is_public` permissions for a workspace
        es_indexer.set_perms(msg)
        if not config()['skip_releng']:
            releng_importer.set_perms(msg)
    elif event_type == 'RELOAD_ELASTIC_ALIASES':
        # Reload aliases on ES from the global config file
        es_indexer.reload_aliases()
    else:
        msg = f"Unrecognized event {event_type}."
        logger.error(msg)
        raise RuntimeError(msg)


def _log_msg_to_elastic(msg):
    """
    Save every message consumed from Kafka to an Elasticsearch index for logging purposes.
    """
    # The key is a hash of the message data body
    # The index document is the error string plus the message data itself
    ts = msg.get('time', int(time.time() * 1000))
    es_indexer._write_to_elastic([{
        'index': config()['msg_log_index_name'],
        'id': ts,
        'doc': msg
    }])


def _fetch_obj_data(msg):
    if not msg.get('wsid') or not msg.get('objid'):
        raise RuntimeError(f'Cannot get object ref from msg: {msg}')
    obj_ref = f"{msg['wsid']}/{msg['objid']}"
    if msg.get('ver'):
        obj_ref += f"/{msg['ver']}"
    try:
        obj_data = ws_client.admin_req('getObjects', {
            'objects': [{'ref': obj_ref}]
        })
    except WorkspaceResponseError as err:
        logger.error(f'Workspace response error: {err.resp_data}')
        # Workspace is deleted; ignore the error
        if (err.resp_data and isinstance(err.resp_data, dict)
                and err.resp_data['error'] and isinstance(err.resp_data['error'], dict)
                and err.resp_data['error'].get('code') == -32500):
            return
        else:
            raise err
    result = obj_data['data'][0]
    if not obj_data or not obj_data['data'] or not obj_data['data'][0]:
        logger.error(obj_data)
        raise RuntimeError("Invalid object result from the workspace")
    return result


def _fetch_ws_info(msg):
    if not msg.get('wsid'):
        raise RuntimeError(f'Cannot get workspace info from msg: {msg}')
    try:
        ws_info = ws_client.admin_req('getWorkspaceInfo', {
            'id': msg['wsid']
        })
    except WorkspaceResponseError as err:
        logger.error(f'Workspace response error: {err.resp_data}')
        raise err
    return ws_info


def _log_err_to_es(msg, err=None):
    """Log an indexing error in an elasticsearch index."""
    # The key is a hash of the message data body
    # The index document is the error string plus the message data itself
    _id = hashlib.blake2b(json.dumps(msg).encode('utf-8')).hexdigest()
    es_indexer._write_to_elastic([
        {
            'index': config()['error_index_name'],
            'id': _id,
            'doc': {'error': str(err), **msg}
        }
    ])


def _delivery_report(err, msg):
    if err is not None:
        logger.error(f'Message delivery failed:\n{err}')
    else:
        logger.info(f'Message delivered to {msg.topic()}')


def main():
    # Set up the logger
    # Make the urllib debug logs less noisy
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    init_logger(logger)

    # Initialize and run the Kafka consumer
    topics = [
        config()['topics']['workspace_events'],
        config()['topics']['admin_events']
    ]
    consumer = kafka.init_consumer(topics)
    atexit.register(lambda signum, stack_frame: kafka.close_consumer(consumer))
    signal.signal(signal.SIGTERM, lambda signum, stack_frame: kafka.close_consumer(consumer))
    signal.signal(signal.SIGINT, lambda signum, stack_frame: kafka.close_consumer(consumer))

    # Run the main thread
    event_loop.start_loop(
        consumer,
        _handle_msg,
        on_success=_log_msg_to_elastic,
        on_failure=_log_err_to_es,
        on_config_update=es_indexer.reload_aliases,
        logger=logger)


if __name__ == '__main__':
    # Remove the ready indicator file if it has been written on a previous boot
    if os.path.exists(config()['proc_ready_path']):
        os.remove(config()['proc_ready_path'])
    # Wait for dependency services (ES and RE) to be live
    wait_for_dependencies(timeout=180)
    # Database initialization
    es_indexer.init_indexes()
    es_indexer.reload_aliases()
    # Touch a temp file indicating the daemon is ready
    with open(config()['proc_ready_path'], 'w') as fd:
        fd.write('')
    main()
