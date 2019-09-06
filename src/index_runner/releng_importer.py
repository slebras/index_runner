"""
Relation Engine (ArangoDB) data importer.

Writes data to arangodb from workspace update events.
"""
from src.utils.workspace_client import download_info
from src.index_runner.releng.import_obj import import_object
from src.utils.re_client import check_doc_existence

# Initialize configuration data


class RelengImporter:

    def ws_event(self, msg):
        """Receive a kafka message."""
        event_type = msg.get('evtype')
        wsid = msg.get('wsid')
        if not wsid:
            raise RuntimeError(f'Invalid wsid in event: {wsid}')
        if not event_type:
            raise RuntimeError(f"Missing 'evtype' in event: {msg}")
        print(f'Received {msg["evtype"]} for {wsid}/{msg.get("objid", "?")}')
        if event_type in ['IMPORT', 'NEW_VERSION', 'COPY_OBJECT', 'RENAME_OBJECT']:
            _import_obj(msg)
        elif event_type == 'IMPORT_NONEXISTENT':
            _import_nonexistent(msg)
        elif event_type == 'OBJECT_DELETE_STATE_CHANGE':
            _delete_obj(msg)
        elif event_type == 'WORKSPACE_DELETE_STATE_CHANGE':
            _delete_ws(msg)
        elif event_type in ['CLONE_WORKSPACE', 'IMPORT_WORKSPACE']:
            _import_ws(msg)
        elif event_type == 'SET_GLOBAL_PERMISSION':
            _set_global_perms(msg)
        else:
            raise RuntimeError(f"Unrecognized event {event_type}.")


def _import_obj(msg):
    print('Downloading obj')
    obj_info = download_info(msg['wsid'], msg['objid'], msg.get('ver'))
    import_object(obj_info)


def _import_nonexistent(msg):
    """Import an object only if it does not exist in RE already."""
    upa = ':'.join([str(p) for p in [msg['wsid'], msg['objid'], msg['ver']]])
    print(f'_import_nonexistent on {upa}')  # TODO
    _id = 'wsfull_object_version/' + upa
    exists = check_doc_existence(_id)
    if not exists:
        _import_obj(msg)


def _delete_obj(msg):
    """Handle an object deletion event (OBJECT_DELETE_STATE_CHANGE)"""
    print('_delete_obj TODO')  # TODO
    raise NotImplementedError()


def _delete_ws(msg):
    """Handle a workspace deletion event (WORKSPACE_DELETE_STATE_CHANGE)."""
    print('_delete_ws TODO')  # TODO
    raise NotImplementedError()


def _import_ws(msg):
    """Import all data for an entire workspace."""
    print('_import_ws TODO')  # TODO
    raise NotImplementedError()


def _set_global_perms(msg):
    """Set permissions for an entire workspace (SET_GLOBAL_PERMISSION)."""
    print('_set_global_perms TODO')  # TODO
    raise NotImplementedError()
