import sys
import traceback
from kbase_workspace_client import WorkspaceClient

from .get_indexer_for_type import get_indexer_for_type
from ..utils.config import get_config


def new_object_version(event_data):
    """
    A new object version has been created on the workspace.
    Handles events NEW_ALL_VERSIONS or NEW_VERSION
    Args:
        event_data - json data from the kafka event
    """
    config = get_config()
    ws_url = config['kbase_endpoint'] + '/ws'
    ws_client = WorkspaceClient(url=ws_url, token=config['ws_token'])
    # New index for all object versions
    if event_data['evtype'] == 'NEW_ALL_VERSIONS':
        # Create an UPA without a version
        upa = f"{event_data['wsid']}/{event_data['objid']}"
        ws_resp = ws_client.admin_req('getObjectInfo', {
            'objects': [{'ref': upa}]
        })
        obj_info = ws_resp['infos'][0]
        vers = obj_info[4]
        event_data['ver'] = vers
        typ, ver = obj_info[2].split('-')
        event_data['objtype'] = typ
        event_data['objtypever'] = ver
        event_data['upa'] = f'{upa}/{vers}'
        print('new event data', event_data)
    indexes = get_indexer_for_type(event_data['objtype'])
    for oindex in indexes:
        try:
            if oindex.get('multi'):
                # _new_object_version_multi_index(event, oindex)
                # TODO
                print('_new_object_version_multi_index')
            elif oindex.get('raw'):
                # _new_raw_version_index(event, oindex)
                # TODO
                print('_new_raw_version_index')
            else:
                # _new_object_version_index(event, oindex)
                # TODO
                print('_new_object_version_index')
        except Exception as e:
            print('Failed for index', e)
            # (event, oindex, e)
            exc_type, exc_value, exc_traceback = sys.exc_info()
            print('=' * 80)
            traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
            traceback.print_exception(exc_type, exc_value, exc_traceback,
                                      limit=2, file=sys.stdout)
