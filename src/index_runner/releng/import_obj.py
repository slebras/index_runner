"""
Take object info from the workspace and import various vertices and edges into Arangodb.

ws_object (unversioned)
    workspace_id
    object_id
    deleted
    _key: wsid/objid
"""
import logging

from src.index_runner.releng import genome
from src.utils.ws_utils import get_type_pieces
from src.utils.re_client import save
from src.utils.config import config
from src.utils.formatting import ts_to_epoch, get_method_key_from_prov, get_module_key_from_prov
from kbase_workspace_client import WorkspaceClient

logger = logging.getLogger('IR')

# need version specific processors here? Or expect the processor to handle all versions?
# could also have an includes field to reduce the amount of data fetched from the ws
_TYPE_PROCESSOR_MAP = {
    'KBaseGenomes.Genome': genome.process_genome
}


def import_object(obj):
    """
    Import all the edges and vertices for a workspace object into RE.
    """
    # TODO handle the ws_latest_version_of edge -- some tricky considerations here
    # Save the ws_object document
    obj_info = obj['info']
    wsid = obj_info[6]
    objid = obj_info[0]
    ws_info = _get_ws_info(wsid)
    obj_key = f'{wsid}:{objid}'
    obj_ver = obj_info[4]
    obj_ver_key = f'{obj_key}:{obj_ver}'

    _save_ws_object(obj_info, ws_info)
    _save_obj_hash(obj_info)
    _save_obj_version(obj_ver_key, obj_ver, obj_info, ws_info)
    _save_copy_edge(obj_ver_key, obj)
    _save_obj_ver_edge(obj_ver_key, obj_key)
    _save_ws_contains_edge(obj_key, obj_info)
    _save_workspace(ws_info)
    _save_type_vertices(obj_info)
    _save_created_with_method_edge(obj_ver_key, obj.get('provenance'))
    _save_created_with_module_edge(obj_ver_key, obj.get('provenance'))
    _save_inst_of_type_edge(obj_ver_key, obj_info)
    _save_owner_edge(obj_ver_key, obj_info)
    _save_referral_edge(obj_ver_key, obj)
    _save_prov_desc_edge(obj_ver_key, obj)
    type_, _ = obj_info[2].split('-')  # 2nd var is version
    if type_ in _TYPE_PROCESSOR_MAP:
        # this could use a lot of memory. There's a bunch of code in the workspace for
        # dealing with this situation, but that'd have to be ported to Python and it's pretty
        # complex, so YAGNI for now.
        ws_client = WorkspaceClient(url=config()['kbase_endpoint'], token=config()['ws_token'])
        resp = ws_client.admin_req('getObjects', {
            'objects': [{
                'ref': obj_ver_key.replace(':', '/'),
            }]
        })
        _TYPE_PROCESSOR_MAP[type_](obj_ver_key, resp['data'][0])


def _save_ws_object(obj_info, ws_info):
    """Runs at most every 300 seconds; otherwise a no-op."""
    wsid = obj_info[6]
    objid = obj_info[0]
    key = f"{wsid}:{objid}"
    logger.info(f'Saving ws_object with key {key}')
    save('ws_object', [{
        '_key': key,
        'workspace_id': wsid,
        'object_id': objid,
        'is_public': ws_info[6] == 'r',
        'deleted': False
    }])


def _save_obj_hash(info_tup):
    obj_hash = info_tup[8]
    obj_hash_type = 'MD5'
    logger.info(f'Saving ws_object_hash with key {obj_hash}')
    save('ws_object_hash', [{
        '_key': obj_hash,
        'type': obj_hash_type
    }])


def _save_obj_version(key, ver, info_tup, ws_info):
    objid = info_tup[0]
    wsid = info_tup[6]
    obj_name = info_tup[1]
    hsh = info_tup[8]
    size = info_tup[9]
    epoch = ts_to_epoch(info_tup[3])
    logger.info(f"Saving ws_object_version with key {key}")
    save('ws_object_version', [{
        '_key': key,
        'workspace_id': wsid,
        'object_id': objid,
        'version': ver,
        'name': obj_name,
        'hash': hsh,
        'size': size,
        'epoch': epoch,
        'deleted': False,
        'is_public': ws_info[6] == 'r',
    }])


def _save_copy_edge(obj_ver_key, obj):
    """Save ws_copied_from document."""
    copy_ref = obj.get('copied')
    if not copy_ref:
        logger.info('Not a copied object.')
        return
    copied_key = copy_ref.replace('/', ':')
    from_id = 'ws_object_version/' + obj_ver_key
    to_id = 'ws_object_version/' + copied_key
    logger.info(f'Saving ws_copied_from edge from {from_id} to {to_id}')
    # "The _from object is a copy of the _to object
    save('ws_copied_from', [{
        '_from': from_id,
        '_to': to_id
    }])


def _save_obj_ver_edge(obj_ver_key, obj_key):
    """Save the ws_version_of edge."""
    # The _from is a version of the _to
    from_id = 'ws_object_version/' + obj_ver_key
    to_id = 'ws_object/' + obj_key
    logger.info(f'Saving ws_version_of edge from {from_id} to {to_id}')
    save('ws_version_of', [{
        '_from': from_id,
        '_to': to_id
    }])


def _save_ws_contains_edge(obj_key, info_tup):
    """Save the ws_workspace_contains_obj edge."""
    from_id = 'ws_workspace/' + str(info_tup[6])
    to_id = 'ws_object/' + obj_key
    logger.info(f'Saving ws_workspace_contains_obj edge from {from_id} to {to_id}')
    save('ws_workspace_contains_obj', {'_from': from_id, '_to': to_id})


def _save_workspace(ws_info):
    """Save the ws_workspace vertex given an object info tuple."""
    wsid = ws_info[0]
    # Workspace info tuple is as follows:
    #    0  1    2     3       4        5          6          7        8
    #   [id,name,owner,moddate,maxobjid,user_perms,globalread,lockstat,metadata]
    metadata = ws_info[-1]
    logger.info(f'Saving workspace vertex {wsid}')
    save('ws_workspace', {
        '_key': str(wsid),
        'narr_name': metadata.get('narrative_nice_name', ''),
        'owner': ws_info[2],
        'max_obj_id': ws_info[4],
        'lock_status': ws_info[7],
        'name': ws_info[1],
        'mod_epoch': ts_to_epoch(ws_info[3]),
        'is_public': ws_info[6] == 'r',
        'is_deleted': False,
        'metadata': metadata
    })


def _get_ws_info(wsid):
    """Fetch the workspace info tuple."""
    workspace_client = WorkspaceClient(url=config()['kbase_endpoint'], token=config()['ws_token'])
    return workspace_client.admin_req('getWorkspaceInfo', {"id": wsid})


def _save_created_with_method_edge(obj_ver_key, prov):
    """Save the ws_obj_created_with_method edge."""
    if not prov or not prov[0] or not prov[0].get('service'):
        return
    method_key = get_method_key_from_prov(prov)
    from_id = 'ws_object_version/' + obj_ver_key
    to_id = 'ws_method_version/' + method_key
    params = prov[0].get('method_params')
    logger.info(f'Saving ws_obj_created_with_method edge from {from_id} to {to_id}')
    save('ws_obj_created_with_method', [{
        '_from': from_id,
        '_to': to_id,
        'method_params': params
    }])


def _save_created_with_module_edge(obj_ver_key, prov):
    """Save the ws_obj_created_with_module edge."""
    if not prov or not prov[0] or not prov[0].get('service'):
        return
    module_key = get_module_key_from_prov(prov)
    from_id = 'ws_object_version/' + obj_ver_key
    to_id = 'ws_module_version/' + module_key
    logger.info(f'Saving ws_obj_created_with_module edge from {from_id} to {to_id}')
    save('ws_obj_created_with_module', [{
        '_from': from_id,
        '_to': to_id
    }])


def _save_inst_of_type_edge(obj_ver_key, info_tup):
    """Save the ws_obj_instance_of_type of edge."""
    from_id = 'ws_object_version/' + obj_ver_key
    obj_type = info_tup[2]
    to_id = 'ws_type_version/' + obj_type
    logger.info(f'Saving ws_obj_instance_of_type edge from {from_id} to {to_id}')
    save('ws_obj_instance_of_type', [{
        '_from': from_id,
        '_to': to_id
    }])


def _save_type_vertices(obj_info):
    """Save associated vertices for an object type."""
    obj_type = obj_info[2]
    (type_module, type_name, type_ver) = get_type_pieces(obj_type)
    (maj_ver, min_ver) = [int(v) for v in type_ver.split('.')]
    logger.info(f'Saving ws_type_version, ws_type, and ws_type_module for {obj_type}')
    save('ws_type_version', {
        '_key': obj_type,
        'type_name': type_name,
        'module_name': type_module,
        'maj_ver': maj_ver,
        'min_ver': min_ver
    })
    save('ws_type', {'_key': f'{type_module}.{type_name}', 'type_name': type_name, 'module_name': type_module})
    save('ws_type_module', {'_key': type_module})


def _save_owner_edge(obj_ver_key, info_tup):
    """Save the ws_owner_of edge."""
    username = info_tup[5]
    from_id = 'ws_user/' + username
    to_id = 'ws_object_version/' + obj_ver_key
    logger.info(f'Saving ws_owner_of edge from {from_id} to {to_id}')
    save('ws_owner_of', [{
        '_from': from_id,
        '_to': to_id
    }])


def _save_referral_edge(obj_ver_key, obj):
    """Save the ws_refers_to edge."""
    from_id = 'ws_object_version/' + obj_ver_key
    for upa in obj.get('refs', []):
        to_id = 'ws_object_version/' + upa.replace('/', ':')
        logger.info(f'Saving ws_refers_to edge from {from_id} to {to_id}')
        save('ws_refers_to', [{
            '_from': from_id,
            '_to': to_id
        }])


def _save_prov_desc_edge(obj_ver_key, obj):
    """Save the ws_prov_descendant_of edge."""
    prov = obj.get('provenance')
    if not prov:
        return
    input_objs = prov[0].get('input_ws_objects', [])
    from_id = 'ws_object_version/' + obj_ver_key
    for upa in input_objs:
        to_id = 'ws_object_version/' + upa.replace('/', ':')
        logger.info(f'Saving ws_prov_descendant_of edge from {from_id} to {to_id}')
        save('ws_prov_descendant_of', [{
            '_from': from_id,
            '_to': to_id
        }])
