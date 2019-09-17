"""
Take object info from the workspace and import various vertices and edges into Arangodb.

ws_object (unversioned)
    workspace_id
    object_id
    deleted
    _key: wsid/objid
"""
from src.index_runner.releng import genome
from src.utils.re_client import save
from src.utils.config import config
from src.utils.formatting import ts_to_epoch, get_method_key_from_prov, get_module_key_from_prov
from kbase_workspace_client import WorkspaceClient

# need version specific processors here? Or expect the processor to handle all versions?
_TYPE_PROCESSOR_MAP = {
    'KBaseGenomes.Genome': genome.process_genome
}


def import_object(obj_info):
    """
    Given a workspace object downloaded to disk, convert it to a ws arangodb document and import it.
    `is_new_version` indicates whether this object is a brand new (latest as of runtime) version of an object.
    """
    # TODO handle the ws_latest_version_of edge -- some tricky considerations here
    # Save the ws_object document
    info_tup = obj_info['info']
    wsid = info_tup[6]
    objid = info_tup[0]
    obj_key = f'{wsid}:{objid}'
    _save_ws_object(obj_key, wsid, objid)
    # Save the ws_object_hash document
    _save_obj_hash(info_tup)
    # Save the ws_object_version document
    obj_ver = info_tup[4]
    obj_ver_key = f'{obj_key}:{obj_ver}'
    _save_obj_version(obj_ver_key, wsid, objid, obj_ver, info_tup)
    _save_copy_edge(obj_ver_key, obj_info)
    _save_obj_ver_edge(obj_ver_key, obj_key)
    _save_ws_contains_edge(obj_key, info_tup)
    prov = obj_info.get('provenance')
    if prov and prov[0] and prov[0].get('service'):
        _save_created_with_method_edge(obj_ver_key, prov)
        _save_created_with_module_edge(obj_ver_key, prov)
    _save_inst_of_type_edge(obj_ver_key, info_tup)
    _save_owner_edge(obj_ver_key, info_tup)
    _save_referral_edge(obj_ver_key, obj_info)
    _save_prov_desc_edge(obj_ver_key, obj_info)
    type_, _ = info_tup[2].split('-')  # 2nd var is version
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


def _save_ws_object(key, wsid, objid):
    print(f'Saving ws_object with key {key}')
    save('ws_object', [{
        '_key': key,
        'workspace_id': wsid,
        'object_id': objid,
        'deleted': False
    }])


def _save_obj_hash(info_tup):
    obj_hash = info_tup[8]
    obj_hash_type = 'MD5'
    print(f'Saving ws_object_hash with key {obj_hash}')
    save('ws_object_hash', [{
        '_key': obj_hash,
        'type': obj_hash_type
    }])


def _save_obj_version(key, wsid, objid, ver, info_tup):
    obj_name = info_tup[1]
    hsh = info_tup[8]
    size = info_tup[9]
    epoch = ts_to_epoch(info_tup[3])
    print(f"Saving ws_object_version with key {key}")
    save('ws_object_version', [{
        '_key': key,
        'workspace_id': wsid,
        'object_id': objid,
        'version': ver,
        'name': obj_name,
        'hash': hsh,
        'size': size,
        'epoch': epoch,
        'deleted': False
    }])


def _save_copy_edge(obj_ver_key, obj_info):
    """Save ws_copied_from document."""
    copy_ref = obj_info.get('copied')
    if not copy_ref:
        print('Not a copied object.')
        return
    copied_key = copy_ref.replace('/', ':')
    from_id = 'ws_object_version/' + obj_ver_key
    to_id = 'ws_object_version/' + copied_key
    print(f'Saving ws_copied_from edge from {from_id} to {to_id}')
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
    print(f'Saving ws_version_of edge from {from_id} to {to_id}')
    save('ws_version_of', [{
        '_from': from_id,
        '_to': to_id
    }])


def _save_ws_contains_edge(obj_key, info_tup):
    """Save the ws_workspace_contains_obj edge."""
    from_id = 'ws_workspace/' + str(info_tup[6])
    to_id = 'ws_object/' + obj_key
    print(f'Saving ws_workspace_contains_obj edge from {from_id} to {to_id}')
    save('ws_workspace_contains_obj', [{
        '_from': from_id,
        '_to': to_id
    }])


def _save_created_with_method_edge(obj_ver_key, prov):
    """Save the ws_obj_created_with_method edge."""
    method_key = get_method_key_from_prov(prov)
    from_id = 'ws_object_version/' + obj_ver_key
    to_id = 'ws_method_version/' + method_key
    params = prov[0].get('method_params')
    print(f'Saving ws_obj_created_with_method edge from {from_id} to {to_id}')
    save('ws_obj_created_with_method', [{
        '_from': from_id,
        '_to': to_id,
        'method_params': params
    }])


def _save_created_with_module_edge(obj_ver_key, prov):
    """Save the ws_obj_created_with_module edge."""
    module_key = get_module_key_from_prov(prov)
    from_id = 'ws_object_version/' + obj_ver_key
    to_id = 'ws_module_version/' + module_key
    print(f'Saving ws_obj_created_with_module edge from {from_id} to {to_id}')
    save('ws_obj_created_with_module', [{
        '_from': from_id,
        '_to': to_id
    }])


def _save_inst_of_type_edge(obj_ver_key, info_tup):
    """Save the ws_obj_instance_of_type of edge."""
    from_id = 'ws_object_version/' + obj_ver_key
    to_id = 'ws_type_version/' + info_tup[2]
    print(f'Saving ws_obj_instance_of_type edge from {from_id} to {to_id}')
    save('ws_obj_instance_of_type', [{
        '_from': from_id,
        '_to': to_id
    }])


def _save_owner_edge(obj_ver_key, info_tup):
    """Save the ws_owner_of edge."""
    username = info_tup[5]
    from_id = 'ws_user/' + username
    to_id = 'ws_object_version/' + obj_ver_key
    print(f'Saving ws_owner_of edge from {from_id} to {to_id}')
    save('ws_owner_of', [{
        '_from': from_id,
        '_to': to_id
    }])


def _save_referral_edge(obj_ver_key, obj_info):
    """Save the ws_refers_to edge."""
    from_id = 'ws_object_version/' + obj_ver_key
    for upa in obj_info.get('refs', []):
        to_id = 'ws_object_version/' + upa.replace('/', ':')
        print(f'Saving ws_refers_to edge from {from_id} to {to_id}')
        save('ws_refers_to', [{
            '_from': from_id,
            '_to': to_id
        }])


def _save_prov_desc_edge(obj_ver_key, obj_info):
    """Save the ws_prov_descendant_of edge."""
    prov = obj_info.get('provenance')
    if not prov:
        return
    input_objs = prov[0].get('input_ws_objects', [])
    from_id = 'ws_object_version/' + obj_ver_key
    for upa in input_objs:
        to_id = 'ws_object_version/' + upa.replace('/', ':')
        print(f'Saving ws_prov_descendant_of edge from {from_id} to {to_id}')
        save('ws_prov_descendant_of', [{
            '_from': from_id,
            '_to': to_id
        }])
