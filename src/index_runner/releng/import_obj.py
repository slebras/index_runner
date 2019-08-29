"""
Take object info from the workspace and import various vertices and edges into Arangodb.

wsfull_object (unversioned)
    workspace_id
    object_id
    deleted
    _key: wsid/objid
"""
from src.utils.re_client import save
from src.utils.formatting import ts_to_epoch, get_method_key_from_prov, get_module_key_from_prov


def import_object(obj_info):
    """
    Given a workspace object downloaded to disk, convert it to a wsfull arangodb document and import it.
    `is_new_version` indicates whether this object is a brand new (latest as of runtime) version of an object.
    """
    # TODO handle the wsfull_latest_version_of edge -- some tricky considerations here
    # Save the wsfull_object document
    info_tup = obj_info['info']
    wsid = info_tup[6]
    objid = info_tup[0]
    obj_key = f'{wsid}:{objid}'
    _save_wsfull_object(obj_key, wsid, objid)
    # Save the wsfull_object_hash document
    _save_obj_hash(info_tup)
    # Save the wsfull_object_version document
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


def _save_wsfull_object(key, wsid, objid):
    print(f'Saving wsfull_object with key {key}')
    save('wsfull_object', [{
        '_key': key,
        'workspace_id': wsid,
        'object_id': objid,
        'deleted': False
    }])


def _save_obj_hash(info_tup):
    obj_hash = info_tup[8]
    obj_hash_type = 'MD5'
    print(f'Saving wsfull_object_hash with key {obj_hash}')
    save('wsfull_object_hash', [{
        '_key': obj_hash,
        'type': obj_hash_type
    }])


def _save_obj_version(key, wsid, objid, ver, info_tup):
    obj_name = info_tup[1]
    hsh = info_tup[8]
    size = info_tup[9]
    epoch = ts_to_epoch(info_tup[3])
    print(f"Saving wsfull_object version with key {key}")
    save('wsfull_object_version', [{
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
    """Save wsfull_copied_from document."""
    copy_ref = obj_info.get('copied')
    if not copy_ref:
        print('Not a copied object.')
        return
    copied_key = copy_ref.replace('/', ':')
    from_id = 'wsfull_object_version/' + obj_ver_key
    to_id = 'wsfull_object_version/' + copied_key
    print(f'Saving wsfull_copied_from edge from {from_id} to {to_id}')
    # "The _from object is a copy of the _to object
    save('wsfull_copied_from', [{
        '_from': from_id,
        '_to': to_id
    }])


def _save_obj_ver_edge(obj_ver_key, obj_key):
    """Save the wsfull_version_of edge."""
    # The _from is a version of the _to
    from_id = 'wsfull_object_version/' + obj_ver_key
    to_id = 'wsfull_object/' + obj_key
    print(f'Saving wsfull_version_of edge from {from_id} to {to_id}')
    save('wsfull_version_of', [{
        '_from': from_id,
        '_to': to_id
    }])


def _save_ws_contains_edge(obj_key, info_tup):
    """Save the wsfull_ws_contains_obj edge."""
    from_id = 'wsfull_workspace/' + str(info_tup[6])
    to_id = 'wsfull_object/' + obj_key
    print(f'Saving wsfull_ws_contains_obj edge from {from_id} to {to_id}')
    save('wsfull_ws_contains_obj', [{
        '_from': from_id,
        '_to': to_id
    }])


def _save_created_with_method_edge(obj_ver_key, prov):
    """Save the wsfull_obj_created_with_method edge."""
    method_key = get_method_key_from_prov(prov)
    from_id = 'wsfull_object_version/' + obj_ver_key
    to_id = 'wsfull_method_version/' + method_key
    params = prov[0].get('method_params')
    print(f'Saving wsfull_obj_created_with_method edge from {from_id} to {to_id}')
    save('wsfull_obj_created_with_method', [{
        '_from': from_id,
        '_to': to_id,
        'method_params': params
    }])


def _save_created_with_module_edge(obj_ver_key, prov):
    """Save the wsfull_obj_created_with_module edge."""
    module_key = get_module_key_from_prov(prov)
    from_id = 'wsfull_object_version/' + obj_ver_key
    to_id = 'wsfull_module_version/' + module_key
    print(f'Saving wsfull_obj_created_with_module edge from {from_id} to {to_id}')
    save('wsfull_obj_created_with_module', [{
        '_from': from_id,
        '_to': to_id
    }])


def _save_inst_of_type_edge(obj_ver_key, info_tup):
    """Save the wsfull_obj_instance_of_type of edge."""
    from_id = 'wsfull_object_version/' + obj_ver_key
    to_id = 'wsfull_type_version/' + info_tup[2]
    print(f'Saving wsfull_obj_instance_of_type edge from {from_id} to {to_id}')
    save('wsfull_obj_instance_of_type', [{
        '_from': from_id,
        '_to': to_id
    }])


def _save_owner_edge(obj_ver_key, info_tup):
    """Save the wsfull_owner_of edge."""
    username = info_tup[5]
    from_id = 'wsfull_user/' + username
    to_id = 'wsfull_object_version/' + obj_ver_key
    print(f'Saving wsfull_owner_of edge from {from_id} to {to_id}')
    save('wsfull_owner_of', [{
        '_from': from_id,
        '_to': to_id
    }])


def _save_referral_edge(obj_ver_key, obj_info):
    """Save the wsfull_refers_to edge."""
    from_id = 'wsfull_object_version/' + obj_ver_key
    for upa in obj_info.get('refs', []):
        to_id = 'wsfull_object_version/' + upa.replace('/', ':')
        print(f'Saving wsfull_refers_to edge from {from_id} to {to_id}')
        save('wsfull_refers_to', [{
            '_from': from_id,
            '_to': to_id
        }])


def _save_prov_desc_edge(obj_ver_key, obj_info):
    """Save the wsfull_prov_descendant_of edge."""
    prov = obj_info.get('provenance')
    if not prov:
        return
    input_objs = prov[0].get('input_ws_objects', [])
    from_id = 'wsfull_object_version/' + obj_ver_key
    for upa in input_objs:
        to_id = 'wsfull_object_version/' + upa.replace('/', ':')
        print(f'Saving wsfull_prov_descendant_of edge from {from_id} to {to_id}')
        save('wsfull_prov_descendant_of', [{
            '_from': from_id,
            '_to': to_id
        }])
