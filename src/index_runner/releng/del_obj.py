"""
Take object info from the workspace and delete all associated vertices and edges into Arangodb.
"""
import logging

from src.index_runner.releng import genome
from src.utils.ws_utils import get_type_pieces
from src.utils.re_client import delete_docs
from src.utils.formatting import sanitize_arangodb_key

logger = logging.getLogger('IR')

# Special deletion handlers for KBase types
_TYPE_PROCESSOR_MAP = {
    'KBaseGenomes.Genome': genome.delete_genome
}


def delete_object(obj_info):
    """
    Delete all the associated edges and vertices for a workspace object into RE.
    """
    # Delete the ws_object document
    wsid = obj_info[6]
    objid = obj_info[0]
    objver = obj_info[4]
    obj_key = f'{wsid}:{objid}'
    obj_ver_key = f'{obj_key}:{objver}'
    _del_ws_object(obj_key)
    _del_obj_hash(obj_info)
    _del_obj_version(obj_ver_key)
    _del_copy_edge(obj_ver_key)
    _del_obj_ver_edge(obj_key)
    _del_ws_contains_edge(obj_key)
    _del_workspace(wsid)
    _del_type_vertices(obj_info)
    _del_created_with_method_edge(obj_ver_key)
    _del_created_with_module_edge(obj_ver_key)
    _del_inst_of_type_edge(obj_ver_key)
    _del_owner_edge(obj_ver_key)
    _del_referral_edge(obj_ver_key)
    _del_prov_desc_edge(obj_ver_key)
    (type_module, type_name, type_ver) = get_type_pieces(obj_info[2])
    _type = f"{type_module}.{type_name}"
    if _type in _TYPE_PROCESSOR_MAP:
        _TYPE_PROCESSOR_MAP[_type](obj_info)


def _del_ws_object(obj_key):
    """Runs at most every 300 seconds; otherwise a no-op."""
    logger.debug(f'Deleting ws_object with key {obj_key}')
    delete_docs('ws_object', {'_key': obj_key})


def _del_obj_hash(obj_info):
    obj_hash = obj_info[8]
    logger.debug(f'Deleting ws_object_hash with key {obj_hash}')
    delete_docs('ws_object_hash', {'_key': obj_hash})


def _del_obj_version(obj_ver_key):
    logger.debug(f"Deleting ws_object_version with key {obj_ver_key}")
    delete_docs('ws_object_version', {'_key': obj_ver_key})


def _del_copy_edge(obj_ver_key):
    """Save ws_copied_from document."""
    from_id = 'ws_object_version/' + obj_ver_key
    logger.debug(f'Deleting ws_copied_from edge with from_id: {from_id}')
    # "The _from object is a copy of the _to object
    delete_docs('ws_copied_from', {'_from': from_id})


def _del_obj_ver_edge(obj_key):
    # The _from is a version of the _to
    to_id = 'ws_object/' + obj_key
    logger.debug(f'Deleting ws_version_of edges that have to_id: {to_id}')
    delete_docs('ws_version_of', {'_to': to_id})


def _del_ws_contains_edge(obj_key):
    to_id = 'ws_object/' + obj_key
    logger.debug(f'Deleting all ws_workspace_contains_obj edges with to_id:  {to_id}')
    delete_docs('ws_workspace_contains_obj', {'_to': to_id})


def _del_workspace(wsid):
    logger.debug(f'Deleting workspace vertex {wsid}')
    delete_docs('ws_workspace', {'_key': str(wsid)})


def _del_created_with_method_edge(obj_ver_key):
    from_id = 'ws_object_version/' + obj_ver_key
    logger.debug(f'Deleting all ws_obj_created_with_method edges with from_id: {from_id}')
    delete_docs('ws_obj_created_with_method', {'_from': from_id})


def _del_created_with_module_edge(obj_ver_key):
    from_id = 'ws_object_version/' + obj_ver_key
    logger.debug(f'Deleting all ws_obj_created_with_module edges with from_id: {from_id}')
    delete_docs('ws_obj_created_with_module', {'_from': from_id})


def _del_inst_of_type_edge(obj_ver_key):
    from_id = 'ws_object_version/' + obj_ver_key
    logger.debug(f'Deleting all ws_obj_instance_of_type edges with from_id: {from_id}')
    delete_docs('ws_obj_instance_of_type', {'_from': from_id})


def _del_type_vertices(obj_info):
    obj_type = sanitize_arangodb_key(obj_info[2])
    (type_module, type_name, type_ver) = get_type_pieces(obj_type)
    (maj_ver, min_ver) = [int(v) for v in type_ver.split('.')]
    logger.debug(f'Deleting ws_type_version, ws_type, and ws_type_module for {obj_type}')
    delete_docs('ws_type_version', {'_key': obj_type})
    delete_docs('ws_type', {'_key': f'{type_module}.{type_name}'})
    delete_docs('ws_type_module', {'_key': type_module})


def _del_owner_edge(obj_ver_key):
    to_id = 'ws_object_version/' + obj_ver_key
    logger.debug(f'Deleting ws_owner_of edge with to_id: {to_id}')
    delete_docs('ws_owner_of', {'_to': to_id})


def _del_referral_edge(obj_ver_key):
    from_id = 'ws_object_version/' + obj_ver_key
    logger.debug(f'Deleting all ws_refers_to edges with from_id: {from_id}')
    delete_docs('ws_refers_to', {'_from': from_id})


def _del_prov_desc_edge(obj_ver_key):
    from_id = 'ws_object_version/' + obj_ver_key
    logger.debug(f'Deleting all ws_prov_descendant_of edges with from_id: {from_id}')
    delete_docs('ws_prov_descendant_of', {'_from': from_id})
