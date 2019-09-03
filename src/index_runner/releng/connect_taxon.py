"""
If a workspace object, such as a genome, has taxonomy info in it, then:
    - try to find the best-match taxon node in the RE ncbi taxonomy
    - create an edge from the ws_object_version to the ncbi taxon vertex
"""
from kbase_workspace_client import WorkspaceClient

from src.utils.re_client import stored_query
from src.utils.config import get_config
from src.utils.re_client import save

_CONFIG = get_config()
_OBJ_VER_COLL = "ws_object_version"
_TAX_VER_COLL = "ncbi_taxon"
_TAX_EDGE_COLL = "ws_object_version_has_taxon"
_COMPAT_TYPES = ["KBaseGenomes.Genome"]

# KBaseGenomeAnnotations.Assembly could also be used. It has a taxon_ref but no
# other taxon-related fields.


def create_taxon_edge(obj_ver_key, obj_info_tup):
    """
    Create an edge between a workspace object with taxonomy info and an NCBI taxon on RE.
    obj_ver_key is the RE vertex key for the object (eg. "123:123:123")
    obj_info_tup is the workspace object info tuple from get_objects2
    """
    # Check if the object is a compatible type
    obj_type = obj_info_tup[2]
    if obj_type not in _COMPAT_TYPES:
        print('Object type not compatible for taxon edge.')
        # No-op
        return
    # TODO Get the scientific name of the object
    ws_client = WorkspaceClient(url=_CONFIG['workspace_url'], token=_CONFIG['ws_token'])
    resp = ws_client.admin_req('getObjects', {
        'objects': [{
            'ref': obj_ver_key.replace(':', '/'),
            'included': ["/taxonomy"]
        }]
    })
    lineage = '; '.split(resp['data']['taxonomy'])
    most_specific = lineage[-1]
    # Search on RE for the taxon ID vertex
    results = stored_query('ncbi_taxon_search_sci_name', {
        'search_text': most_specific,
        'offset': 0,
        'limit': 1,
    })
    if results['count'] == 0:
        print('No matching taxon found for object.')
        # No matching taxon found; no-op
        return
    match = results['result'][0]
    # Create an edge from the ws_object_ver to the taxon
    tax_id = match['_key']
    from_id = f"{_OBJ_VER_COLL}/{obj_ver_key}"
    to_id = f"{_TAX_VER_COLL}/{tax_id}"
    print(f'Creating edge from {from_id} to {to_id}')
    save(_TAX_EDGE_COLL, [{'_from': from_id, '_to': to_id}])
