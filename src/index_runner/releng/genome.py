"""
If a workspace object, such as a genome, has taxonomy info in it, then:
    - try to find the best-match taxon node in the RE ncbi taxonomy
    - create an edge from the ws_object_version to the ncbi taxon vertex
"""
import re
import time
from kbase_workspace_client import WorkspaceClient

from src.utils.re_client import stored_query
from src.utils.config import config
from src.utils.re_client import save

_OBJ_VER_COLL = "ws_object_version"
_TAX_VER_COLL = "ncbi_taxon"
_TAX_EDGE_COLL = "ws_obj_version_has_taxon"
_COMPAT_TYPES = ["KBaseGenomes.Genome"]

# KBaseGenomeAnnotations.Assembly could also be used. It has a taxon_ref but no
# other taxon-related fields.


def process_genome(obj_ver_key, obj_info_tup):
    """
    Create an edge between a workspace object with taxonomy info and an NCBI taxon on RE.
    obj_ver_key is the RE vertex key for the object (eg. "123:123:123")
    obj_info_tup is the workspace object info tuple from get_objects2
    """
    # obj_ver_key and obj_info_tup are kind of redundant
    # Check if the object is a compatible type
    obj_type = obj_info_tup[2].split("-")[0]
    if obj_type not in _COMPAT_TYPES:
        print('Object type not compatible for taxon edge.')
        print(f'Object type is {obj_type}')
        # No-op
        return
    ws_client = WorkspaceClient(url=config()['kbase_endpoint'], token=config()['ws_token'])
    resp = ws_client.admin_req('getObjects', {
        'objects': [{
            'ref': obj_ver_key.replace(':', '/'),
            'included': ["/taxonomy"]
        }]
    })
    data = resp['data'][0]['data']
    if 'taxonomy' not in data:
        print('No lineage in object; skipping..')
        return
    lineage = data['taxonomy'].split(';')
    # Get the species or strain name, and filter out any non-alphabet chars
    most_specific = re.sub(r'[^a-zA-Z ]', '', lineage[-1].strip())
    # Search by scientific name via the RE API
    adb_resp = stored_query('ncbi_taxon_search_sci_name', {
        'search_text': most_specific,
        'ts': int(time.time() * 1000),
        'offset': 0,
        'limit': 10,
    })
    # `adb_results` will be a dict with keys for 'total_count' and 'results'
    adb_results = adb_resp['results'][0]
    if adb_results['total_count'] == 0:
        print('No matching taxon found for object.')
        # No matching taxon found; no-op
        return
    match = adb_results['results'][0]
    # Create an edge from the ws_object_ver to the taxon
    tax_key = match['_key']
    from_id = f"{_OBJ_VER_COLL}/{obj_ver_key}"
    to_id = f"{_TAX_VER_COLL}/{tax_key}"
    print(f'Creating taxon edge from {from_id} to {to_id}')
    save(_TAX_EDGE_COLL, [{'_from': from_id, '_to': to_id, 'assigned_by': '_system'}])
