"""
If a workspace object, such as a genome, has taxonomy info in it, then:
    - try to find the best-match taxon node in the RE ncbi taxonomy
    - create an edge from the ws_object_version to the ncbi taxon vertex
"""
import re as _re
import time as _time

from src.utils.re_client import stored_query as _stored_query
from src.utils.re_client import save as _save

_OBJ_VER_COLL = "ws_object_version"
_TAX_VER_COLL = "ncbi_taxon"
_TAX_EDGE_COLL = "ws_obj_version_has_taxon"
_WS_FEAT_COLL = 'ws_genome_features'
_WS_FEAT_EDGE_COLL = 'ws_genome_has_feature'


# KBaseGenomeAnnotations.Assembly could also be used. It has a taxon_ref but no
# other taxon-related fields.


def process_genome(obj_ver_key, obj_data):
    """
    Create an edge between a workspace object with taxonomy info and an NCBI taxon on RE.
    obj_ver_key is the RE vertex key for the object (eg. "123:123:123")
    obj_info_tup is the workspace object info tuple from get_objects2
    """
    # obj_ver_key and obj_info_tup are kind of redundant
    # Check if the object is a compatible type
    _generate_taxon_edge(obj_ver_key, obj_data)
    _generate_features(obj_ver_key, obj_data)


def _generate_taxon_edge(obj_ver_key, obj_data):
    if 'taxonomy' not in obj_data['data']:
        print('No lineage in object; skipping..')
        return
    lineage = obj_data['data']['taxonomy'].split(';')
    # Get the species or strain name, and filter out any non-alphabet chars
    most_specific = _re.sub(r'[^a-zA-Z ]', '', lineage[-1].strip())
    # Search by scientific name via the RE API
    adb_resp = _stored_query('ncbi_taxon_search_sci_name', {
        'search_text': most_specific,
        'ts': int(_time.time() * 1000),
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
    _save(_TAX_EDGE_COLL, [{'_from': from_id, '_to': to_id, 'assigned_by': '_system'}])


def _generate_features(obj_ver_key, obj_data):
    d = obj_data['data']
    if not d.get('features'):
        return

    verts = []
    edges = []
    wsid = obj_data['info'][6]
    objid = obj_data['info'][0]
    ver = obj_data['info'][4]
    # might want to do this in smaller batches if memory pressure is an issue
    for f in d['features']:
        feature_key = f'{obj_ver_key}_{f["id"]}'  # check f['id'] for weird chars?
        verts.append({
            '_key': feature_key,
            'workspace_id': wsid,
            'object_id': objid,
            'version': ver,
            'feature_id': f['id']
        })
        edges.append({
            '_key': f'{feature_key}',  # make a unique key so overwrites work
            '_from': f'{_OBJ_VER_COLL}/{obj_ver_key}',
            '_to': f'{_WS_FEAT_COLL}/{feature_key}'
        })

    # hmm, this could leave the db in a corrupt state... options are 1) rollback 2) retry 3) leave
    # rollback is kind of impossible as an error here implies the re api isn't reachable
    # retry is doable, but should probably be implemented much higher in the stack
    # So 3 for now
    # reindexing will overwrite and fix
    _save(_WS_FEAT_COLL, verts)
    _save(_WS_FEAT_EDGE_COLL, edges)
