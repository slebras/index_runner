"""
If a workspace object, such as a genome, has taxonomy info in it, then:
    - try to find the best-match taxon node in the RE ncbi taxonomy
    - create an edge from the ws_object_version to the ncbi taxon vertex
"""
import time
from collections import defaultdict as _defaultdict
import datetime as _datetime
import itertools as _itertools
import logging
from kbase_workspace_client import WorkspaceClient

from src.utils.config import config
from src.utils.re_client import stored_query as _stored_query
from src.utils.re_client import save as _save
from src.utils.re_client import delete_docs, execute_query
# may want to html encode vs replace with _ to avoid collisions? Seems really improbable
from src.utils.re_client import clean_key as _clean_key
from src.utils.re_client import MAX_ADB_INTEGER as _MAX_ADB_INTEGER

logger = logging.getLogger('IR')

_OBJ_VER_COLL = "ws_object_version"
_TAX_VER_COLL = "ncbi_taxon"
_TAX_EDGE_COLL = "ws_obj_version_has_taxon"
_GO_TERM_COLL = 'GO_terms'
_WS_FEAT_COLL = 'ws_genome_features'
_WS_FEAT_EDGE_COLL = 'ws_genome_has_feature'
_WS_FEAT_TO_GO_COLL = 'ws_feature_has_GO_annotation'

_ONTOLOGY_GO_KEY = 'GO'  # per Jason Baumohl
_ONTOLOGY_TERMS = 'ontology_terms'

_MAX_RE_QUERY_SIZE = 10000  # make this a constant in some central file?

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
    if not config()['skip_features']:
        _generate_features(obj_ver_key, obj_data)
        _generate_GO_links(obj_ver_key, obj_data)


def delete_genome(obj_ver_key, obj_info):
    _del_taxon_edge(obj_info)
    wsid = obj_info[6]
    objid = obj_info[0]
    objver = obj_info[4]
    obj_key = f'{wsid}:{objid}'
    obj_ver_key = f'{obj_key}:{objver}'
    _del_features(obj_ver_key, wsid, objid, objver)
    _del_GO_links(obj_ver_key)


def _del_taxon_edge(obj_ver_key):
    from_id = f"{_OBJ_VER_COLL}/{obj_ver_key}"
    logger.debug(f"Deleting all {_TAX_EDGE_COLL} edges with from_id: {from_id}")
    delete_docs(_TAX_EDGE_COLL, {'_from': from_id})


def _del_features(obj_ver_key, wsid, objid, objver):
    pass
    from_id = f'{_OBJ_VER_COLL}/{obj_ver_key}'
    logger.debug(f"Deleting all feature vertices and edges for {obj_ver_key}")
    delete_docs(_WS_FEAT_COLL, {'workspace_id': wsid, 'object_id': objid, 'version': objver})
    delete_docs(_WS_FEAT_EDGE_COLL, {'_from': from_id})


def _del_GO_links(obj_ver_key):
    query = f"""
    FOR edge IN {_WS_FEAT_TO_GO_COLL}
        FILTER LIKE(edge._from, "{_WS_FEAT_COLL}/{obj_ver_key}%")
        REMOVE edge IN {_WS_FEAT_TO_GO_COLL}
    """
    execute_query(query)


def _generate_taxon_edge(obj_ver_key, obj_data):
    if 'taxon_ref' not in obj_data['data']:
        logger.info('No taxon ref in object; skipping..')
        return
    ws_client = WorkspaceClient(url=config()['kbase_endpoint'], token=config()['ws_token'])
    result = ws_client.admin_req('getObjects', {
        'objects': [{'ref': obj_data['data']['taxon_ref']}]
    })
    taxonomy_id = result['data'][0]['data']['taxonomy_id']
    adb_resp = _stored_query('ncbi_fetch_taxon', {
       'id': str(taxonomy_id),
       'ts': int(time.time() * 1000),
    })
    adb_results = adb_resp['results']
    if not adb_results:
        logger.info(f'No taxonomy node in database for id {taxonomy_id}')
        return
    tax_key = adb_results[0]['_key']
    # Create an edge from the ws_object_ver to the taxon
    from_id = f"{_OBJ_VER_COLL}/{obj_ver_key}"
    to_id = f"{_TAX_VER_COLL}/{tax_key}"
    logger.info(f'Creating taxon edge from {from_id} to {to_id}')
    _save(_TAX_EDGE_COLL, [{'_from': from_id, '_to': to_id, 'assigned_by': '_system'}])


def _generate_features(obj_ver_key, obj_data):
    d = obj_data['data']
    if not d.get('features'):
        logger.info(f'Genome {obj_ver_key} has no features')
        return
    verts = []
    edges = []
    wsid = obj_data['info'][6]
    objid = obj_data['info'][0]
    ver = obj_data['info'][4]
    # might want to do this in smaller batches if memory pressure is an issue
    for f in d['features']:
        feature_key = _clean_key(f'{obj_ver_key}_{f["id"]}')
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
    logger.info(f'Saving {len(verts)} features for genome {obj_ver_key}')
    # hmm, this could leave the db in a corrupt state... options are 1) rollback 2) retry 3) leave
    # rollback is kind of impossible as an error here implies the re api isn't reachable
    # retry is doable, but should probably be implemented much higher in the stack
    # So 3 for now
    # reindexing will overwrite and fix
    _save(_WS_FEAT_COLL, verts)
    _save(_WS_FEAT_EDGE_COLL, edges)


def _generate_GO_links(obj_ver_key, obj_data):
    d = obj_data['data']
    if not d.get('features'):
        # no features logged already in _generate_features
        return
    f_to_go = {}
    for f in d['features']:
        # this works for Genome-8.2 to 10.0 in production
        if _ONTOLOGY_TERMS in f and _ONTOLOGY_GO_KEY in f[_ONTOLOGY_TERMS]:
            f_to_go[f['id']] = f[_ONTOLOGY_TERMS][_ONTOLOGY_GO_KEY].keys()
    terms_set = {i for items in f_to_go.values() for i in items}  # flatten
    query_time = _now_epoch_ms()
    # might want to do this in smaller batches if memory pressure is an issue
    resolved_terms = _resolve_GO_terms(terms_set, query_time)
    edges = []
    for f in f_to_go:
        for g in f_to_go[f]:
            if g not in resolved_terms:
                logger.info(f"Couldn't resolve GO term {g} in Genome {obj_ver_key} feature {f}")
            else:
                featurekey = _clean_key(f'{obj_ver_key}_{f}')
                edges.append({
                    '_key': f'{featurekey}::{resolved_terms[g]}::kbase_RE_indexer',
                    '_from': f'{_WS_FEAT_COLL}/{featurekey}',
                    '_to': f'{_GO_TERM_COLL}/{resolved_terms[g]}',
                    'kbase_id': obj_ver_key,
                    'source': 'kbase_RE_indexer',
                    'expired': _MAX_ADB_INTEGER
                })
    created_time = _now_epoch_ms() + 20 * len(edges)  # allow 20 ms to transport & save each edge
    for e in edges:
        e['created'] = created_time
    logger.info(f'Writing {len(edges)} feature -> GO edges for genome {obj_ver_key}')
    _save(_WS_FEAT_TO_GO_COLL, edges, on_duplicate='ignore')


# terms that can't be resolved are missing from results
def _resolve_GO_terms(terms_set, query_time):
    if not terms_set:
        return {}
    terms_set_copy = set(terms_set)
    terms_set = None  # prevent accidental side effects
    resolved = {}
    # make a list to avoid modification while iterating
    for chunk in _chunkiter(list(terms_set_copy), _MAX_RE_QUERY_SIZE):
        c = list(chunk)  # iterator to list
        res = _stored_query('GO_get_terms', {'ids': c, 'ts': query_time})
        t_to_key = {t['id']: t['_key'] for t in res['results']}
        for t in c:
            if t in t_to_key:
                resolved[t] = t_to_key[t]
                terms_set_copy.remove(t)
    replaced_by = {}
    for chunk in _chunkiter(terms_set_copy, _MAX_RE_QUERY_SIZE):
        c = list(chunk)
        res = _stored_query('GO_get_merges_from', {'froms': c})
        from_to_time = _defaultdict(list)  # type: dict
        for e in res['results']:
            from_to_time[e['from']].append((e['to'], e['created']))
        for f in from_to_time.keys():
            to = sorted(from_to_time[f], key=lambda tt: tt[1])[-1]  # get most recent edge
            replaced_by[f] = to[0]
    terms_set_copy = None  # type: ignore
    res = _resolve_GO_terms(set(replaced_by.values()), query_time)
    for old, new in replaced_by.items():
        if new in res:
            resolved[old] = res[new]
    return resolved


def _now_epoch_ms():
    return int(_datetime.datetime.now(tz=_datetime.timezone.utc).timestamp()) * 1000


def _chunkiter(iterable, size):
    """
    Iterate over chunks of size 'size' of an iterable.
    """
    iterator = iter(iterable)
    for first in iterator:
        yield _itertools.chain([first], _itertools.islice(iterator, size - 1))
