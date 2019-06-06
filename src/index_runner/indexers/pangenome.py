# generator that returns pangenome indexes

_NAMESPACE = "WS"
_PANGENOME_INDEX_VERSION = 1
_PANGENOME_ORTHOLOG_FAMILY_INDEX_VERSION = 1
_PANGENOME_INDEX_NAME = 'pangenome:' + str(_PANGENOME_INDEX_VERSION)
_PANGENOME_ORTHOLOG_FAMILY_INDEX_NAME = (
    'pangenome_orthologfamily:' + str(_PANGENOME_ORTHOLOG_FAMILY_INDEX_VERSION)
)


def index_pangenome(obj_data, ws_info, obj_data_v1):
    """
    """
    info = obj_data['info']
    data = obj_data['data']
    workspace_id = info[6]
    object_id = info[0]
    parent_id = f"{_NAMESPACE}::{workspace_id}:{object_id}"
    yield {
        'doc': {
            'pangenome_id': data.get('id'),
            'pangenome_name': data.get('name'),
            'pangenome_type': data.get('type'),
            'genome_upas': data.get('genome_refs', []),
        },
        'index': _PANGENOME_INDEX_NAME,
        'id': parent_id
    }
    for ortholog_family in data.get('orthologs', []):
        ortholog_id = ortholog_family.get('id', '')
        gene_ids = [ortho[0] for ortho in ortholog_family.get('orthologs', [])]
        yield {
            'doc': {
                'ortholog_id': ortholog_id,
                'ortholog_type': ortholog_family.get('type', None),
                'function': ortholog_family.get('function', None),
                'gene_ids': gene_ids,
            },
            'index': _PANGENOME_ORTHOLOG_FAMILY_INDEX_NAME,
            'id': parent_id + f'::orth::{ortholog_id}',
            'no_defaults': True
        }
