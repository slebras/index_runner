# generator that returns pangenome indexes


def main(obj_data, ws_info, obj_data_v1, conf):
    """
    """
    info = obj_data['info']
    data = obj_data['data']
    workspace_id = info[6]
    object_id = info[0]
    parent_id = f"{conf['namespace']}::{workspace_id}:{object_id}"
    yield {
        '_action': 'index',
        'doc': {
            'pangenome_id': data.get('id'),
            'pangenome_name': data.get('name'),
            'pangenome_type': data.get('type'),
            'genome_upas': data.get('genome_refs', []),
        },
        'index': conf['pangenome_index_name'],
        'id': parent_id
    }
    for ortholog_family in data.get('orthologs', []):
        ortholog_id = ortholog_family.get('id', '')
        gene_ids = [ortho[0] for ortho in ortholog_family.get('orthologs', [])]
        yield {
            '_action': 'index',
            'doc': {
                'ortholog_id': ortholog_id,
                'ortholog_type': ortholog_family.get('type', None),
                'function': ortholog_family.get('function', None),
                'gene_ids': gene_ids,
            },
            'index': conf['pangenome_ortholog_family_index_name'],
            'id': parent_id + f'::orth::{ortholog_id}',
            '_no_defaults': True
        }
