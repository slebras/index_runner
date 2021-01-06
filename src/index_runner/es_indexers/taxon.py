

def main(obj_data, ws_info, obj_data_v1, conf):
    """
    Currently indexes following workspace types:
        KBaseGenomeAnnotations.Taxon-1.0
    """
    info = obj_data['info']
    data = obj_data['data']

    workspace_id = info[6]
    object_id = info[0]

    yield {
        '_action': 'index',
        'doc': {
            'scientific_name': data.get('scientific_name'),
            'scientific_lineage': data.get('scientific_lineage'),
            'domain': data.get('domain'),
            'kingdom': data.get('kingdom'),
            'parent_taxon_ref': data.get('parent_taxon_ref', None),
            'genetic_code': data.get('genetic_code', None),
            'aliases': data.get('aliases', [])
        },
        'index': conf['index_name'],
        'id': f"{conf['namespace']}::{workspace_id}:{object_id}"
    }
