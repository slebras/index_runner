# genome indexer
from .indexer_utils import mean

_NAMESPACE = "WS"
_GENOME_INDEX_VERSION = 1
_GENOME_FEATURE_INDEX_VERSION = 2
_GENOME_INDEX_NAME = 'genome:' + str(_GENOME_INDEX_VERSION)
_GENOME_FEATURE_INDEX_NAME = 'genome_features:' + str(_GENOME_FEATURE_INDEX_VERSION)


def index_genome(obj_data, ws_info, obj_data_v1):
    """
    Currently indexes following workspace types:
        ci:              KBaseGenomes.Genome-13.0+
        narrative(prod): KBaseGenomes.Genome-8.1+
    """
    info = obj_data['info']
    if not obj_data.get('data'):
        raise Exception("no data in object")
    data = obj_data['data']
    workspace_id = info[6]
    object_id = info[0]
    version = info[4]
    '''
    feature
        feat_type
        sequence_length
        functions
        functional_description
        genome_workspace_id (upa)
    '''
    assembly_ref = ":".join(data.get('assembly_ref', data.get('contigset_ref', "")).split('/'))

    publication_titles = [pub[2] for pub in data.get('publications', [])]
    publication_authors = [pub[5] for pub in data.get('publications', [])]
    genome_scientific_name = data.get('scientific_name', None)
    genome_id = f"{_NAMESPACE}::{workspace_id}:{object_id}"
    genome_index = {
        'doc': {
            'genome_id': data.get('id', None),
            'scientific_name': genome_scientific_name,
            'publication_titles': publication_titles,
            'publication_authors': publication_authors,
            'size': data.get('dna_size', None),
            'num_contigs': data.get('num_contigs', None),
            'genome_type': data.get('genome_type', None),
            'gc_content': data.get('gc_content', None),
            'taxonomy': data.get('taxonomy', None),
            'mean_contig_length': mean(data.get('contig_lengths', [])),
            'external_origination_date': data.get('external_source_origination_date', None),
            'original_source_file_name': data.get('original_source_file_name', None),

            # Things we may want to add that are used in current genome search.
            'cds_count': len(data.get('cdss', [])),
            'feature_count': len(data.get('features', [])),
            'mrna_count': len(data.get('mrnas', [])),
            'non_coding_feature_count': len(data.get('non_coding_features', [])),
            'assembly_ref': assembly_ref,
            'source_id': data.get('source_id', []),
            'feature_counts': data.get('feature_counts', None),
            'source': data.get('source', None),
            'warnings': data.get('warnings', None)
        },
        'index': _GENOME_INDEX_NAME,
        'id': genome_id
    }
    yield genome_index

    is_public = ws_info[6] == 'r'
    # gupa = f"{workspace_id}/{object_id}/{version}"
    # iterate through the features and yield for each feature
    for feat_type, field in [('gene', 'features'), ('non_coding_feature', 'non_coding_features'),
                             ('CDS', 'cdss'), ('mrna', 'mrnas')]:
        for feat in data.get(field, []):
            functions = feat.get('functions')
            if feat.get('location'):
                contig_ids, starts, strands, stops = zip(*feat.get('location'))
                contig_ids, starts, strands, stops = list(contig_ids), list(starts), list(strands), list(stops)
            else:
                contig_ids, starts, strands, stops = None, None, None, None
            # contig_ids = [l[0] for l in feat.get('location', [])]
            seq_len = feat.get('dna_sequence_length', None)
            feature_id = feat.get('id', "")
            feature_index = {
                'doc': {
                    'id': feature_id,
                    'feature_type': feat.get('type', None),
                    'functions': functions,
                    'contig_ids': contig_ids,
                    'sequence_length': seq_len,
                    'id': feature_id,
                    'genome_scientific_name': genome_scientific_name,
                    'obj_type_name': "GenomeFeature",  # hack to get ui for features to work.
                    # 'genome_upa': gupa,
                    'guid': f"{workspace_id}:{object_id}",
                    'access_group': workspace_id,
                    'is_public': is_public,
                    'parent_id': genome_id,
                    'genome_version': int(version),
                    'assembly_ref': assembly_ref,
                    'genome_feature_type': feat_type,
                    'starts': starts,
                    'strands': strands,
                    'stops': stops,
                    'aliases': feat.get('aliases', None),
                },
                'index': _GENOME_FEATURE_INDEX_NAME,
                'id': genome_id + f'::ft::{feature_id}',
                'no_defaults': True
            }
            yield feature_index
