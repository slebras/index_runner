# genome indexer
from .indexer_utils import mean


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
    # iterate through the features and yield for each feature
    publication_titles = [pub[2] for pub in data.get('publications', [])]
    publication_authors = [pub[5] for pub in data.get('publications', [])]

    genome_index = {
        'doc': {
            'genome_id': data.get('id', None),
            'scientific_name': data.get('scientific_name', None),
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
        },
        'index': "genome",
        'id': f"{workspace_id}:{object_id}"
    }
    yield genome_index

    gupa = f"{workspace_id}/{object_id}/{version}"

    for feat_type, field in [('gene', 'features'), ('non_coding_feature', 'non_coding_features'),
                             ('CDS', 'cdss'), ('mrna', 'mrnas')]:
        for feat in data.get(field, []):
            functions = feat.get('functions')
            contig_ids = [l[0] for l in feat.get('locations', [])]
            seq_len = feat.get('dna_sequence_length', None)

            feature_id = feat.get('id', "")

            feature_index = {
                'doc': {
                    'functions': functions,
                    'contig_ids': contig_ids,
                    'sequence_length': seq_len,
                    'id': feature_id,
                    'genome_upa': gupa,
                },
                'index': 'genome_features',
                'id': f'{workspace_id}:{object_id}:{feature_id}',
            }
            yield feature_index
