# genome indexer
from src.index_runner.es_indexers.indexer_utils import mean
from src.utils.config import config


def main(obj_data, ws_info, obj_data_v1, conf):
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
    genome_id = f"{conf['namespace']}::{workspace_id}:{object_id}"
    genome_index = {
        '_action': 'index',
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
        'index': conf['index_name'],
        'id': genome_id
    }
    yield genome_index
    # gupa = f"{workspace_id}/{object_id}/{version}"
    # iterate through the features and yield for each feature
    if config()['skip_features']:
        # Indexing of genome features is turned off in the env
        return
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
                '_action': 'index',
                'doc': {
                    'id': feature_id,
                    'feature_type': feat.get('type', feat_type),
                    'functions': functions,
                    'contig_ids': contig_ids,
                    'sequence_length': seq_len,
                    'id': feature_id,
                    'obj_type_name': "GenomeFeature",  # hack to get ui for features to work.
                    'assembly_ref': assembly_ref,
                    'starts': starts,
                    'strands': strands,
                    'stops': stops,
                    'aliases': feat.get('aliases', None),
                    # Parent data from the Genome
                    'parent_id': genome_id,
                    'genome_version': int(version),
                    'genome_scientific_name': genome_scientific_name,
                    'genome_taxonomy': data.get('taxonomy'),
                    'genome_source': data.get('source'),
                    'genome_source_id': data.get('source_id'),
                    'genome_size': data.get('dna_size'),
                    'genome_num_contigs': data.get('num_contigs'),
                    'genome_feature_count': len(data.get('features', [])),
                    'genome_gc_content': data.get('gc_content')
                },
                'index': conf['features_index_name'],
                'id': genome_id + f'::ft::{feature_id}'
            }
            yield feature_index
