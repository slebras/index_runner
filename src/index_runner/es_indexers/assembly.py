from src.index_runner.es_indexers.indexer_utils import mean


def main(obj_data, ws_info, obj_data_v1, conf):
    """
    Currently Handles the follownig workspace types:
         KBaseGenomeAnnotations.Assembly-6.0
    """
    info = obj_data['info']
    data = obj_data['data']
    workspace_id = info[6]
    object_id = info[0]
    # get mean contig length
    if data.get('contigs'):
        # we do not include the contig if it does not store the requisite field
        mean_contig_length = mean([contig.get('length') for _, contig
                                   in data['contigs'].items() if contig.get('length')])
        percent_complete_contigs = mean([contig.get('is_complete') for _, contig
                                         in data['contigs'].items() if contig.get('is_complete')])
        percent_circle_contigs = mean([contig.get('is_circ') for _, contig
                                       in data['contigs'].items() if contig.get('is_circ')])
    else:
        mean_contig_length, percent_complete_contigs, percent_circle_contigs = None, None, None
    yield {
        '_action': 'index',
        'doc': {
            "assembly_name": data.get("name", None),
            "mean_contig_length": mean_contig_length,
            "percent_complete_contigs": percent_complete_contigs,
            "percent_circle_contigs": percent_circle_contigs,
            "assembly_id": data.get('assembly_id', None),
            "gc_content": data.get('gc_content', None),
            "size": data.get('dna_size', None),
            "num_contigs": data.get('num_contigs', None),
            "taxon_ref": data.get('taxon_ref', None),
            "external_origination_date": data.get('external_source_origination_date', None),
            "external_source_id": data.get('external_source_id', None),
            "external_source": data.get('external_source', None),
        },
        'index': conf['index_name'],
        'id': f"{conf['namespace']}::{workspace_id}:{object_id}",
    }
