from .indexer_utils import default_fields
from utils.get_path import get_path


def index_reads(obj_data, ws_info, obj_data_v1):
    '''
    Indexes both singleend reads and pairedend reads
    '''
    obj_info = obj_data['info']
    data = obj_data['data']
    workspace_id = obj_info[6]
    object_id = obj_info[0]
    reads_type = obj_info[2].split('-')[0]
    reads_type_version = str(obj_info[2].split('-')[1])

    interleaved = bool(data.get('interleaved', False))
    sequencing_tech = data.get('sequencing_tech', "")

    # if lib1 exists, we know that its they're paired-end reads
    if data.get('lib1'):
        size = (get_path(obj_data, ['data', 'lib1', 'size'], 0) +
                get_path(obj_data, ['data', 'lib2', 'size'], 0))
    elif data.get('lib'):
        size = get_path(obj_data, ['data', 'lib', 'size'], 0)
    else:
        size = None

    single_genome = bool(data.get('single_genome', False))
    gc_content = data.get('gc_content', None)
    # the average (mean) read length size
    mean_read_length = data.get('read_length_mean', None)
    # mean quality scores
    qual_mean = data.get('qual_mean', None)
    # the scale of phred scores
    phred_type = data.get('phred_type', None)

    yield {
        'doc': {
            'phred_type': phred_type,
            'gc_content': gc_content,
            'mean_quality_score': qual_mean,
            'mean_read_length': mean_read_length,
            'sequencing_tech': sequencing_tech,
            'reads_type': reads_type,
            'reads_type_version': reads_type_version,
            'size': size,
            'interleaved': interleaved,
            'single_genome': single_genome,
            **default_fields(obj_data, ws_info, obj_data_v1)
        },
        'index': 'reads',
        'id': f'{workspace_id}:{object_id}'
    }
