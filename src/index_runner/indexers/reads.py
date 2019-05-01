# from . import indexer_utils


def index_reads(obj_data, ws_info, obj_data_v1):
    '''
    Indexes both singleend reads and pairedend reads
    '''
    data = obj_data['data'][0]
    obj_info = data['info']
    workspace_id = obj_info[6]
    object_id = obj_info[0]

    reads_type = obj_info[2].split('-')[0]
    reads_type_version = float(obj_info[2].split('-')[1])
    # not sure we want this for reads (but maybe?)
    # shared_users = indexer_utils.get_shared_users(ws_id)

    if data.get('data'):
        if data['data'].get('interleaved'):
            interleaved = bool(data['data']['interleaved'])
        else:
            interleaved = False

        if data['data'].get('sequencing_tech'):
            sequencing_tech = data['data']['sequencing_tech']
        else:
            sequencing_tech = ""

        size = None
        # if lib1 exists, we know that its they're paired-end reads
        if data['data'].get('lib1'):
            if data['data']['lib1'].get('size'):
                size = data['data']['lib1']['size']
            if data['data'].get('lib2'):
                if data['data']['lib2'].get('size'):
                    if size is None:
                        size += data['data']['lib2']['size']
                    else:
                        size = data['data']['lib2']['size']

        if data['data'].get('lib'):
            if data['data']['lib'].get('size'):
                size = data['data']['lib']['size']

        if data['data'].get('single_genome'):
            single_genome = data['data']['single_genome']
        else:
            single_genome = None

        if data['data'].get('gc_content'):
            gc_content = data['data']['gc_content']
        else:
            gc_content = None

        # the average (mean) read length size
        if data['data'].get('read_length_mean'):
            mean_read_length = data['data']['read_length_mean']
        else:
            mean_read_length = None

        # mean quality scores
        if data['data'].get('qual_mean'):
            qual_mean = data['data']['qual_mean']
        else:
            qual_mean = None

        # the scale of phred scores
        if data['data'].get('phred_type'):
            phred_type = data['data']['phred_type']
        else:
            phred_type = None

    else:
        raise Exception("no data provided.")

    reads_name = obj_info[1]
    return {
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
            'name': reads_name
        },
        'index': 'reads',
        'id': f'{workspace_id}:{object_id}'
    }
