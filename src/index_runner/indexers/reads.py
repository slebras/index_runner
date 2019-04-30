from utils.get_path import get_path
from . import indexer_utils


def index_reads(obj_data, ws_info, obj_data_v1):
    '''
    Indexes both singleend reads and pairedend reads
    '''
    data = obj_data['data'][0]
    obj_info = data['info']

    upa = ":".join([str(data['info'][6]), str(data['info'][0]), str(data['info'][4])])

    ws_id = obj_info[6]
    reads_type = obj_info[2].split('-')[0]
    reads_type_version = obj_info[2].split('-')[1]
    shared_users = indexer_utils.get_shared_users(ws_id)

    # Get the rest of the data
    if data.get('provenance'):
        prov_services = []
        for prov in data_obj['provenance']:
            prov_service = prov['service']
            prov_services.append(prov_service)

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
        if data['data'].get('lib1'):
            if data['data']['lib1'].get('size'):
                size = data['data']['lib1']['size']
            else:
                size = None

        if data['data'].get('lib'):
            if data['data']['lib'].get('size'):
                size = data['data']['lib']['size']
            else:
                size = None

        if data['data'].get('single_genome'):
            single_genome = data['data']['single_genome']
        else:
            single_genome = None

    else:
        raise Exception("no data provided.")
    # gc_content, read_length_mean, qual_mean, phred_type

    metadata = obj_info[-1] or {}  # last elem of obj info is a metadata dict
    reads_name = metadata.get('name')
    is_public = ws_info[6] == 'r'
    return {
        'doc':{
            'sequencing_tech': sequencing_tech,
            'reads_type': reads_type,
            'reads_type_version': reads_type_version,
            'size': size,
            'interleaved': interleaved,
            'single_genome': single_genome,
            'provenance_services': prov_services,
            'name': reads_name,
            'public': is_public,
            'islast': True,
            'shared': False
        },
        'index': 'reads',
        'id': upa
    }


