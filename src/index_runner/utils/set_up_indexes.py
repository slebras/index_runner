import sys
import json
from confluent_kafka import Producer

from .config import get_config


_HEADERS = {"Content-Type": "application/json"}

# Universal type mappings for every doc in every index.
_GLOBAL_MAPPINGS = {
    'timestamp': {'type': 'date'},
    'obj_name': {'type': 'keyword'},
    'guid': {'type': 'keyword'},
    'creation_date': {'type': 'date'},
    'shared_users': {'type': 'keyword'},
    'access_group': {'type': 'integer'},
    'creator': {'type': 'keyword'},
    'version': {'type': 'integer'},
    'obj_id': {'type': 'integer'},
    'is_public': {'type': 'boolean'},
}

# Type-specific index mappings
_MAPPINGS = {
    'narrative:1': {
        'alias': 'narrative',
        'properties': {
            'narrative_title': {'type': 'text'},
            'version': {'type': 'integer'},
            'obj_id': {'type': 'integer'},
            'data_objects': {
                'type': 'nested',
                'properties': {
                    'name': {'type': 'keyword'},
                    'obj_type': {'type': 'keyword'}
                }
            },
            'cells': {
                'type': 'object',
                'properties': {
                    'desc': {'type': 'text'},
                    'cell_type': {'type': 'keyword'}
                }
            },
            'total_cells': {'type': 'short'},
        }
    },
    "reads:1": {
        'alias': 'reads',
        'properties': {
            'sequencing_tech': {'type': 'keyword'},
            'size': {'type': 'integer'},
            'interleaved': {'type': 'boolean'},
            'single_genome': {'type': 'boolean'},
            'reads_type': {'type': 'keyword'},
            'reads_type_version': {'type': 'keyword'},
            'provenance_services': {'type': 'keyword'},
            'phred_type': {'type': 'text'},
            'gc_content': {'type': 'float'},
            'mean_quality_score': {'type': 'float'},
            'mean_read_length': {'type': 'float'},
        }
    },
    "assembly:1": {
        'alias': 'assembly',
        'properties': {
            "assembly_name": {'type': 'keyword'},
            "mean_contig_length": {'type': 'float'},
            "percent_complete_contigs": {'type': 'float'},
            "percent_circle_contigs": {'type': 'float'},
            "assembly_type": {'type': 'keyword'},
            "assembly_type_version": {'type': 'keyword'},
            "assembly_id": {'type': 'keyword'},
            "gc_content": {'type': 'float'},
            "size": {'type': 'integer'},
            "num_contigs": {'type': 'integer'},
            "taxon_ref": {'type': 'keyword'},
            "external_origination_date": {'type': 'keyword'},  # should maybe be of type 'date'?
            "external_source_id": {'type': 'keyword'},
            "external_source": {'type': 'keyword'},
        }
    },
    "genome:1": {
        'alias': "genome",
        'properties': {
            'genome_id': {'type': 'keyword'},
            'scientific_name': {'type': 'keyword'},
            'size': {'type': 'integer'},
            'num_contigs': {'type': 'integer'},
            'genome_type': {'type': 'keyword'},
            'gc_content': {'type': 'float'},
            'taxonomy': {'type': 'keyword'},
            'mean_contig_length': {'type': 'float'},
            'external_origination_date': {'type': 'keyword'},  # should maybe be of type 'date'?
            'original_source_file_name': {'type': 'keyword'},
        }
    },
    "genome_features:1": {
        'alias': "genome_features",
        'properties': {
                'functions': {'type': 'keyword'},
                'contig_ids': {'type': 'keyword'},
                'sequence_length': {'type': 'integer'},
                'id': {'type': 'keyword'},
                'genome_upa': {'type': 'keyword'},
        }
    }
}


def set_up_indexes():
    print("Setting up indices...")
    config = get_config()
    producer = Producer({'bootstrap.servers': config['kafka_server']})
    for index, mapping in _MAPPINGS.items():
        producer.produce(
            config['topics']['elasticsearch_updates'],
            json.dumps({
                'name': index,
                'alias': mapping['alias'],
                'props': {**mapping['properties'], **_GLOBAL_MAPPINGS}  # type: ignore
            }),
            'init_index',
            callback=_delivery_report
        )
        producer.poll(60)


def _delivery_report(err, msg):
    if err is not None:
        sys.stderr.write(f'Message delivery failed for {msg.key()} in {msg.topic()}: {err}\n')
    else:
        print(f'Message "{msg.key()}" delivered to {msg.topic()}')
