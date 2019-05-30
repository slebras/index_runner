import sys
import json
from confluent_kafka import Producer

from . import ws_type
from .config import get_config


_HEADERS = {"Content-Type": "application/json"}
_CONFIG = get_config()
# Universal type mappings for every doc in every index.

_GLOBAL_MAPPINGS = _CONFIG['global']['global_ws_mappings']
_MAPPINGS = _CONFIG['global']['mappings']
# _GLOBAL_MAPPINGS = {
#     'timestamp': {'type': 'date'},
#     'obj_name': {'type': 'keyword'},
#     'guid': {'type': 'keyword'},
#     'creation_date': {'type': 'date'},
#     'shared_users': {'type': 'keyword'},
#     'access_group': {'type': 'integer'},
#     'creator': {'type': 'keyword'},
#     'version': {'type': 'integer'},
#     'obj_id': {'type': 'integer'},
#     'is_public': {'type': 'boolean'},
#     'copied': {'type': 'keyword'},
#     'tags': {'type': 'keyword'},
#     'obj_type_version': {'type': 'keyword'},
#     'obj_type_module': {'type': 'keyword'},
#     'obj_type_name': {'type': 'keyword'}
# }

# Type-specific index mappings
# _MAPPINGS = {
#     'narrative:1': {
#         'alias': 'narrative',
#         'properties': {
#             'narrative_title': {'type': 'text'},
#             'data_objects': {
#                 'type': 'nested',
#                 'properties': {
#                     'name': {'type': 'keyword'},
#                     'obj_type': {'type': 'keyword'}
#                 }
#             },
#             'cells': {
#                 'type': 'object',
#                 'properties': {
#                     'desc': {'type': 'text'},
#                     'cell_type': {'type': 'keyword'}
#                 }
#             },
#             'total_cells': {'type': 'short'},
#         }
#     },
#     "reads:1": {
#         'alias': 'reads',
#         'properties': {
#             'sequencing_tech': {'type': 'keyword'},
#             'size': {'type': 'integer'},
#             'interleaved': {'type': 'boolean'},
#             'single_genome': {'type': 'boolean'},
#             'provenance_services': {'type': 'keyword'},
#             'phred_type': {'type': 'text'},
#             'gc_content': {'type': 'float'},
#             'mean_quality_score': {'type': 'float'},
#             'mean_read_length': {'type': 'float'},
#         }
#     },
#     "assembly:1": {
#         'alias': 'assembly',
#         'properties': {
#             "assembly_name": {'type': 'keyword'},
#             "mean_contig_length": {'type': 'float'},
#             "percent_complete_contigs": {'type': 'float'},
#             "percent_circle_contigs": {'type': 'float'},
#             "assembly_id": {'type': 'keyword'},
#             "gc_content": {'type': 'float'},
#             "size": {'type': 'integer'},
#             "num_contigs": {'type': 'integer'},
#             "taxon_ref": {'type': 'keyword'},
#             "external_origination_date": {'type': 'keyword'},  # should maybe be of type 'date'?
#             "external_source_id": {'type': 'keyword'},
#             "external_source": {'type': 'keyword'},
#         }
#     },
#     "genome:1": {
#         'alias': "genome",
#         'properties': {
#             'genome_id': {'type': 'keyword'},
#             'scientific_name': {'type': 'keyword'},
#             'size': {'type': 'integer'},
#             'num_contigs': {'type': 'integer'},
#             'genome_type': {'type': 'keyword'},
#             'gc_content': {'type': 'float'},
#             'taxonomy': {'type': 'keyword'},
#             'mean_contig_length': {'type': 'float'},
#             'external_origination_date': {'type': 'keyword'},  # should maybe be of type 'date'?
#             'original_source_file_name': {'type': 'keyword'},
#             # new fields to include:
#             'cds_count': {'type': 'integer'},
#             'feature_count': {'type': 'integer'},
#             'mrna_count': {'type': 'integer'},
#             'non_coding_feature_count': {'type': 'integer'},
#             'assembly_ref': {'type': 'keyword'},
#             'source_id': {'type': 'keyword'},
#             'feature_counts': {'type': 'object'},
#             'source': {'type': 'keyword'},
#             'warnings': {'type': 'text'},
#         }
#     },
#     "genome_features:1": {
#         'alias': "genome_features",
#         'properties': {
#                 'feature_type': {'type': 'keyword'},
#                 'functions': {'type': 'keyword'},
#                 'contig_ids': {'type': 'keyword'},
#                 'sequence_length': {'type': 'integer'},
#                 'id': {'type': 'keyword'},
#                 # 'genome_upa': {'type': 'keyword'},
#                 'guid': {'type': 'keyword'},
#                 'genome_version': {'type': 'integer'},
#                 # new fields to include:
#                 'assembly_ref': {'type': 'keyword'},
#                 'genome_feature_type': {'type': 'keyword'},
#                 'starts': {'type': 'integer'},
#                 'strands': {'type': 'keyword'},
#                 'stops': {'type': 'integer'},
#                 'aliases': {'type': 'keyword'},
#         }
#     },
#     "pangenome:1": {
#         'alias': "pangenome",
#         'properties': {
#             'pangenome_id': {'type': 'keyword'},
#             'pangenome_name': {'type': 'keyword'},
#             'pangenome_type': {'type': 'keyword'},
#             'genome_upas': {'type': 'keyword'},
#         }
#     },
#     "pangenome_orthologfamily:1": {
#         'alias': "pangenome_orthologfamily",
#         'properties': {
#                 'ortholog_id': {'type': 'keyword'},
#                 'ortholog_type': {'type': 'keyword'},
#                 'function': {'type': 'keyword'},
#                 'gene_ids': {'type': 'keyword'},
#         }
#     },
#     "taxon:1": {
#         'alias': "taxon",
#         'properties': {
#             'scientific_name': {'type': 'keyword'},
#             'scientific_lineage': {'type': 'keyword'},
#             'domain': {'type': 'keyword'},
#             'kingdom': {'type': 'keyword'},
#             'parent_taxon_ref': {'type': 'keyword'},
#             'genetic_code': {'type': 'integer'},
#             'aliases': {'type': 'keyword'}
#         }
#     },
#     "tree:1": {
#         'alias': "tree",
#         'properties': {
#             'tree_name': {'type': 'keyword'},
#             'type': {'type': 'keyword'},
#             'labels': {
#                 'type': 'nested',
#                 'properties': {
#                     'node_id': {'type': 'text'},
#                     'label': {'type': 'text'}
#                 }
#             },
#         }
#     }
# }


def set_up_indexes():
    print("Setting up indices...")
    producer = Producer({'bootstrap.servers': _CONFIG['kafka_server']})
    for index, mapping in _MAPPINGS.items():
        producer.produce(
            _CONFIG['topics']['elasticsearch_updates'],
            json.dumps({
                'name': index,
                'alias': mapping['alias'],
                'props': {**mapping['properties'], **_GLOBAL_MAPPINGS}  # type: ignore
            }),
            'init_index',
            callback=_delivery_report
        )
        producer.poll(60)


def set_up_generic_index(full_type_name):
    """
    Set up an index for a generic type for which we don't have a specific
    indexer function.
    """
    producer = Producer({'bootstrap.servers': _CONFIG['kafka_server']})
    (module_name, type_name, type_ver) = ws_type.get_pieces(full_type_name)
    index_name = type_name.lower()
    producer.produce(
        _CONFIG['topics']['elasticsearch_updates'],
        json.dumps({
            'name': index_name + ':0',
            'alias': index_name,
            'props': _GLOBAL_MAPPINGS
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
