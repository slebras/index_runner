# KBaseMetagenomes.AnnotatedMetagenomeAssembly indexer
from src.index_runner.es_indexers.indexer_utils import mean, handle_id_to_file

import json
import gzip
import shutil
import os

_NAMESPACE = "WS"
_VER_NAMESPACE = "WSVER"
_AMA_INDEX_VERSION = 1
_AMA_FEATURES_INDEX_VERSION = 1
_AMA_INDEX_NAME = "annotated_metagenome_assembly:" + str(_AMA_INDEX_VERSION)
_AMA_FEATURES_INDEX_NAME = "annotated_metagenome_assembly_features:" + str(_AMA_FEATURES_INDEX_VERSION)
# version indices
_VER_AMA_INDEX_VERSION = 1
_VER_AMA_FEATURES_INDEX_VERSION = 1
_VER_AMA_INDEX_NAME = "annotated_metagenome_assembly_version:" + str(_VER_AMA_INDEX_VERSION)
_VER_AMA_FEATURES_INDEX_NAME = "annotated_metagenome_assembly_features_version:" + str(_VER_AMA_FEATURES_INDEX_VERSION)

_DIR = os.path.dirname(os.path.realpath(__file__))


def _index_ama(features_file_gz_path, data, ama_id, ver_ama_id):
    """"""
    publication_titles = [pub[2] for pub in data.get('publications', [])]
    publication_authors = [pub[5] for pub in data.get('publications', [])]
    ama_index = {
        '_action': 'index',
        'doc': {
            'size': data.get('dna_size'),
            'source_id': data.get('source_id'),
            'source': data.get('source'),
            'gc_content': data.get('gc_content'),
            'warnings': data.get('warnings'),
            'num_contigs': data.get('num_contigs'),
            'mean_contig_length': mean(data.get('contig_lengths', [])),
            'external_source_origination_date': data.get('external_source_origination_date'),
            'original_source_file_name': data.get('original_source_file_name'),
            'environment': data.get('environment'),
            'num_features': data.get('num_features'),
            'publication_authors': publication_authors,
            'publication_titles': publication_titles,
            'molecule_type': data.get('molecule_type'),
            'assembly_ref': data.get('assembly_ref'),
            'notes': data.get('notes'),
            # not sure what to do with the following fields.
            # list<Ontology_event> ontology_events;
            # mapping<string, mapping<string, string>> ontologies_present;
        },
        'index': _AMA_INDEX_NAME,
        'id': ama_id
    }
    ama_index['id'] = ama_id
    yield ama_index
    ama_index['id'] = ver_ama_id
    ama_index['index'] = _VER_AMA_INDEX_NAME
    yield ama_index

    # unzip gzip file.
    features_file_path = _DIR + "/features.json"
    with gzip.open(features_file_gz_path, "rb") as f_in:
        with open(features_file_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    with open(features_file_path) as f:
        features = json.load(f)

    for feat in features:
        id_ = feat.get('id')
        feat_id = ama_id + f"::ama_ft::{id_}"
        ver_feat_id = ver_ama_id + f"::ama_ft::{id_}"
        # calculate gc content for each feature.
        # if feat.get('dna_sequence'):
        #     dna_seq = feat.get('dna_sequence')
        #     feat_gc_content = ((float(dna_seq.lower().count('c')) + float(dna_seq.lower().count('g'))) / len(dna_seq))

        if feat.get('location'):
            contig_ids, starts, strands, stops = zip(*feat.get('location'))
            contig_ids, starts, strands, stops = list(contig_ids), list(starts), list(strands), list(stops)
        else:
            contig_ids, starts, strands, stops = None, None, None, None

        feat_index = {
            '_action': 'index',
            'doc': {
                'id': id_,
                'type': feat.get('type'),
                'size': feat.get('dna_sequence_length'),
                'starts': starts,
                'strands': strands,
                'stops': stops,
                'contig_ids': contig_ids,
                'functions': feat.get('functions'),
                'functional_descriptions': feat.get('functional_descriptions'),
                'warnings': feat.get('warnings'),
                'parent_gene': feat.get('parent_gene'),
                'inference_data': feat.get('inference_data'),
                'dna_sequence': feat.get('dna_sequence'),
                # 'gc_content': feat_gc_content,
                # Parent ids below
                'parent_id': ama_id,
                'annotated_metagenome_assembly_size': data.get('dna_size'),
                'annotated_metagenome_assembly_num_features': data.get('num_features'),
                'annotated_metagenome_assembly_num_contigs': data.get('num_contigs'),
                'annotated_metagenome_assembly_gc_content': data.get('gc_content')
            },
            'index': _AMA_FEATURES_INDEX_NAME,
            'id': feat_id
        }
        yield feat_index
        feat_index['id'] = ver_feat_id
        feat_index['doc']['parent_id'] = ver_ama_id
        feat_index['index'] = _VER_AMA_FEATURES_INDEX_NAME
        yield feat_index
    # remove unzipped file
    os.remove(features_file_path)


def index_annotated_metagenome_assembly(obj_data, ws_info, obj_data_v1):
    """
    Currently indexes following workspace types:
        ci:              KBaseMetagenomes.AnnotatedMetagenomeAssembly-1.0
        narrative(prod): KBaseMetagenomes.AnnotatedMetagenomeAssembly-1.0
    """
    if not obj_data.get('data'):
        raise Exception("no data in object")
    data = obj_data.get('data')
    info = obj_data.get('info')
    workspace_id = info[6]
    object_id = info[0]
    version = info[4]

    ama_id = f"{_NAMESPACE}::{workspace_id}:{object_id}"
    ver_ama_id = f"{_VER_NAMESPACE}::{workspace_id}:{object_id}:{version}"

    if not data.get('features_handle_ref'):
        raise Exception("AnnotatedMetagenomeAssembly object does not have features_handle_ref"
                        " field. Can not index features to ElasticSearch.")

    # Download features file
    features_handle_ref = data.get('features_handle_ref')
    features_file_gz_path = _DIR + "/features.json.gz"
    handle_id_to_file(features_handle_ref, features_file_gz_path)

    for doc in _index_ama(features_file_gz_path, data, ama_id, ver_ama_id):
        yield doc
    # remove zipped file
    os.remove(features_file_gz_path)
