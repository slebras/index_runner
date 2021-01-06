'''
Create a link for SampleSet
'''
import time
import hashlib as _hashlib
import datetime as _datetime
from typing import List, Dict
# from collections import defaultdict as _defaultdict
# import itertools as _itertools

from src.utils.logger import logger
from src.utils.re_client import stored_query as _stored_query
from src.utils.re_client import save as _save
from src.utils.re_client import get_doc as _get_doc
from src.utils.re_client import clean_key as _clean_key
from src.utils.sample_utils import get_sample as _get_sample
from src.utils.re_client import MAX_ADB_INTEGER as _MAX_ADB_INTEGER


# EXTREMELY HACKY FOR NOW. SHOULD BE RETRIEVED (?) FROM THE SAMPLE_SERVICE
SAMPLE_COLL = 'samples_sample'
SAMPLE_VER_COLL = 'samples_version'
SAMPLE_NODE_COLL = 'samples_nodes'
SAMPLE_ONTOLOGY_COLL = 'sample_ontology_link'

# This should be information retrieved from the 'metadata_validation.yml'
# file in the sample_service_validator_config
SAMPLE_ONTOLOGY_VALIDATED_TERMS = [
    ('biome', 'ENVO_terms'),
    ('feature', 'ENVO_terms'),
    ('ENIGMA:material', 'ENVO_terms')
]


def process_sample_set(obj_ver_key: str, obj_data: dict) -> None:
    """
    obj_ver_key: object version key
    obj_data: object data
    """
    # term_bank dictionary for storing arango document information about
    # already encountered terms. mapping of ontology_term -> arango "_id" field
    term_bank: Dict[str, str] = {}
    edges: List[dict] = []
    # iterate per sample
    for sample_info in obj_data['data']['samples']:
        # retrieve the sample metadata
        sample = _get_sample(sample_info)
        sample_version_uuid = _get_sample_version_uuid(sample)
        # term_bank object and edges list passed by reference
        # find terms we know are ontology terms
        _generate_link_information(sample, sample_version_uuid, edges, term_bank)
    # add creation timestamp for edge link, (same for all edges).
    created_timestamp = _now_epoch_ms() + 20 * len(edges)  # allow 20 ms to transport & save each edge
    for e in edges:
        e['created'] = created_timestamp
    logger.info(f'Writing {len(edges)} sample -> ontology edges '
                f'for samples in SampleSet {obj_ver_key}')
    # save link in bulk operation
    _save(SAMPLE_ONTOLOGY_COLL, edges)


def _now_epoch_ms() -> int:
    return int(_datetime.datetime.now(tz=_datetime.timezone.utc).timestamp()) * 1000


def _get_sample_version_uuid(sample: dict) -> str:
    '''
    sample: sample object as defined in SampleService
    '''
    sample_id = sample['id']
    sample_doc = _get_doc(SAMPLE_COLL, sample_id)
    maxver = len(sample_doc['vers'])
    version = sample.get('version', maxver)
    if version > maxver:
        raise ValueError(f'No version "{version}" for sample {sample_id}. Current max version={maxver}')
    sample_version_uuid = sample_doc['vers'][version - 1]
    return sample_version_uuid


def _hash(string: str) -> str:
    return _hashlib.md5(string.encode("utf-8")).hexdigest()  # nosec


def _generate_link_information(sample: dict, sample_version_uuid: str, edges: list, term_bank: dict):
    '''
    sample: sample object as defined in SampleService
    sample_version_uuid: uuid identifier for sample version document
    edges: list to append new edge documents to
    term_bank: dictionary of ontology_id stored in samples to ontology document id in arango
    '''
    # iterate through the sample nodes
    for node in sample['node_tree']:
        node_id = node['id']
        # used as part of _key for node in arango
        node_uuid = _hash(node_id)
        node_key = f"{sample['id']}_{sample_version_uuid}_{node_uuid}"
        node_doc_id = f"{SAMPLE_NODE_COLL}/{node_key}"
        # find terms we know are ontology terms
        for metadata_term, ontology_collection in SAMPLE_ONTOLOGY_VALIDATED_TERMS:
            if node['meta_controlled'].get(metadata_term):
                # for now, this is the only way that ontology_terms are stored
                ontology_id = node['meta_controlled'][metadata_term]['value']
                if term_bank.get(ontology_id):
                    # use existing document information, avoid new query
                    ontology_doc_id = term_bank[ontology_id]
                else:
                    adb_resp = _stored_query('ontology_get_terms', {
                       'ids': [str(ontology_id)],
                       'ts': int(time.time() * 1000),
                       '@onto_terms': ontology_collection
                    })
                    adb_results = adb_resp['results']
                    if not adb_results:
                        logger.info(f'No ontology node in database for id {ontology_id}')
                        continue
                    ontology_doc_id = adb_results[0]['_id']
                    # save ontology_id document address
                    term_bank[ontology_id] = ontology_doc_id
                # ontology_doc_id contains the source ontology
                ontology_collection, ontology_doc_key = ontology_doc_id.split('/')
                edge = {
                    "_from": node_doc_id,
                    "_to": ontology_doc_id,
                    "_key": _clean_key(f"{node_uuid}_{ontology_doc_key}"),  # placeholder _key for now.
                    "createdby": "_system",  # Should be owner of sample (?)
                    "expired": _MAX_ADB_INTEGER,
                    "sample_id": sample['id'],
                    "sample_version": sample['version'],
                    "sample_version_uuid": sample_version_uuid,
                    "sample_node_name": node_id,
                    "sample_node_uuid": node_uuid,
                    "sample_metadata_term": metadata_term,
                    "ontology_term": ontology_id,
                    "ontology_collection": ontology_collection
                }
                edges.append(edge)
