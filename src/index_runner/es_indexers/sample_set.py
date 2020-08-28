from src.utils.config import config
import json
# import uuid
import requests


_NAMESPACE = "WS"
_VER_NAMESPACE = "WSVER"
_SAMPLE_NAMESPACE = "SMP"
# versioned and non-versioned index have same version
_SAMPLE_SET_INDEX_VERSION = 1
_SAMPLE_SET_INDEX_NAME = 'sample_set_' + str(_SAMPLE_SET_INDEX_VERSION)
_VER_SAMPLE_SET_INDEX_NAME = 'sample_set_version_' + str(_SAMPLE_SET_INDEX_VERSION)
# versioned and non-versioned index have same version
_SAMPLE_INDEX_VERSION = 1
_SAMPLE_INDEX_NAME = 'sample_' + str(_SAMPLE_INDEX_VERSION)
# _VER_SAMPLE_INDEX_NAME = 'sample_version_' + str(_SAMPLE_INDEX_VERSION)


def _get_document(sample_document_id):
    """ Get Sample document (if it exists) from Elasticsearch, otherwise return None
    sample_document_id - "SMP::kbase_sample_id:kbase_sample_version"
    """
    es_url = config()['elasticsearch_url']
    prefix = config()['elasticsearch_index_prefix']
    es_url += f"/{prefix}.sample/_doc/{sample_document_id}"
    resp = requests.get(url=es_url)
    # if not resp.ok:
    #     raise Exception(f"Not able to query {es_url} - {resp.text}")
    respj = resp.json()
    if respj.get('error'):
        raise Exception(f"Query to {es_url} resulted in error - {respj['error']}")
    if respj['found']:
        return respj['_source']
    else:
        return None


def _get_sample(sample_info):
    """ Get sample from SampleService
    sample_info - dict containing 'id' and 'version' of a sample
    """
    headers = {"Authorization": config()['ws_token']}
    params = {
        "id": sample_info['id'],
        "as_admin": True
    }
    if sample_info.get('version'):
        params['version'] = sample_info['version']
    payload = {
        "method": "SampleService.get_sample",
        "id": "",  # str(uuid.uuid4()),
        "params": [params],
        "version": "1.1"
    }
    resp = requests.post(url=config()['sample_service_url'], headers=headers, data=json.dumps(payload))
    if not resp.ok:
        raise RuntimeError(f"Returned from sample service with status {resp.status_code} - {resp.text}")
    resp_json = resp.json()
    if resp_json.get('error'):
        raise RuntimeError(f"Error from SampleService - {resp_json['error']}")
    sample = resp_json['result'][0]
    return sample


def _flatten_meta(meta, prefix=None):
    """ Flattens metadata fields in a Sample object. Fields are concatenated into a
        single string field to save into an Elasticsearch index
    meta   - Sample Metadata to be flattened
    prefix - (optional) prefix for the metadata values. default=None
    """
    new_meta = {}
    for key in meta:
        if prefix:
            val = prefix + ":"
        else:
            val = ""
        if "value" in meta[key]:
            val += str(meta[key]['value'])
        if "units" in meta[key]:
            val += ";" + str(meta[key]['units'])
        new_meta[key] = val
    return new_meta


def _combine_meta(meta, flattened_meta, idx):
    """ Combine newly flattened metadata with existing metadata. This Function is designed to keep the indexing
        of the different metadata fields consistent for each node within the sample node tree s.t. all the
        fields in index (idx) 0 will be from item 0 in the node tree. Empty string ("") entries are Empty and
        added simply so that the indexing of all fields line up.
    meta           - existing metadata.
    flattened_meta - newly flattened metadata.
    idx            - current index of ndoe_tree.
    """
    for key in flattened_meta:
        if key in meta:
            meta[key] += ["" for _ in range(idx - len(meta[key]))] + [flattened_meta[key]]
        else:
            meta[key] = ["" for _ in range(idx)] + [flattened_meta[key]]
    return meta


def index_sample_set(obj_data, ws_info, obj_data_v1):
    """Indexer for KBaseSets.SampleSet object type"""
    info = obj_data['info']
    if not obj_data.get('data'):
        raise Exception("no data in object")
    data = obj_data['data']
    workspace_id = info[6]
    object_id = info[0]
    version = info[4]
    sample_set_id = f"{_NAMESPACE}::{workspace_id}:{object_id}"
    ver_sample_set_id = f"{_VER_NAMESPACE}::{workspace_id}:{object_id}:{version}"

    sample_set_index = {
        "_action": "index",
        "doc": {
            "description": data["description"],
            "sample_ids": [s['id'] for s in data['samples']],
            "sample_names": [s['name'] for s in data['samples']],
            "sample_versions": [s['version'] for s in data['samples']]
        },
        "index": _SAMPLE_SET_INDEX_NAME,
        "id": sample_set_id
    }
    yield sample_set_index
    ver_sample_set_index = dict(sample_set_index)
    ver_sample_set_index['index'] = _VER_SAMPLE_SET_INDEX_NAME
    ver_sample_set_index['id'] = ver_sample_set_id
    yield ver_sample_set_index

    for samp in data["samples"]:
        # query the sample service for sample

        sample_id = f"{_SAMPLE_NAMESPACE}::{samp['id']}"
        if samp.get('version'):
            sample_id += f":{samp['version']}"
            sample = None
        else:
            # get latest version of sample
            sample = _get_sample(samp)
            sample_id += f":{sample['version']}"
        # check if sample already indexed
        document = _get_document(sample_id)
        if document:
            # up date index to include this WS
            document['sample_set_ids'].append(ver_sample_set_id)
        else:
            if sample is None:
                sample = _get_sample(samp)
            # not sure on how we need to handle more than 1 node.
            if len(sample['node_tree']) == 1:
                meta_controlled = _flatten_meta(
                    sample['node_tree'][0]['meta_controlled']
                )
                meta_user = _flatten_meta(
                    sample['node_tree'][0]['meta_user']
                )
                meta_controlled['node_id'] = sample['node_tree'][0]['id']
            else:
                meta_controlled, meta_user = {}, {}
                for idx, node in enumerate(sample['node_tree']):
                    meta_controlled = _combine_meta(
                        meta_controlled,
                        _flatten_meta(
                            node['meta_controlled']
                        ),
                        idx
                    )
                    meta_user = _combine_meta(
                        meta_user,
                        _flatten_meta(
                            node['meta_user']
                        ),
                        idx
                    )
                    meta_controlled['node_id'] = node['id']

            document = {
                "save_date": sample['save_date'],
                "sample_version": sample['version'],
                "name": sample['name'],
                "sample_set_ids": [ver_sample_set_id],
                **meta_user,
                **meta_controlled
            }

        sample_index = {
            "_action": "index",
            "doc": document,
            "index": _SAMPLE_INDEX_NAME,
            "id": sample_id
        }
        yield sample_index
