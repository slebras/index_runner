"""
This indexer is for the Sample objects represented in the SampleService and
the SampleSet object in the Workspace.
"""
# import uuid
from src.utils.es_utils import _get_document
from src.utils.sample_utils import get_sample as _get_sample


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


def main(obj_data, ws_info, obj_data_v1, conf):
    """Indexer for KBaseSets.SampleSet object type"""
    info = obj_data['info']
    if not obj_data.get('data'):
        raise Exception("no data in object")
    data = obj_data['data']
    workspace_id = info[6]
    object_id = info[0]
    version = info[4]
    sample_set_id = f"{conf['namespace']}::{workspace_id}:{object_id}"
    ver_sample_set_id = f"{conf['ver_namespace']}::{workspace_id}:{object_id}:{version}"

    sample_set_index = {
        "_action": "index",
        "doc": {
            "description": data["description"],
            "sample_ids": [s['id'] for s in data['samples']],
            "sample_names": [s['name'] for s in data['samples']],
            "sample_versions": [s['version'] for s in data['samples']]
        },
        "index": conf['sample_set_index_name'],
        "id": sample_set_id
    }
    yield sample_set_index
    ver_sample_set_index = dict(sample_set_index)
    ver_sample_set_index['index'] = conf['ver_sample_set_index_name']
    ver_sample_set_index['id'] = ver_sample_set_id
    yield ver_sample_set_index

    for samp in data["samples"]:
        # query the sample service for sample

        sample_id = f"{conf['sample_namespace']}::{samp['id']}"
        if samp.get('version'):
            sample_id += f":{samp['version']}"
            sample = None
        else:
            # get latest version of sample
            sample = _get_sample(samp)
            sample_id += f":{sample['version']}"
        # check if sample already indexed
        document = _get_document("sample", sample_id)
        if document:
            # up date index to include this WS
            document['sample_set_ids'].append(ver_sample_set_id)
        else:
            if sample is None:
                sample = _get_sample(samp)
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
                if 'node_id' in meta_controlled:
                    meta_controlled['node_id'].append(node['id'])
                else:
                    meta_controlled['node_id'] = [node['id']]

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
            "index": conf['sample_index_name'],
            "id": sample_id
        }
        yield sample_index
