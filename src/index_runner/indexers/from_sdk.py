import os
import uuid
import json
import docker
import requests
from ..utils.config import get_config
from ..utils import ws_utils


_CONFIG = get_config()
_DOCKER = docker.from_env()
_SCRATCH = "/indexer_data"
_NAMESPACE = "WS"



def _get_docker_image_name(module_name, module_version=None):
    """Query the Catalog to get the docker image name for the indexer application"""
    catalog_service_url = _CONFIG['kbase-endpoint'] + '/catalog'
    params = {
        "method": "Catalog.get_module_version",
        "version": "1.1",
        "id": 'id',
        "params": [{
            "module_name": module_name
        }]
    }
    if module_version is not None:
        params['params'][0]['version'] = module_version

    resp = requests.post(catalog_service_url, json.dumps(params))
    try:
        json_resp = resp.json()
    except Exception:
        raise resp.text
    result = json_resp['result'][0]
    return result["docker_img_name"]


def _verify_and_format_output(data_path, workspace_id, object_id, index_name_ver, sub_obj_index):
    """make sure the sdk indexers follow conventions, and stream the if necessary."""
    def check_datatypes(d):
        """verify that the outputs of the sdk indexer follows these rather strict conventions"""
        if isinstance(d, dict):
            for key, val in d.items():
                # not sure if we want to do recursive here or not (not for now)
                if isinstance(key, str):
                    raise ValueError("Keys returned from indexer must be strings")
                if isinstance(val, str) or isinstance(val, int) or isinstance(val, float) or \
                   val is None or isinstance(val, bool):
                    raise ValueError("Values returned from indexer must be strings, integers, floats or Nonetype")
        if isinstance(d, list):
            for val in d:
                check_datatypes(val)

    def format_data(d):
        if d.get('sub_type') and d.get('sub_id'):
            if sub_obj_index is not None:
                index_name = sub_obj_index
            else:
                # we should have a sub_obj_index
                raise ValueError(f"Received 'sub_type' and 'sub_id' fields from indexer with no 'sub_obj_index' \
                                   field specified in the 'sdk_indexer_apps' field for the {index_name_ver} index. ")
            es_id = f"{_NAMESPACE}::{workspace_id}:{object_id}::{d.get('sub_type')}::{d.get('sub_id')}"
        else:
            index_name = index_name_ver
            es_id = f"{_NAMESPACE}::{workspace_id}:{object_id}"
        return {
            "index": index_name,
            "id": es_id,
            "doc": d['doc']
        }

    if os.path.exists(data_path):
        with open(data_path, 'r') as fd:
            for line in fd.readlines():
                data = json.loads(line)
                check_datatypes(data['doc'])
                yield format_data(data)


def _pull_docker_image(image):
    """check if image exists, if not pull it."""
    li = _DOCKER.images.list()
    pulled = False
    for im in li:
        if image in im.tags:
            # id_ = im.id
            pulled = True
    if not pulled:
        print("Pulling %s" % image)
        _DOCKER.images.pull(image).id
    # return id_


def _setup_docker_inputs(job_dir, obj_data, ws_info, obj_data_v1, sdk_image, sdk_func):
    """set up parameters for input to the sdk application"""
    data_dir = job_dir + "/data"
    os.makedirs(data_dir)
    obj_data_path = data_dir + "/obj_data.json"
    ws_info_path = data_dir + "/ws_info.json"
    obj_data_v1_path = data_dir + "/obj_data_v1.json"

    # write data to file
    with open(obj_data_path, "w") as fd:
        json.dump(obj_data, fd)
    with open(ws_info_path, "w") as fd:
        json.dump(ws_info, fd)
    with open(obj_data_v1_path, "w") as fd:
        json.dump(obj_data_v1, fd)

    input_ = {
        "version": "1.1",
        "method": sdk_image + "." + sdk_func,
        "params": [{
            'obj_data_path': obj_data_path,
            'ws_info_path': ws_info_path,
            'obj_data_v1_path': obj_data_v1_path
        }],
        "context": dict()
    }
    ijson = job_dir + "/input.json"
    with open(ijson, "w") as f:
        f.write(json.dumps(input_))


def index_from_sdk(obj_data, ws_info, obj_data_v1):
    """Index from an sdk application"""
    type_module, type_name, type_version = ws_utils.get_type_pieces(obj_data['info'][2])

    indexer_app_vars = _CONFIG['global']['sdk_indexer_apps'][type_module + '.' + type_name]
    sdk_app = indexer_app_vars['sdk_app']
    sdk_func = indexer_app_vars['sdk_func']
    sdk_version = indexer_app_vars.get('sdk_version', None)
    sub_obj_index = indexer_app_vars.get('sub_obj_index', None)
    if sub_obj_index is not None and _CONFIG['global']['latest_versions'].get(sub_obj_index):
        sub_obj_index = _CONFIG['global']['latest_versions'][sub_obj_index]
    else:
        raise ValueError(f"No 'latest_versions' field specified for {sub_obj_index} index in global config")

    workspace_id = obj_data['info'][6]
    object_id = obj_data['info'][0]

    if _CONFIG['global']['ws_type_to_indexes'].get(str(type_module) + "." str(type_name)):
        index_name = _CONFIG['global']['ws_type_to_indexes'][str(type_module) + "." str(type_name)]
    else:
        raise ValueError(f"global config does not nto have 'ws_type_to_indexes' field for {type_module}.{type_name}")
    if _CONFIG['global']['latest_versions'].get(index_name):
        index_name_ver = _CONFIG['global']['latest_versions'][index_name]
    else:
        raise ValueError(f"global config does not have 'latest_versions' field for {index_name} \
                index with workspace object type {type_module}.{type_name}:{type_version}")

    image = _get_docker_image_name(sdk_image, sdk_version)
    _pull_docker_image(image)

    # maybe make this tempfile stuff?
    job_id = str(uuid.uuid1())
    job_dir = _SCRATCH + "/" + job_id
    os.makedirs(job_dir)

    # write inputs to files
    _setup_docker_inputs(job_dir, obj_data, ws_info, obj_data_v1, sdk_image, sdk_func)

    # with open(job_dir + "/token", "w") as f:
    #     f.write(self.token)
    vols = {
        job_dir: {'bind': '/kb/module/work', 'mode': 'rw'}
    }
    # not sure why we are doing this
    env = {
        'SDK_CALLBACK_URL': 'not_supported_yet',
    }

    # Run docker container.
    _DOCKER.containers.run(image, 'async',
                           environment=env,
                           volumes=vols)

    with open(job_dir + "/output.json") as fd:
        job_out = json.load(fd)

    # DO SOME ERROR CHECKING HERE
    yield _verify_and_format_output(job_out['filepath'], workspace_id, object_id, index_name_ver, sub_obj_index)
