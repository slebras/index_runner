import os
import uuid
import json
import shutil
import docker
import requests
import logging
from configparser import ConfigParser

from src.utils.config import config
from src.utils import ws_utils

logging.getLogger(__name__)

_DOCKER = docker.from_env()
_SCRATCH = "/scratch"
_NAMESPACE = "WS"
# the mount needs to be the absolute path on the machine that started the index_runner
_MOUNT_DIR = config()["mount_dir"]
_TOKEN = config()['ws_token']
_IN_APP_JOB_DIR = "/kb/module/work"


def _get_docker_image_name(sdk_app, module_version=None):
    """Query the Catalog to get the docker image name for the indexer application"""
    catalog_service_url = config()['catalog_url']
    params = {
        "method": "Catalog.get_module_version",
        "version": "1.1",
        "id": 'id',  # this is arbitrary/not used (but required).
        "params": [{
            "module_name": sdk_app
        }]
    }
    if module_version is not None:
        params['params'][0]['version'] = module_version  # type: ignore
    resp = requests.post(url=catalog_service_url, data=json.dumps(params))
    try:
        json_resp = resp.json()
    except Exception:
        raise ValueError(resp.text)
    if json_resp.get('error'):
        raise RuntimeError(json_resp['error'])
    result = json_resp['result'][0]
    return result["docker_img_name"]


def _verify_and_format_output(data_path, job_dir, workspace_id, object_id, index_name_ver, sub_obj_index):
    """make sure the sdk indexers follow conventions."""
    def check_datatypes(d):
        """verify that the outputs of the sdk indexer follows these rather strict conventions"""
        if isinstance(d, dict):
            for key, val in d.items():
                # not sure if we want to do recursive here or not (not for now)
                if not isinstance(key, str):
                    raise ValueError(f"Keys returned from indexer must be strings instead \
                                       key '{key}' is type '{type(key)}'")
                check_datatypes(val)
        elif isinstance(d, list):
            for val in d:
                check_datatypes(val)
        else:
            if d is not None and not isinstance(d, str) and not isinstance(d, int) \
               and not isinstance(d, float) and not isinstance(d, bool):
                raise ValueError(f"Values returned from indexer must be strings, integers, \
                                   floats or Nonetype, instead val '{d}' is type '{type(d)}'")

    def format_data(d):
        if d.get('sub_type') and d.get('sub_id'):
            if sub_obj_index is not None:
                index_name = sub_obj_index
            else:
                # we should have a sub_obj_index
                raise ValueError(f"Received 'sub_type' and 'sub_id' fields from indexer with no 'sub_obj_index' \
                                   field specified in the 'sdk_indexer_apps' field for the {index_name_ver} index. ")
            es_id = f"{_NAMESPACE}::{workspace_id}:{object_id}::{d.get('sub_type')}::{d.get('sub_id')}"
        elif d.get('sub_type') or d.get('sub_id'):
            raise ValueError(f"Only one of sub_id or sub_type specified, both or neither required. object: {d}")
        else:
            index_name = index_name_ver
            es_id = f"{_NAMESPACE}::{workspace_id}:{object_id}"
        ret = {
            "_action": "index",
            "index": index_name,
            "id": es_id,
            "doc": d['doc']
        }
        if d.get('_no_defaults'):
            ret["_no_defaults"] = True
        return ret

    if os.path.exists(data_path):
        with open(data_path, 'r') as fd:
            for line in fd.readlines():
                data = json.loads(line)
                check_datatypes(data['doc'])
                yield format_data(data)
    else:
        raise RuntimeError(f"filepath {data_path} does not exist.")
    _cleanup(job_dir)


def _cleanup(job_dir):
    """Cleanup after all indices are yielded, delete the job directory to avoid excessive memory usage."""
    shutil.rmtree(job_dir)


def _pull_docker_image(image):
    """check if image exists, if not pull it."""
    li = _DOCKER.images.list()
    pulled = False
    for im in li:
        if image in im.tags:
            # id_ = im.id
            pulled = True
    if not pulled:
        logging.info("Pulling %s" % image)
        _DOCKER.images.pull(image)


def _setup_docker_inputs(job_dir, obj_data, ws_info, obj_data_v1, sdk_app, sdk_func):
    """set up parameters for input to the sdk application"""
    data_dir = job_dir + "/data"
    os.makedirs(data_dir)
    scratch_dir = job_dir + "/tmp"  # nosec
    os.mkdir(scratch_dir)  # nosec

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

    # we want to provide the app the path within its context.
    obj_data_path = _IN_APP_JOB_DIR + "/data/obj_data.json"
    ws_info_path = _IN_APP_JOB_DIR + "/data/ws_info.json"
    obj_data_v1_path = _IN_APP_JOB_DIR + "/data/obj_data_v1.json"

    input_ = {
        "version": "1.1",
        "method": sdk_app + "." + sdk_func,
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

    # write config for sdk application
    sdk_config = ConfigParser()
    sdk_config['global'] = {
        'kbase_endpoint': config()['kbase_endpoint'],
        'workspace_url': config()['workspace_url'],
        'scratch': "/kb/module/work/tmp"
    }
    with open(job_dir + '/config.properties', 'w') as configfile:
        sdk_config.write(configfile)

    # set up token.
    with open(job_dir + '/token', 'w') as fd:
        fd.write(_TOKEN)


def _get_index_name(type_module, type_name, type_version):
    """Get the name of the index we write to, defined in the global config."""
    if config()['global']['ws_type_to_indexes'].get(type_module + "." + type_name):
        index_name = config()['global']['ws_type_to_indexes'][type_module + "." + type_name]
    else:
        raise ValueError(f"global config does not have 'ws_type_to_indexes' field for {type_module}.{type_name}")
    if config()['global']['latest_versions'].get(index_name):
        index_name_ver = config()['global']['latest_versions'][index_name]
    else:
        raise ValueError(f"global config does not have 'latest_versions' field for {index_name} \
                index with workspace object type {type_module}.{type_name}:{type_version}")
    return index_name_ver


def _get_sub_obj_index(indexer_app_vars):
    """Get the name of the sub object index, if applicable, return None otherwise."""
    sub_obj_index = indexer_app_vars.get('sub_obj_index', None)
    if config()['global']['latest_versions'].get(sub_obj_index):
        sub_obj_index = config()['global']['latest_versions'][sub_obj_index]
    elif sub_obj_index is None:
        # here we expect no sub_obj_index, so we move on
        pass
    else:
        raise ValueError(f"No 'latest_versions' field specified for {sub_obj_index} index in global config")
    return sub_obj_index


def index_from_sdk(obj_data, ws_info, obj_data_v1):
    """Index from an sdk application"""
    type_module, type_name, type_version = ws_utils.get_type_pieces(obj_data['info'][2])

    indexer_app_vars = config()['global']['sdk_indexer_apps'][type_module + '.' + type_name]
    sdk_app = indexer_app_vars['sdk_app']
    sdk_func = indexer_app_vars['sdk_func']
    sdk_version = indexer_app_vars.get('sdk_version', None)
    sub_obj_index = _get_sub_obj_index(indexer_app_vars)

    workspace_id = obj_data['info'][6]
    object_id = obj_data['info'][0]

    index_name_ver = _get_index_name(type_module, type_name, type_version)
    image = _get_docker_image_name(sdk_app, sdk_version)
    _pull_docker_image(image)

    job_dir = _SCRATCH + "/" + str(uuid.uuid1())
    os.makedirs(job_dir)
    _setup_docker_inputs(job_dir, obj_data, ws_info, obj_data_v1, sdk_app, sdk_func)

    # the volume mount must be relative to the Host, so we add _MOUNT_DIR to job_directory.
    vols = {
        _MOUNT_DIR + job_dir: {'bind': _IN_APP_JOB_DIR, 'mode': 'rw'}
    }
    env = {
        'SDK_CALLBACK_URL': 'not_supported_yet',
        'KBASE_ENDPOINT': config()['kbase_endpoint']
    }

    # Run docker container.
    _DOCKER.containers.run(image, 'async',
                           environment=env,
                           volumes=vols)

    with open(job_dir + "/output.json") as fd:
        job_out = json.load(fd)
    if job_out.get('error'):
        raise RuntimeError(f"Error from sdk application: {job_out['error']}")
    job_out = job_out['result'][0]
    if job_out.get('filepath'):
        filepath = job_out['filepath'].replace(_IN_APP_JOB_DIR, job_dir, 1)
    else:
        raise RuntimeError(f"Unknown sdk application error: {job_out}")

    return _verify_and_format_output(filepath, job_dir, workspace_id, object_id, index_name_ver, sub_obj_index)
