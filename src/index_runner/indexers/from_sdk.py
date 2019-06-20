"""
Import 
"""
import uuid
import json
import docker
import requests
import tempfile
from ..utils.config import get_config

_CONFIG = get_config()
_DOCKER = docker.from_env()
_SCRATCH = _CONFIG['scratch']
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
    if module_version != None:
        params['params'][0]['version'] = module_version

    resp = requests.post(catalog_service_url, json.dumps(params))
    try:
        json_resp = resp.json()
    except Exception:
        raise resp.text
    result = json_resp['result'][0]
    return result["docker_img_name"]


def _verify_and_format_output(data_path, workspace_id, object_id):
    """make sure the sdk indexers follow conventions, and stream the if necessary."""
    def check_datatypes(d):
        """verify that the outputs of the sdk indexer follows these rather strict conventions"""
        if isinstance(d, dict):
            for key, val in d.items():
                # not sure if we want to do recursive here or not (not for now)
                assert isinstance(key, str), "Keys returned from indexer must be strings"
                assert isinstance(val, str) or isinstance(val, int) or isinstance(val, float) or val is None or isinstance(val, bool), \
                       "Values returned from indexer must be strings, integers, floats or Nonetype"
        if isinstance(d, list):
            for val in d: check_datatypes(val)

    def format_data(d):
        sub_type = d.get('sub_type', None)
        sub_id = d.get('sub_id', None)
        if d.get('sub_type') and d.get('sub_id'):
            es_id = f"{_NAMESPACE}::{workspace_id}:{object_id}::{d.get('sub_type')}::{d.get('sub_id')}"
        else:
            es_id = f"{_NAMESPACE}::{workspace_id}:{object_id}"
        return {
            "index": index_name + ":" + indexer_version,
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
            id_ = im.id
            pulled = True
    if not pulled:
        print("Pulling %s"%image)
        id_ = _DOCKER.images.pull(image).id
    return id_


def _setup_docker_inputs(job_dir, obj_data, ws_info, obj_data_v1):
    """set up parameters for input to the sdk application"""
    data_dir = job_dir + "/data"
    os.makedirs(data_dir)
    obj_data_path    = data_dir + "/obj_data.json"
    ws_info_path     = data_dir + "/ws_info.json"
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


def index_from_sdk(sdk_image, sdk_func, sdk_version, obj_data, ws_info, obj_data_v1):
	"""Index from an sdk application"""
    workspace_id = obj_data['info'][6]
    object_id = obj_data['info'][0]

    image = _get_docker_image_name(sdk_image, module_version=sdk_version)
    id_   = _pull_docker_image(image)

    # maybe make this tempfile stuff?
    job_id = str(uuid.uuid1())
    job_dir = _SCRATCH + "/" + job_id
    os.makedirs(job_dir)

    # write inputs to files
    _setup_docker_inputs(job_dir, obj_data, ws_info, obj_data_v1)

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

    # DO SOME ERROR CHECKING HERE
    yield _verify_and_format_output(job_dir + "/output.json", workspace_id, object_id)
