import json
import requests
from src.utils.config import config


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
