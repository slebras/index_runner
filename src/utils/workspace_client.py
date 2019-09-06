"""
Make API requests to the kbase workspace JSON RPC server.
"""
import json
import requests

from src.utils.config import config


def download_info(wsid, objid, ver=None):
    """
    Download object info from the workspace.
    """
    ref = '/'.join([str(n) for n in [wsid, objid, ver] if n])
    result = admin_req('getObjects', {
        'objects': [{'ref': ref}],
        'no_data': 1
    })
    return result['data'][0]


def req(method, params):
    """
    Make a JSON RPC request to the workspace server.
    KIDL docs: https://kbase.us/services/ws/docs/Workspace.html
    """
    payload = {'version': '1.1', 'method': method, 'params': [params]}
    return _post_req(payload)


def admin_req(method, params):
    """
    Make a JSON RPC administration request (method is wrapped inside the "administer" method).
    Docs for this interface: https://kbase.us/services/ws/docs/administrationinterface.html
    """
    payload = {
        'version': '1.1',
        'method': 'Workspace.administer',
        'params': [{'command': method, 'params': params}]
    }
    return _post_req(payload)


def _post_req(payload):
    """Make a post request to the workspace server and process the response."""
    headers = {'Authorization': config()['ws_token']}
    resp = requests.post(config()['workspace_url'], data=json.dumps(payload), headers=headers)
    if not resp.ok:
        raise RuntimeError('Error response from workspace:\n%s' % resp.text)
    resp_json = resp.json()
    if 'error' in resp_json:
        raise RuntimeError('Error response from workspace:\n%s' % resp.text)
    elif 'result' not in resp_json or not len(resp_json['result']):
        raise RuntimeError('Invalid workspace response:\n%s' % resp.text)
    return resp_json['result'][0]
