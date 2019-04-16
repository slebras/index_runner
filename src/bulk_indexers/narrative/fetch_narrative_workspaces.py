import os
import json
import requests

from utils.get_path import get_path


_WS_URL = os.environ.get('WORKSPACE_URL', 'https://ci.kbase.us/services/ws')


def main(outpath, token):
    """
    Iterate over all workspaces and fetch all that have narratives.
    Writes workspace info of each to ws_info.json
    """
    outfd = open(outpath, 'a')
    try:
        for wsid in range(1, 42000):
            resp = requests.post(
                _WS_URL,
                data=json.dumps({
                    'method': 'administer',
                    'params': [{
                        'command': 'getWorkspaceInfo',
                        'params': {'id': wsid}
                    }]
                }),
                headers={'Authorization': token}
            ).json()
            if get_path(resp, ['result', 0, -1, 'narrative_nice_name']):
                print(f'Found narrative in workspace {wsid}')
                json.dump({'ws_info': resp['result'][0]}, outfd)
                outfd.write('\n')
    finally:
        outfd.close()
        print('..done')
