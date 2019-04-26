import json
import requests

from utils.get_path import get_path


def main(inpath, outpath, token):
    """
    Fetch the object data for each narrative object by the full upa
    (ws_id/obj_id/ver).
    """
    infd = open(inpath)
    outfd = open(outpath, 'a')
    try:
        for line in infd:
            j = json.loads(line)
            upa = j['upa']
            result = requests.post(
                os.environ.get('WORKSPACE_URL', 'https://ci.kbase.us/services/ws'),
                data=json.dumps({
                    'method': 'administer',
                    'params': [{
                        'command': 'getObjects',
                        'params': {'objects': [{'ref': upa}]}
                    }]
                }),
                headers={'Authorization': token}
            ).json()
            result = get_path(result, ['result', 0])
            if result:
                j['full_data'] = result
                json.dump(j, outfd)
                outfd.write('\n')
    finally:
        infd.close()
        outfd.close()
