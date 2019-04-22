import json
import requests


def main(inpath, outpath, token):
    """
    Fetch the UPAs of the actual narrative object for each workspace narrative
    id. Writes the upas to narrative_upas.csv
    """
    infd = open(inpath)
    outfd = open(outpath, 'a')
    try:
        for line in infd:
            j = json.loads(line)
            wsid = j['ws_info'][0]
            resp = requests.post(
                'https://ci.kbase.us/services/ws',
                data=json.dumps({
                    'method': 'administer',
                    'params': [{
                        'command': 'listObjects',
                        'params': {'ids': [wsid]}
                    }]
                }),
                headers={'Authorization': token}
            ).json()
            result = resp['result'][0]
            for obj in result:
                ws_type = obj[2]
                if ws_type == 'KBaseNarrative.Narrative-4.0':
                    j['upa'] = f"{obj[6]}/{obj[0]}/{obj[4]}"
                    json.dump(j, outfd)
                    outfd.write('\n')
    finally:
        infd.close()
        outfd.close()
