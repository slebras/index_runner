import os
import json
import requests


def main():
    """Index all un-indexed objects in all workspaces for an environment."""
    out_path = os.environ['OUT_PATH']
    max_ws_id = int(os.environ['MAX_WS_ID'])
    min_ws_id = int(os.environ['MIN_WS_ID'])
    token = os.environ['AUTH_TOKEN']
    with open(out_path, 'w') as out_fd:
        out_fd.write("workspace_id\tobject_id\n")
        for ws_id in range(min_ws_id, max_ws_id):
            resp = requests.post(
                'https://ci.kbase.us/services/ws',
                data=json.dumps({
                    'method': 'administer',
                    'params': [{'command': 'listObjects', 'params': {'ids': [ws_id]}}]
                }),
                headers={'Authorization': token}
            ).json()
            result = resp['result'][0]
            for obj in result:
                ws_type = obj[2]
                if ws_type == 'KBaseNarrative.Narrative-4.0':
                    print(f"{obj[6]}/{obj[0]}")
                    out_fd.write(f"{obj[6]}\t{obj[0]}\n")


if __name__ == '__main__':
    main()
