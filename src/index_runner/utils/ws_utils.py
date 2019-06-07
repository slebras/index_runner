from .config import get_config
from kbase_workspace_client import WorkspaceClient

_CONFIG = get_config()
_WS_CLIENT = WorkspaceClient(url=_CONFIG['workspace_url'], token=_CONFIG['ws_token'])


def get_obj_ids_from_ws(wsid):
    results = _WS_CLIENT.admin_req("listObjects", {"ids": [wsid]})
    return [obj[0] for obj in results]
