from kbase_workspace_client import WorkspaceClient
from src.utils.config import get_config

_CONFIG = get_config()
_WS_CLIENT = WorkspaceClient(url=_CONFIG['workspace_url'], token=_CONFIG['ws_token'])


def get_type_pieces(type_str):
    """
    Given a full type string, returns (module, name, ver)
     - Given "KBaseNarrative.Narrative-4.0"
     - Returns ("KBaseNarrative", "Narrative", "4.0")
    """
    (full_name, type_version) = type_str.split('-')
    (type_module, type_name) = full_name.split('.')
    return (type_module, type_name, type_version)
