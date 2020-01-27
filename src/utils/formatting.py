"""
Data formatting utilities.
"""
import time


def ts_to_epoch(ts):
    """Convert a string timestamp into a ms epoch integer."""
    return int(time.mktime(time.strptime(ts, "%Y-%m-%dT%H:%M:%S%z"))) * 1000


def _get_module_ver_hash(prov):
    """Get module commit hash, falling back to semantic version, and finally 'UNKNOWN'"""
    ver = None
    subacts = prov[0].get('subactions')
    if subacts:
        ver = subacts[0].get('commit')
    if not ver:
        ver = prov[0].get('service_ver', 'UNKNOWN')
    return ver


def get_method_key_from_prov(prov):
    """
    From provenance data, such as:
     {
          "service": "narrative",
          "service_ver": "3.10.0",
          "input_ws_objects": [],
          "resolved_ws_objects": [],
          "external_data": [],
          "subactions": [],
          "custom": {},
          "description": "Saved by KBase Narrative Interface"
      }
    Return a string for the ws_method_version _key field
    in the format:  "service_name:commit_hash:method_name"
    """
    serv = prov[0]['service']
    commit = _get_module_ver_hash(prov)
    meth = prov[0].get('method', 'UNKNOWN')
    return f"{serv}:{commit}:{meth}"


def get_module_key_from_prov(prov):
    """
    From provenance data, return a string for the ws_module_Version _key field
    in the format: "module_name:commit_hash"
    """
    serv = prov[0]['service']
    commit = _get_module_ver_hash(prov)
    return f"{serv}:{commit}"
