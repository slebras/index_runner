

def get_pieces(type_str):
    """
    Given a full type string, returns (module, name, ver)
     - Given "KBaseNarrative.Narrative-4.0"
     - Returns ("KBaseNarrative", "Narrative", "4.0")
    """
    (type_module_name, type_version) = type_str.split('-')
    (type_module, type_name) = type_module_name.split('.')
    return (type_module_name, type_name, type_version)
