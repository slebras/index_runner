import importlib
import yaml

with open('spec/elasticsearch_modules.yaml') as fd:
    es_modules = yaml.safe_load(fd)


def get_es_module(type_module, type_name, type_version=None):
    """
    Fetch the Elasticsearch indexer module and configuration for a given WS type
    """
    for entry in es_modules['elasticsearch']:
        module_match = entry['module'] == type_module
        name_match = entry['type'] == type_name
        ver_match = type_version is None or entry['verison'] == type_version
        if module_match and name_match and ver_match:
            # Dynamically load the module
            path = entry['source']
            mod = importlib.import_module(path)
            return (mod.main, entry['config'])
    raise ModuleNotFound()


class ModuleNotFound(Exception):
    pass
