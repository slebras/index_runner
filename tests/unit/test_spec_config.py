import importlib
import jsonschema
import yaml

with open('spec/ama_config.yaml') as fd:
    ama_spec = yaml.safe_load(fd)

with open('spec/config.yaml') as fd:
    standard_spec = yaml.safe_load(fd)

with open('spec/elasticsearch_modules.yaml') as fd:
    es_modules = yaml.safe_load(fd)

print('YAML successfully parsed')


def test_sdk_index_names():
    spec = standard_spec
    for (key, val) in spec['sdk_indexer_apps'].items():
        subobj_index = val.get('sub_obj_index')
        if subobj_index is not None:
            assert subobj_index in spec['aliases']
            assert subobj_index in spec['latest_versions']


def test_alias_index_names():
    for spec in (standard_spec, ama_spec):
        for (key, ls) in spec['aliases'].items():
            for idx in ls:
                assert idx in spec['mappings']


def test_type_to_indexes():
    for spec in (standard_spec, ama_spec):
        for (key, val) in spec['ws_type_to_indexes'].items():
            assert val in spec['latest_versions']
            assert val in spec['aliases']


def test_latest_version_names():
    for spec in (standard_spec, ama_spec):
        for (key, val) in spec['latest_versions'].items():
            assert val in spec['mappings']


def test_mappings():
    for spec in (standard_spec, ama_spec):
        for (key, val) in spec['mappings'].items():
            global_mappings = val.get('global_mappings', [])
            assert isinstance(global_mappings, list)
            for name in global_mappings:
                assert name in spec['global_mappings']
            assert 'properties' in val


def test_elasticsearch_module_schema():
    """
    Validate the schema for elasticsearch indexer module config.
    """
    schema = es_modules['indexers_schema']
    data = es_modules['indexers']
    jsonschema.validate(data, schema)


def test_elasticsearch_module_paths():
    """
    Validate all the file paths in the elasticsearch module config.
    """
    for mod in es_modules['indexers']:
        path = mod['source'].strip()
        mod = importlib.import_module(path)
        assert mod.main
