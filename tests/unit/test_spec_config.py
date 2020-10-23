import yaml

with open('spec/config.yaml') as fd:
    spec = yaml.safe_load(fd)
    print('YAML successfully parsed')


def test_sdk_index_names():
    for (key, val) in spec['sdk_indexer_apps'].items():
        subobj_index = val.get('sub_obj_index')
        if subobj_index is not None:
            assert subobj_index in spec['aliases']
            assert subobj_index in spec['latest_versions']


def test_alias_index_names():
    for (key, ls) in spec['aliases'].items():
        for idx in ls:
            assert idx in spec['mappings']


def test_type_to_indexes():
    for (key, val) in spec['ws_type_to_indexes'].items():
        assert val in spec['latest_versions']
        assert val in spec['aliases']


def test_latest_version_names():
    for (key, val) in spec['latest_versions'].items():
        assert val in spec['mappings']


def test_mappings():
    for (key, val) in spec['mappings'].items():
        global_mappings = val.get('global_mappings', [])
        assert isinstance(global_mappings, list)
        for name in global_mappings:
            assert name in spec['global_mappings']
        assert 'properties' in val
