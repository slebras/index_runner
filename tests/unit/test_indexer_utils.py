from src.index_runner.es_indexers import indexer_utils


def test_merge_default_fields():
    """
    """
    pre_defaults = {
        "creator": 'user1',
        "access_group": 12345,
        "obj_name": 'obj-name',
        "shared_users": ['user1', 'user2', 'user3', 'user4'],
        "timestamp": 1234098,
        "creation_date": 1234070,
        "is_public": False,
        "version": 1,
        "obj_id": 1,
        "copied": None,
        "tags": [],
        "obj_type_version": 1,
        "obj_type_module": "KBaseSets",
        "obj_type_name": "SampleSet"
    }
    indexer = {
        'doc': {
            "save_date": 1234098,
            "sample_version": 1,
            "name": 'sample_name',
            "sample_set_ids": ["SMP:1:1"],
        }
    }
    indexer['doc'].update(pre_defaults)
    defaults = dict(pre_defaults)
    defaults['shared_users'] = [
        'user1', 'user5', 'user6', 'user7'
    ]
    defaults['access_group'] = 12000
    indexer_ret = indexer_utils.merge_default_fields(indexer, defaults)
    compare = {
        'doc': {
            'save_date': 1234098,
            'sample_version': 1,
            'name': 'sample_name',
            'sample_set_ids': ['SMP:1:1'],
            'creator': 'user1',
            'access_group': [12345, 12000],
            'obj_name': 'obj-name',
            'shared_users': ['user1', 'user2', 'user3', 'user4', 'user5', 'user6', 'user7'],
            'timestamp': 1234098,
            'creation_date': 1234070,
            'is_public': False,
            'version': 1,
            'obj_id': 1,
            'copied': None,
            'tags': [],
            'obj_type_version': 1,
            'obj_type_module': 'KBaseSets',
            'obj_type_name': 'SampleSet'
        }
    }
    assert compare == indexer_ret
