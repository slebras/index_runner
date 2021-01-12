from src.index_runner.es_indexers import indexer_utils


def lists_are_same(L1, L2):
    """compare two lists"""
    return len(L1) == len(L2) and sorted(L1) == sorted(L2)


def test_merge_scalars():
    defaults = {
        "key": 1
    }
    indexer_doc = {
        "doc": {
            "key": 2
        }
    }
    indexer_ret = indexer_utils.merge_default_fields(indexer_doc, defaults)
    assert type(indexer_ret['doc']['key']) == list
    assert lists_are_same(indexer_ret['doc']['key'], [1, 2])


def test_merge_same_scalar():
    defaults = {
        "key": "1"
    }
    indexer_doc = {
        "doc": {
            "key": "1"
        }
    }
    indexer_ret = indexer_utils.merge_default_fields(indexer_doc, defaults)
    assert indexer_ret['doc']['key'] == "1"


def test_merge_lists():
    # merge two lists
    defaults = {
        "key": [1, 2]
    }
    indexer_doc = {
        "doc": {
            "key": [1, 3]
        }
    }
    indexer_ret = indexer_utils.merge_default_fields(indexer_doc, defaults)
    assert type(indexer_ret['doc']['key']) == list
    assert lists_are_same(indexer_ret['doc']['key'], [1, 2, 3])
    # merge list into scalar
    defaults = {
        "key": 1
    }
    indexer_doc = {
        "doc": {
            "key": [1, 3]
        }
    }
    indexer_ret = indexer_utils.merge_default_fields(indexer_doc, defaults)
    assert type(indexer_ret['doc']['key']) == list
    assert lists_are_same(indexer_ret['doc']['key'], [1, 3])
    # merge scalar into list
    defaults = {
        "key": [1, 2]
    }
    indexer_doc = {
        "doc": {
            "key": 3
        }
    }
    indexer_ret = indexer_utils.merge_default_fields(indexer_doc, defaults)
    assert type(indexer_ret['doc']['key']) == list
    assert lists_are_same(indexer_ret['doc']['key'], [1, 2, 3])


def test_merge_add_new_field():
    defaults = {
        "key": "1"
    }
    indexer_doc = {
        "doc": {
            "key2": "2"
        }
    }
    indexer_ret = indexer_utils.merge_default_fields(indexer_doc, defaults)
    assert indexer_ret['doc']['key'] == "1"
    assert indexer_ret['doc']['key2'] == "2"


def test_merge_no_doc_error():
    defaults = {}
    indexer_doc = {}
    # assert error message
    try:
        _ = indexer_utils.merge_default_fields(indexer_doc, defaults)
    except ValueError as err:
        assert str(err) == "indexer return data should have 'doc' dictionary {}"
