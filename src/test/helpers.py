

def assert_subset(testCls, subset, _dict):
    """
    Replacement for the deprecated `assertDictContainsSubset` method.
    Assert that all keys and values in `subset` are present in `_dict`
    """
    for (key, val) in subset.items():
        testCls.assertEqual(subset.get(key), _dict.get(key))
