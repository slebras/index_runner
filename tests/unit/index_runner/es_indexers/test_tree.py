import os
import json

from src.utils.get_es_module import get_es_module

# Load test data
with open(os.path.join(os.path.dirname(__file__), 'data/tree.json')) as fd:
    data = json.load(fd)
(indexer, conf) = get_es_module('KBaseTrees', 'Tree')


def test_tree_indexer():
    """Valid indexing test"""
    # Mock the request against the sample service
    results = indexer(data['obj1'], data['ws_info1'], data['obj1'], conf)
    for (idx, result) in enumerate(list(results)):
        assert result == data['expected_result1'][idx]
