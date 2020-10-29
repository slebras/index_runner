import os
import json

from src.index_runner.es_indexers.tree import index_tree


# Load test data
with open(os.path.join(os.path.dirname(__file__), 'data/tree.json')) as fd:
    data = json.load(fd)


def test_tree_indexer():
    """Valid indexing test"""
    # Mock the request against the sample service
    results = index_tree(data['obj1'], data['ws_info1'], data['obj1'])
    for (idx, result) in enumerate(list(results)):
        assert result == data['expected_result1'][idx]
