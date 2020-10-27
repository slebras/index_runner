import os
import json

from src.index_runner.es_indexers.assembly import index_assembly


# Load test data
with open(os.path.join(os.path.dirname(__file__), 'data/test_reads.json')) as fd:
    data = json.load(fd)


def test_assembly_indexer():
    """Valid indexing test"""
    # Mock the request against the sample service
    results = index_assembly(data['obj1'], data['ws_info1'], data['obj1'])
    for (idx, result) in enumerate(list(results)):
        assert result == data['expected_result1'][idx]
