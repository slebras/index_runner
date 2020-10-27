import os
import json

from src.index_runner.es_indexers.reads import index_reads

# Load test data
with open(os.path.join(os.path.dirname(__file__), 'data/reads.json')) as fd:
    data = json.load(fd)


def test_reads_valid1():
    """Valid reads indexing test"""
    # Mock the request against the sample service
    results = index_reads(data['obj1'], data['ws_info1'], data['obj1'])
    print('results', enumerate(list(results)))
    for (idx, result) in enumerate(list(results)):
        assert result == data['expected_result1'][idx]
