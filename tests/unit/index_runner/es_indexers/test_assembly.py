import os
import json

from src.utils.get_es_module import get_es_module

# Load test data
with open(os.path.join(os.path.dirname(__file__), 'data/assembly.json')) as fd:
    data = json.load(fd)
# Load module config
(indexer, conf) = get_es_module('KBaseGenomeAnnotations', 'Assembly')


def test_assembly_indexer():
    """Valid indexing test"""
    # Mock the request against the sample service
    results = indexer(data['obj1'], data['ws_info1'], data['obj1'], conf)
    for (idx, result) in enumerate(list(results)):
        assert result == data['expected_result1'][idx]
