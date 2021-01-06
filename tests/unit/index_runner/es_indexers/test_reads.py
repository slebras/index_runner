import os
import json

from src.utils.get_es_module import get_es_module

# Load test data
with open(os.path.join(os.path.dirname(__file__), 'data/reads.json')) as fd:
    data = json.load(fd)
(indexer, conf) = get_es_module('KBaseFile', 'PairedEndLibrary')


def test_reads_valid1():
    """Valid reads indexing test"""
    results = indexer(data['obj1'], data['ws_info1'], data['obj1'], conf)
    for (idx, result) in enumerate(list(results)):
        assert result == data['expected_result1'][idx]
