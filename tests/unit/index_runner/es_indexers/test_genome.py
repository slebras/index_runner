import os
import json

from src.index_runner.es_indexers.genome import index_genome

_DIR = os.path.dirname(__file__)

# Load test data
with open(os.path.join(_DIR, 'data/genome.json')) as fd:
    data = json.load(fd)
with open(os.path.join(_DIR, 'data/genome_check_against.json')) as fd:
    check_against = json.load(fd)


def test_genome_valid1():
    """Valid indexing test"""
    results = index_genome(data['obj1'], data['ws_info1'], data['obj1'])
    for (idx, result) in enumerate(list(results)):
        assert result == check_against[idx]
