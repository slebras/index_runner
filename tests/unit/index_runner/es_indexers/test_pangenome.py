import os
import json

from src.utils.get_es_module import get_es_module

_DIR = os.path.dirname(__file__)

# Load test data
with open(os.path.join(_DIR, 'data/pangenome.json')) as fd:
    data = json.load(fd)
with open(os.path.join(_DIR, 'data/pangenome_check_against.json')) as fd:
    check_against = json.load(fd)
# Load module config
(indexer, conf) = get_es_module('KBaseGenomes', 'Pangenome')


def test_genome_valid1():
    """Valid indexing test"""
    results = indexer(data['obj1'], data['ws_info1'], data['obj1'], conf)
    for (idx, result) in enumerate(list(results)):
        assert result == check_against[idx]
