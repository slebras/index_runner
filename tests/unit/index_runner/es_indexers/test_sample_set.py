import json
import os
import responses

from src.utils.config import config
from src.utils.get_es_module import get_es_module

# Load test data
with open(os.path.join(os.path.dirname(__file__), 'data/sample_set.json')) as fd:
    data = json.load(fd)
(indexer, conf) = get_es_module('KBaseSets', 'SampleSet')


@responses.activate
def test_sample_set_indexer1():
    # Mock the request that checks for an existing sample
    url = config()['elasticsearch_url'] + '/search2.sample/_doc/SMP::1:1'
    responses.add(responses.GET, url, json={'found': False})
    # Mock the request against the sample service
    responses.add(responses.POST, config()['sample_service_url'], json=data['sample_service_resp1'])
    results = indexer(data['obj1'], data['ws_info1'], data['obj1'], conf)
    for (idx, result) in enumerate(list(results)):
        assert result == data['expected_result1'][idx]
