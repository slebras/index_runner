"""
Tests for the narrative indexer
"""
import json
import os
import pytest
import responses

from src.utils.config import config
from src.utils.get_es_module import get_es_module

_DIR = os.path.dirname(os.path.realpath(__file__))

# Load test data
with open(os.path.join(_DIR, 'data', 'narrative.json')) as fd:
    data = json.load(fd)
# Load module config
(indexer, conf) = get_es_module('KBaseNarrative', 'Narrative')


def test_basic_valid():
    """Test the happy case."""
    narr_obj = data['obj_valid']
    ws_info = data['wsinfo_valid']
    results = list(indexer(narr_obj, ws_info, {}, conf))
    assert len(results) == 1
    result = results[0]
    assert result['_action'] == 'index'
    assert result['index'] == 'narrative_2'
    assert result['id'] == 'WS::33192:1'
    doc = result['doc']
    assert doc['narrative_title'] == 'Test fiesta'
    assert not doc['is_narratorial']
    assert doc['creator'] == 'jayrbolton'
    assert doc['owner'] == 'jayrbolton'
    assert doc['modified_at'] > 0
    doc['cells'] == [
        {'desc': '', 'cell_type': 'code_cell'},
        {'desc': 'Align Reads using Bowtie2 v2.3.2', 'cell_type': 'kbase_app'},
        {'desc': 'Echo test', 'cell_type': 'kbase_app'},
    ]
    assert len(doc['data_objects']) > 0
    for obj in doc['data_objects']:
        assert obj['name']
        assert obj['obj_type']
    # Static narrative fields
    assert doc['static_narrative_saved'] == '1597187531703'
    assert doc['static_narrative_ref'] == '/33192/56/'


def test_temporary_narr():
    """Test that temporary narratives get skipped."""
    narr_obj = data['obj_temporary']
    ws_info = data['wsinfo_temporary']
    results = list(indexer(narr_obj, ws_info, {}, conf))
    assert len(results) == 0


@responses.activate
def test_narratorial():
    """Test that a narratorial gets flagged as such."""
    mock_resp = {
        "result": [[]]
    }
    responses.add(responses.POST, config()['workspace_url'], json=mock_resp)
    narr_obj = data['obj_narratorial']
    ws_info = data['wsinfo_narratorial']
    results = list(indexer(narr_obj, ws_info, {}, conf))
    assert len(results) == 1
    result = results[0]
    assert result['doc']['is_narratorial']


def test_fail_no_obj_metadata():
    """Test that a narrative index fails without obj metadata."""
    narr_obj = data["obj_no_metadata"]
    ws_info = data["wsinfo_valid"]
    with pytest.raises(RuntimeError) as ctx:
        list(indexer(narr_obj, ws_info, {}, conf))
    assert 'no metadata' in str(ctx.value)


def test_fail_no_ws_metadata():
    """Test that a narrative index fails without workspace metadata."""
    narr_obj = data["obj_valid"]
    ws_info = data["wsinfo_no_metadata"]
    with pytest.raises(RuntimeError) as ctx:
        list(indexer(narr_obj, ws_info, {}, conf))
    assert 'no metadata' in str(ctx.value)
