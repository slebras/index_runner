"""
Tests for the narrative indexer
"""
import json
import os

from src.index_runner.es_indexers.narrative import index_narrative

_DIR = os.path.dirname(os.path.realpath(__file__))


def test_basic_valid(self):
    """Test the happy case."""
    with open(os.path.join(_DIR, 'test_data', 'narrative_obj_valid.json')) as fd:
        narr_obj = json.load(fd)
    with open(os.path.join(_DIR, 'test_data', 'narrative_wsinfo_valid.json')) as fd:
        ws_info = json.load(fd)
    results = list(index_narrative(narr_obj, ws_info, {}))
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
    self.assertEqual(doc['cells'], [
        {'desc': '', 'cell_type': 'code_cell'},
        {'desc': 'Align Reads using Bowtie2 v2.3.2', 'cell_type': 'kbase_app'},
        {'desc': 'Echo test', 'cell_type': 'kbase_app'},
    ])
    assert len(doc['data_objects']) > 0
    for obj in doc['data_objects']:
        assert obj['name']
        assert obj['obj_type']
    # Static narrative fields
    assert doc['static_narrative_saved'] == '1597187531703'
    assert doc['static_narrative_ref'] == '/33192/56/'


def test_temporary_narr(self):
    """Test that temporary narratives get skipped."""
    with open(os.path.join(_DIR, 'test_data', 'narrative_obj_temporary.json')) as fd:
        narr_obj = json.load(fd)
    with open(os.path.join(_DIR, 'test_data', 'narrative_wsinfo_temporary.json')) as fd:
        ws_info = json.load(fd)
    results = list(index_narrative(narr_obj, ws_info, {}))
    assert len(results) == 0


def test_narratorial(self):
    """Test that a narratorial gets flagged as such."""
    with open(os.path.join(_DIR, 'test_data', 'narrative_obj_narratorial.json')) as fd:
        narr_obj = json.load(fd)
    with open(os.path.join(_DIR, 'test_data', 'narrative_wsinfo_narratorial.json')) as fd:
        ws_info = json.load(fd)
    results = list(index_narrative(narr_obj, ws_info, {}))
    assert len(results) == 1
    result = results[0]
    self.assertEqual(result['doc']['is_narratorial'], True)


def test_fail_no_obj_metadata(self):
    """Test that a narrative index fails without obj metadata."""
    with open(os.path.join(_DIR, 'test_data', 'narrative_obj_no_metadata.json')) as fd:
        narr_obj = json.load(fd)
    with open(os.path.join(_DIR, 'test_data', 'narrative_wsinfo_valid.json')) as fd:
        ws_info = json.load(fd)
    with self.assertRaises(RuntimeError) as ctx:
        list(index_narrative(narr_obj, ws_info, {}))
    assert 'no metadata' in str(ctx.exception)


def test_fail_no_ws_metadata(self):
    """Test that a narrative index fails without workspace metadata."""
    with open(os.path.join(_DIR, 'test_data', 'narrative_obj_valid.json')) as fd:
        narr_obj = json.load(fd)
    with open(os.path.join(_DIR, 'test_data', 'narrative_wsinfo_no_metadata.json')) as fd:
        ws_info = json.load(fd)
    with self.assertRaises(RuntimeError) as ctx:
        list(index_narrative(narr_obj, ws_info, {}))
    assert 'no metadata' in str(ctx.exception)
