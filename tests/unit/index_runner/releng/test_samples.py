# import os
# import json

from src.index_runner.releng.samples import _generate_link_information
from src.index_runner.releng.samples import _hash

# _DIR = os.path.dirname(os.path.realpath(__file__))


def test_generate_sample_set_edges():
    sample = {
        'node_tree': [{
            'id': "node_id",
            'meta_controlled': {
                'biome': {"value": "ENVO:01000221"},
                'feature': {"value": "this:term"}
            }
        }],
        'id': "sample-uuid",
        'version': 1
    }
    sample_version_uuid = "sample-version-uuid"
    edges = []
    term_bank = {
        "ENVO:01000221": "ENVO_terms/ENVO:01000221_v2019-03-14",
        "this:term": "onto_coll/this:term_v2019-01-01"
    }
    _generate_link_information(sample, sample_version_uuid, edges, term_bank)
    assert len(edges) == 2
    node_id = _hash("node_id")
    for e in edges:
        assert "from" in e
        assert "to" in e
        assert e["from"] == f"samples_nodes/sample-uuid_sample-version-uuid_{node_id}"
