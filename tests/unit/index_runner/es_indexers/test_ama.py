import os
import json
import tempfile
import shutil

from src.index_runner.es_indexers.annotated_metagenome_assembly import _index_ama
from src.utils.get_es_module import get_es_module

_DIR = os.path.dirname(os.path.realpath(__file__))

# Load test data
with open(os.path.join(_DIR, 'data/ama.json')) as fd:
    data = json.load(fd)
with open(os.path.join(_DIR, 'data/ama_check_against.json')) as fd:
    check_against = json.load(fd)
# Load module config
(indexer, conf) = get_es_module('KBaseMetagenomes', 'AnnotatedMetagenomeAssembly')


def test_annotated_metagenome_assembly_indexer():
    # the annotated_metagenome_assembly 'check_against' data is really big, so we keep it in an external file
    features_test_file = os.path.join(_DIR, "data", "features.json.gz")
    info = data['obj']['info']
    workspace_id = info[6]
    object_id = info[0]
    version = info[4]
    ama_index = f"WS::{workspace_id}:{object_id}"
    ver_ama_index = f"WSVER::{workspace_id}:{object_id}:{version}"

    try:
        tmp_dir = tempfile.mkdtemp()
        features_path = os.path.join(tmp_dir, "features.json.gz")
        shutil.copy(features_test_file, features_path)
        results = _index_ama(
            features_path,
            data['obj']['data'],
            ama_index,
            ver_ama_index,
            tmp_dir,
            conf
        )
        for (idx, msg_data) in enumerate(results):
            assert msg_data == check_against[idx]
    finally:
        shutil.rmtree(tmp_dir)
