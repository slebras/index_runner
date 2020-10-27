import os
import json
import tempfile
import shutil

_DIR = os.path.dirname(os.path.realpath(__file__))


def test_annotated_metagenome_assembly_indexer():
    # the annotated_meganome_assembly 'check_against' data is really big, so we keep it in an external file
    event_data_str = "annotated_metagenome_assembly_save"
    with open(os.path.join(_DIR, 'test_data/annotated_metagenome_assembly_check_against.json')) as fd:
        check_against = json.load(fd)
    print(f'Testing {event_data_str} indexer...')
    json_data_path = f"{event_data_str}_{event_data['wsid']}_{event_data['objid']}.json"
    with open(os.path.join(_DIR, 'test_data', json_data_path)) as fd:
        test_data = json.load(fd)

    features_test_file = os.path.join(_DIR, "test_data", "features.json.gz")
    info = test_data['obj']['info']
    workspace_id = info[6]
    object_id = info[0]
    version = info[4]
    ama_index = f"WS::{workspace_id}:{object_id}"
    ver_ama_index = f"WSVER::{workspace_id}:{object_id}:{version}"

    try:
        tmp_dir = tempfile.mkdtemp()
        features_test_file_copy_path = os.path.join(tmp_dir, "features.json.gz")
        shutil.copy(features_test_file, features_test_file_copy_path)
        for (idx, msg_data) in enumerate(_index_ama(features_test_file_copy_path, test_data['obj']['data'],
                                                    ama_index, ver_ama_index, tmp_dir)):
            assert msg_data == check_against[idx]
    finally:
        shutil.rmtree(tmp_dir)

