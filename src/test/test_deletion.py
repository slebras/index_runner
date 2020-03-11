import unittest
import time
import src.index_runner.es_indexer as es_indexer
import src.index_runner.releng_importer as releng_importer
import src.utils.re_client as re_client
import src.test.helpers as helpers


class TestDeletion(unittest.TestCase):
    """
    Test deletion actions for both elasticsearch and arangodb
    """

    def test_es_deletion(self):
        wsid = _NEW_MSG['wsid']
        objid = _NEW_MSG['objid']
        es_id = f"WS::{wsid}:{objid}"  # type: ignore
        print(f"WS::33192:23 vs {es_id}")
        es_indexer.run_indexer(_OBJ, _WS_INFO, _NEW_MSG)
        es_doc = helpers.get_es_doc_blocking(es_id)
        self.assertTrue(es_doc)
        es_indexer.delete_obj(_DEL_MSG)
        start = time.time()
        while True:
            es_doc = helpers.get_es_doc(es_id)
            if not es_doc:
                return
            if time.time() > (start + 120):
                raise RuntimeError("Doc never deleted")
            print('Waiting for es doc to be deleted..')
            time.sleep(3)

    def test_arango_deletion(self):
        wsid = _NEW_MSG['wsid']
        objid = _NEW_MSG['objid']
        re_key = f"{wsid}:{objid}"
        releng_importer.run_importer(_OBJ, _WS_INFO, _NEW_MSG)
        re_doc = re_client.get_doc('ws_object', re_key)
        self.assertTrue(re_doc)
        releng_importer.delete_obj(_DEL_MSG)
        re_doc = re_client.get_doc('ws_object', re_key)['results'][0]
        print('re_doc', re_doc)
        self.assertTrue(re_doc['deleted'])


# -- Test data

_OBJ = {
    "data": {
        "assembly_id": "GCF_000762265.1_assembly",
        "base_counts": {"A": 1, "C": 2, "G": 3, "T": 4},
        "contigs": {
            "NZ_CP006933.1": {
                "contig_id": "NZ_CP006933.1",
                "description": "Methanobacterium formicicum strain BRM9, complete genome",
                "gc_content": 0.4134,
                "is_circ": 1,
                "length": 2449987,
                "md5": "17e25ea2632697d4ebd8d19de0b1ecee",
                "name": "NZ_CP006933.1"
            }
        },
        "dna_size": 2449987,
        "fasta_handle_info": {
            "handle": {
                "file_name": "GCF_000762265.1_assembly.fasta",
                "hid": "KBH_480528",
                "id": "8a94e19b-68c7-4c86-8391-5c10588b1c0c",
                "remote_md5": "1772808484d8f0a50518de86c9bca280",
                "type": "shock",
                "url": "https://ci.kbase.us/services/shock-api"
            },
            "node_file_name": "GCF_000762265.1_assembly.fasta",
            "shock_id": "8a94e19b-68c7-4c86-8391-5c10588b1c0c",
            "size": 2490893
        },
        "fasta_handle_ref": "KBH_480528",
        "gc_content": 0.4134,
        "md5": "3113b0e9d69510c2f02d223d053b14d5",
        "num_contigs": 1,
        "type": "isolate"
    },
    "info": [
        23,
        "GCF_000762265.1_assembly",
        "KBaseGenomeAnnotations.Assembly-6.0",
        "2020-01-09T19:04:59+0000",
        2,
        "jayrbolton",
        33192,
        "jayrbolton:narrative_1528306445083",
        "f9dbc6acf1176660b67bfb43ddcc79f5",
        857,
        {
            "GC content": "0.4134",
            "Size": "2449987",
            "N Contigs": "1",
            "MD5": "3113b0e9d69510c2f02d223d053b14d5"
        }
    ],
    "path": ["33192/23/2"],
    "provenance": [],
    "creator": "jayrbolton",
    "orig_wsid": 45320,
    "created": "2019-12-10T21:50:08+0000",
    "epoch": 1576014608185,
    "refs": [],
    "copied": "45320/13/1",
    "copy_source_inaccessible": 0,
    "extracted_ids": {"handle": ["KBH_480528"]}
}

_WS_INFO = [
      33192,
      "jayrbolton:narrative_1528306445083",
      "jayrbolton",
      "2020-01-09T19:04:59+0000",
      23,
      "n",
      "r",
      "unlocked",
      {"narrative_nice_name": "Test fiesta", "is_temporary": "false", "narrative": "1", "data_palette_id": "2"}
]

_NEW_MSG = {
   "wsid": 33192,
   "ver": 2,
   "perm": None,
   "evtype": "NEW_VERSION",
   "objid": 23,
   "time": 1578439639664,
   "objtype": "KBaseGenomeAnnotations.Assembly-6.0",
   "permusers": [],
   "user": "jayrbolton"
}

_DEL_MSG = {
   "wsid": 33192,
   "ver": 2,
   "perm": None,
   "evtype": 'OBJECT_DELETE_STATE_CHANGE',
   "objid": 23,
   "time": 1578439639664,
   "objtype": "KBaseGenomeAnnotations.Assembly-6.0",
   "permusers": [],
   "user": "jayrbolton"
}
