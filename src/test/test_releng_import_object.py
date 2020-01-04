"""
Test importing objects into the RE directly, without going through the higher layers of code.
"""

from src.index_runner.releng.import_obj import import_object
from src.utils.service_utils import wait_for_dependencies
import unittest
import datetime
from src.utils.re_client import save, get_doc, get_edge, clear_collection, get_all_documents
from src.utils import re_client
from src.test.helpers import assert_subset

# TODO TEST more tests. Just tests very basic happy path for now


def clear_collections():
    for c in ['ws_object', 'ws_object_version', 'ws_object_hash',
              'ws_version_of', 'ws_workspace', 'ws_workspace_contains_obj',
              'ws_obj_instance_of_type', 'ws_owner_of',
              'ws_obj_version_has_taxon', 'ws_genome_features',
              'ws_genome_has_feature', 'ncbi_taxon', 'ws_copied_from',
              'ws_method_version', 'ws_obj_created_with_method',
              'ws_module_version', 'ws_obj_created_with_module',
              'ws_type_version', 'ws_type', 'ws_type_module', 'ws_refers_to',
              'ws_prov_descendant_of']:
        clear_collection(c)


class TestRelEngImportObject(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        wait_for_dependencies(elasticsearch=False)

    def test_genome(self):
        """
        Test importing a genome into the RE, including creating a object -> ncbi_taxon edge.
        """
        clear_collections()
        # stick a taxon node into the RE so search works.
        # TODO remove when we switch to pulling the taxon ID directly.
        save('ncbi_taxon', [{
                "_key": "562_2018-11-01",
                "id": "562",
                "scientific_name": "Bacillus subtilis",
                "rank": "species",
                "aliases": [],  # dumped the aliases
                "ncbi_taxon_id": 562,
                "gencode": 11,
                "first_version": "2018-11-01",
                "last_version": "2019-08-01",
                "created": 0,
                "expired": 9007199254740991,
                "release_created": 0,
                "release_expired": 9007199254740991,
                "strain": False
            }])

        # add some go terms and merge edges
        # feature 1 -> go 1 will be a standard lookup
        # feature 1 -> go 2 will already exist and so should be a noop
        # feature 1 -> go 3 will go through 2 merge edges to resolve
        # feature 1 -> go 100 will not resolve
        # feature 2 has no GO annotations
        # omit the first/last version and release created/expired fields, not necessary

        save('GO_terms', [
            # check that the query only finds extant nodes
            add_go_fields(
                {'_key': 'GO:1_v1',
                 'id': 'GO:1',
                 'name': 'X-gene',
                 'created': 0,
                 'expired': 6,
                 }),
            add_go_fields(
                {'_key': 'GO:1_v2',
                 'id': 'GO:1',
                 'name': 'prof-X-gene',
                 'created': 7,
                 'expired': re_client.MAX_ADB_INTEGER,
                 }),
            add_go_fields(
                {'_key': 'GO:2_v1',
                 'id': 'GO:2',
                 'name': 'IPR015239',
                 'created': 0,
                 'expired': re_client.MAX_ADB_INTEGER,
                 }),
            # check that the query only finds extant nodes
            add_go_fields(
                {'_key': 'GO:3_v1',
                 'id': 'GO:3',
                 'name': 'thingy',
                 'created': 0,
                 'expired': 99,
                 }),
            add_go_fields(
                {'_key': 'GO:5_v1',
                 'id': 'GO:5',
                 'name': 'GDF-8',
                 'created': 0,
                 'expired': re_client.MAX_ADB_INTEGER,
                 }),
        ])
        save('GO_merges', [
            # check that only the newest edge is considered
            {'_key': 'GO:3::GO:42::replaced_by_v1',
             'id': 'GO:3::GO:42::replaced_by',
             'type': 'replaced_by',
             'from': 'GO:3',
             'to': 'GO:42',
             '_from': 'GO_terms/GO:3_v1',
             '_to': 'GO_terms/GO:42_v1',
             'created': 0,
             'expired': 99,
             },
            {'_key': 'GO:3::GO:4::replaced_by_v1',
             'id': 'GO:3::GO:4::replaced_by',
             'type': 'replaced_by',
             'from': 'GO:3',
             'to': 'GO:4',
             '_from': 'GO_terms/GO:3_v1',
             '_to': 'GO_terms/GO:4_v1',
             'created': 100,
             'expired': re_client.MAX_ADB_INTEGER,
             },
            {'_key': 'GO:4::GO:5::replaced_by_v1',
             'id': 'GO:4::GO:5::replaced_by',
             'type': 'replaced_by',
             'from': 'GO:4',
             'to': 'GO:5',
             '_from': 'GO_terms/GO:4_v1',
             '_to': 'GO_terms/GO:5_v1',
             'created': 0,
             'expired': re_client.MAX_ADB_INTEGER,
             },
        ], display_errors=True)

        # add an edge that already exists and so should not be overwritten
        save('ws_feature_has_GO_annotation', [
            {'_key': '6:7:8_id1-_o::GO:2_v1::kbase_RE_indexer',
             '_from': 'ws_genome_features/6:7:8_id1-_o',
             '_to': 'GO_terms/GO:2_v1',
             'source': 'kbase_RE_indexer',
             'expired': re_client.MAX_ADB_INTEGER,
             'created': 678,
             }
        ])
        (type_module, type_name, maj_ver, min_ver) = ("KBaseGenomes", "Genome", 15, 1)
        obj_type = f"{type_module}.{type_name}-{maj_ver}.{min_ver}"
        # trigger the import
        wsid = 6
        import_object({
            "info": [
                7,
                "my_genome",
                obj_type,
                "2016-10-05T17:11:32+0000",
                8,
                "someuser",
                wsid,
                "godilovebacillus",
                "31b40bb1004929f69cd4acfe247ea46d",
                351,
                {}
            ]
        })
        # check results
        obj_doc = get_re_doc('ws_object', '6:7')
        self.assertEqual(obj_doc['workspace_id'], 6)
        self.assertEqual(obj_doc['object_id'], 7)
        self.assertEqual(obj_doc['deleted'], False)
        hsh = "31b40bb1004929f69cd4acfe247ea46d"
        # Check for ws_object_hash
        hash_doc = get_re_doc('ws_object_hash', hsh)
        self.assertEqual(hash_doc['type'], 'MD5')
        # Check for ws_object_version
        ver_doc = get_re_doc('ws_object_version', '6:7:8')
        self.assertEqual(ver_doc['workspace_id'], 6)
        self.assertEqual(ver_doc['object_id'], 7)
        self.assertEqual(ver_doc['version'], 8)
        self.assertEqual(ver_doc['name'], "my_genome")
        self.assertEqual(ver_doc['hash'], "31b40bb1004929f69cd4acfe247ea46d")
        self.assertEqual(ver_doc['size'], 351)
        self.assertEqual(ver_doc['epoch'], 1475687492000)
        self.assertEqual(ver_doc['deleted'], False)
        # TODO Check for ws_copied_from
        # copy_edge = _wait_for_re_edge(
        #     'ws_copied_from',  # collection
        #     'ws_object_version/41347:5:1',  # from
        #     'ws_object_version/1:2:3'  # to
        # )
        # self.assertTrue(copy_edge)
        # Check for ws_version_of
        ver_edge = get_re_edge(
            'ws_version_of',  # collection
            'ws_object_version/6:7:8',  # from
            'ws_object/6:7'  # to
        )
        self.assertTrue(ver_edge)
        # Check for ws_workspace_contains_obj
        contains_edge = get_re_edge(
            'ws_workspace_contains_obj',  # collection
            'ws_workspace/6',  # from
            'ws_object/6:7'  # to
        )
        self.assertTrue(contains_edge)
        # TODO Check for ws_obj_created_with_method edge
        # created_with_edge = _wait_for_re_edge(
        #     'ws_obj_created_with_method',  # collection
        #     'ws_object_version/41347:5:1',  # from
        #     'ws_method_version/narrative:3.10.0:UNKNOWN'  # to
        # )
        # self.assertEqual(created_with_edge['method_params'], None)
        # TODO Check for ws_obj_created_with_module edge
        # module_edge = _wait_for_re_edge(
        #     'ws_obj_created_with_module',  # collection
        #     'ws_object_version/41347:5:1',  # from
        #     'ws_module_version/narrative:3.10.0'  # to
        # )
        # self.assertTrue(module_edge)
        # Check for ws_obj_instance_of_type
        type_edge = get_re_edge(
            'ws_obj_instance_of_type',  # collection
            'ws_object_version/6:7:8',  # from
            'ws_type_version/KBaseGenomes.Genome-15.1'  # to
        )
        # Check for the workspace vertex
        ws_metadata = {'cell_count': '1', 'data_palette_id': '2',
                       'is_temporary': 'false', 'narrative': '1',
                       'narrative_nice_name': 'narr6', 'searchtags':
                       'narrative'}
        assert_subset(self, {
            '_key': str(wsid),
            'narr_name': 'narr' + str(wsid),
            'owner': 'username',
            'lock_status': 'unlocked',
            'name': 'username:narrative_' + str(wsid),
            'is_public': True,
            'is_deleted': False,
            'metadata': ws_metadata,
        }, get_re_doc('ws_workspace', str(wsid)))
        # Check for type vertices
        assert_subset(self, {
            '_key': 'KBaseGenomes.Genome-15.1',
            'module_name': 'KBaseGenomes',
            'type_name': 'Genome',
            'maj_ver': 15,
            'min_ver': 1
        }, get_re_doc('ws_type_version', obj_type))
        assert_subset(self, {
            '_key': 'KBaseGenomes.Genome',
            'module_name': 'KBaseGenomes',
            'type_name': 'Genome'
        }, get_re_doc('ws_type', f"{type_module}.{type_name}"))
        assert_subset(self, {
            '_key': 'KBaseGenomes'
        }, get_re_doc('ws_type_module', type_module))
        self.assertTrue(type_edge)
        # Check for the ws_owner_of edge
        owner_edge = get_re_edge(
            'ws_owner_of',  # collection
            'ws_user/someuser',  # from
            'ws_object_version/6:7:8',  # to
        )
        self.assertTrue(owner_edge)
        # TODO Check for the ws_refers_to edges
        # referral_edge1 = get_edge(
        #     'ws_refers_to',  # collection
        #     'ws_object_version/41347:5:1',  # from
        #     'ws_object_version/1:1:1',  # to
        # )
        # self.assertTrue(referral_edge1)
        # referral_edge2 = _wait_for_re_edge(
        #     'ws_refers_to',  # collection
        #     'ws_object_version/41347:5:1',  # from
        #     'ws_object_version/2:2:2',  # to
        # )
        # self.assertTrue(referral_edge2)
        # TODO Check for the ws_prov_descendant_of edges
        # prov_edge1 = _wait_for_re_edge(
        #     'ws_prov_descendant_of',  # collection
        #     'ws_object_version/41347:5:1',  # from
        #     'ws_object_version/1:1:1',  # to
        # )
        # self.assertTrue(prov_edge1)
        # prov_edge2 = _wait_for_re_edge(
        #     'ws_prov_descendant_of',  # collection
        #     'ws_object_version/41347:5:1',  # from
        #     'ws_object_version/2:2:2',  # to
        # )
        # self.assertTrue(prov_edge2)

        taxon_edge = get_re_edge(
            'ws_obj_version_has_taxon',
            'ws_object_version/6:7:8',
            'ncbi_taxon/562_2018-11-01')

        assert_subset(self, {
            '_from': 'ws_object_version/6:7:8',
            '_to': 'ncbi_taxon/562_2018-11-01',
            'assigned_by': '_system'
        }, taxon_edge)

        f1 = get_re_doc('ws_genome_features', '6:7:8_id1-_o')
        del f1['updated_at']
        self.assertEqual(f1, {
            '_key': '6:7:8_id1-_o',
            '_id': 'ws_genome_features/6:7:8_id1-_o',
            'workspace_id': 6,
            'object_id': 7,
            'version': 8,
            'feature_id': 'id1-|o'})

        f2 = get_re_doc('ws_genome_features', '6:7:8_id2')
        del f2['updated_at']
        self.assertEqual(f2, {
            '_key': '6:7:8_id2',
            '_id': 'ws_genome_features/6:7:8_id2',
            'workspace_id': 6,
            'object_id': 7,
            'version': 8,
            'feature_id': 'id2'})

        e1 = get_re_edge(
            'ws_genome_has_feature',
            'ws_object_version/6:7:8',
            'ws_genome_features/6:7:8_id1-_o',
            False)
        del e1['updated_at']
        self.assertEqual(e1, {
            '_key': '6:7:8_id1-_o',
            '_id': 'ws_genome_has_feature/6:7:8_id1-_o',
            '_from': 'ws_object_version/6:7:8',
            '_to': 'ws_genome_features/6:7:8_id1-_o',
        })
        e2 = get_re_edge(
            'ws_genome_has_feature',
            'ws_object_version/6:7:8',
            'ws_genome_features/6:7:8_id2',
            False)
        del e2['updated_at']
        self.assertEqual(e2, {
            '_key': '6:7:8_id2',
            '_id': 'ws_genome_has_feature/6:7:8_id2',
            '_from': 'ws_object_version/6:7:8',
            '_to': 'ws_genome_features/6:7:8_id2',
        })

        f_to_GO_edges = sorted(get_re_docs('ws_feature_has_GO_annotation'), key=lambda e: e['_to'])
        now = int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp()) * 1000
        for e in f_to_GO_edges:
            created = e['created']
            del e['created']
            del e['_rev']
            del e['updated_at']
            if e['_to'] != 'GO_terms/GO:2_v1':
                # check time is close to now
                self.assertLess(created, now + 2000)
                self.assertGreater(created, now - 5000)
            else:
                # check time wasn't overwritten
                self.assertEqual(created, 678)

        expected = [
            {'_key': '6:7:8_id1-_o::GO:1_v2::kbase_RE_indexer',
             '_id': 'ws_feature_has_GO_annotation/6:7:8_id1-_o::GO:1_v2::kbase_RE_indexer',
             '_from': 'ws_genome_features/6:7:8_id1-_o',
             '_to': 'GO_terms/GO:1_v2',
             'source': 'kbase_RE_indexer',
             'expired': 9007199254740991,
             },
            {'_key': '6:7:8_id1-_o::GO:2_v1::kbase_RE_indexer',
             '_id': 'ws_feature_has_GO_annotation/6:7:8_id1-_o::GO:2_v1::kbase_RE_indexer',
             '_from': 'ws_genome_features/6:7:8_id1-_o',
             '_to': 'GO_terms/GO:2_v1',
             'source': 'kbase_RE_indexer',
             'expired': 9007199254740991,
             },
            {'_key': '6:7:8_id1-_o::GO:5_v1::kbase_RE_indexer',
             '_id': 'ws_feature_has_GO_annotation/6:7:8_id1-_o::GO:5_v1::kbase_RE_indexer',
             '_from': 'ws_genome_features/6:7:8_id1-_o',
             '_to': 'GO_terms/GO:5_v1',
             'source': 'kbase_RE_indexer',
             'expired': 9007199254740991,
             }
        ]
        self.assertEqual(f_to_GO_edges, expected)

    def test_genome_no_feature_key(self):
        """
        Test importing a genome without a feature key or tax lineage into the RE.
        """
        self._genome_no_features(8, "my_genome2")

    def test_genome_no_features_key(self):
        """
        Test importing a genome with an empty features array and no tax lineage into the RE.
        """
        self._genome_no_features(9, "my_genome3")

    def _genome_no_features(self, objid, objname):
        clear_collections()

        # trigger the import
        import_object({
            "info": [
                objid,
                objname,
                "KBaseGenomes.Genome-15.1",
                "2016-10-05T17:11:32+0000",
                1,
                "someuser",
                6,
                "godilovebacillus",
                "31b40bb1004929f69cd4acfe247ea46d",
                351,
                {}
            ]
            })

        # only check one collection, since we already checked this stuff in previous tests
        # Check for ws_object_version
        ver_doc = get_re_doc('ws_object_version', f'6:{objid}:1')
        self.assertEqual(ver_doc['workspace_id'], 6)
        self.assertEqual(ver_doc['object_id'], objid)
        self.assertEqual(ver_doc['version'], 1)
        self.assertEqual(ver_doc['name'], objname)
        self.assertEqual(ver_doc['hash'], "31b40bb1004929f69cd4acfe247ea46d")
        self.assertEqual(ver_doc['size'], 351)
        self.assertEqual(ver_doc['epoch'], 1475687492000)
        self.assertEqual(ver_doc['deleted'], False)

        # check no taxon or feature data added
        self.assertEqual(get_re_docs('ws_obj_version_has_taxon'), [])
        self.assertEqual(get_re_docs('ws_genome_features'), [])
        self.assertEqual(get_re_docs('ws_genome_has_feature'), [])


def get_re_doc(collection, key):
    d = get_doc(collection, key)
    if len(d['results']) > 0:
        r = d['results'][0]
        del r['_rev']
        return r
    return None


def get_re_docs(collection):
    return get_all_documents(collection)['results']


def get_re_edge(collection, from_, to, del_key=True):
    d = get_edge(collection, from_, to)
    if len(d['results']) > 0:
        r = d['results'][0]
        if del_key:
            del r['_key']
            del r['_id']
        del r['_rev']
        return r
    return None


def add_go_fields(term):
    """
    Adds empty required GO fields other than ID and name to a go term dict.
    """
    u = dict(term)  # no side effects
    u['type'] = 'CLASS'
    u['namespace'] = None
    u['alt_ids'] = []
    u['def'] = None
    u['comments'] = []
    u['subsets'] = []
    u['synonyms'] = []
    u['xrefs'] = []
    return u
