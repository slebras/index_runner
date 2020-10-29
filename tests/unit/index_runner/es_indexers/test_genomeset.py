"""
The genomeset indexer uses the SDK, and we don't yet have a good way to test those.
"""

# # Some previous code
# def test_from_sdk(self):
#     check_against = [{
#         '_action': 'index',
#         'index': "genomeset_1",
#         'id': "WS::22385:82",
#         'doc': {
#             "genomes": [
#                 {"label": "gen1", "genome_ref": "22385/65/1"},
#                 {"label": "gen2", "genome_ref": "22385/68/1"},
#                 {"label": "gen3", "genome_ref": "22385/70/1"},
#                 {"label": "gen4", "genome_ref": "22385/74/1"},
#                 {"label": "gen5", "genome_ref": "22385/76/1"},
#                 {"label": "gen6", "genome_ref": "22385/80/1"}
#             ],
#             "description": "Listeria monocytogenes Roary Test"
#         }
#     }]
#     self._default_obj_test('genomeset_save', index_from_sdk, check_against)
