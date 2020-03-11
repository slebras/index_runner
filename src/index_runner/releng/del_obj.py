"""
Take object info from the workspace and delete all associated vertices and edges into Arangodb.
"""
import logging

from src.index_runner.releng import genome
import src.utils.re_client as re_client

logger = logging.getLogger('IR')

_OBJ_COLL_NAME = 'ws_object'

# Special deletion handlers for KBase types
_TYPE_PROCESSOR_MAP = {
    'KBaseGenomes.Genome': genome.delete_genome
}


def delete_object(obj_info):
    """
    Delete all the associated edges and vertices for a workspace object into RE.
    """
    # Delete documents associated with this kbase object
    # For now, the ws_object document is simply flagged
    wsid = obj_info[6]
    objid = obj_info[0]
    obj_key = f'{wsid}:{objid}'
    results = re_client.get_doc(_OBJ_COLL_NAME, obj_key).get('results')
    if not results:
        logger.warning(f"No RE document found with key {obj_key}. Cannot delete.")
    obj_doc = results[0]
    for key in ['updated_at', '_id', '_rev']:
        del obj_doc[key]
    obj_doc['deleted'] = True
    re_client.save(_OBJ_COLL_NAME, obj_doc)
