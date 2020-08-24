"""
Take object info from the workspace and delete all associated vertices and edges into Arangodb.
"""
import src.utils.logger as logger
import src.utils.re_client as re_client

_OBJ_COLL_NAME = 'ws_object'
_OBJ_VER_COLL_NAME = 'ws_object_version'


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
    # Delete the unversioned object
    re_client.save(_OBJ_COLL_NAME, obj_doc)
    # Delete all versioned objects
    query = f"""
    FOR doc IN ws_object_version
        FILTER doc.workspace_id == @wsid AND doc.object_id == @objid
        UPDATE {{deleted: true, _key: doc._key}} IN {_OBJ_VER_COLL_NAME}
    """
    re_client.execute_query(query, {'wsid': wsid, 'objid': objid})
