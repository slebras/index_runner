

def main(obj_data, ws_info, obj_data_v1, conf):
    """
    Compatible with:
        KBaseTrees.Tree-1.0
    """
    info = obj_data['info']
    data = obj_data['data']
    workspace_id = info[6]
    object_id = info[0]
    yield {
        '_action': 'index',
        'doc': {
            'tree_name': data.get('name', None),
            'type': data.get('type', None),
            'labels': [
                {'node_id': key, 'label': val}
                for key, val in data.get('default_node_labels', {}).items()
            ],
        },
        'index': conf['index_name'],
        'id': f"{conf['namespace']}::{workspace_id}:{object_id}"
    }
