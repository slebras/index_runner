"""
Indexer logic based on type
"""
from kbase_workspace_client import WorkspaceClient
from kbase_workspace_client.exceptions import WorkspaceResponseError

from ..utils.config import get_config


def index_obj(msg_data):
    """
    For a newly created object, generate the index document for it and push to
    the elasticsearch topic on Kafka.

    Args:
        msg_data - json event data received from the kafka workspace events stream
    """
    # Fetch the object data from the workspace API
    upa = _get_upa_from_msg_data(msg_data)
    config = get_config()
    ws_url = config['workspace_url']
    ws_client = WorkspaceClient(url=ws_url, token=config['ws_token'])
    upa = _get_upa_from_msg_data(msg_data)
    try:
        obj_data = ws_client.admin_req('getObjects', {
            'objects': [{'ref': upa}]
        })
    except WorkspaceResponseError as err:
        print('Workspace response error:', err.resp_data)
        raise err
    # Dispatch to a specific type handler to produce the search document
    (type_module_name, type_version) = msg_data['objtype'].split('-')
    (type_module, type_name) = type_module_name.split('.')
    indexer = _find_indexer(type_module, type_name, type_version)
    return indexer(obj_data)


def _find_indexer(type_module, type_name, type_version):
    """
    Find the indexer function for the given object type within the indexer_directory list.
    """
    for entry in indexer_directory:
        module_match = ('module' not in entry) or entry['module'] == type_module
        name_match = ('type' not in entry) or entry['type'] == type_name
        ver_match = ('version' not in entry) or entry['version'] == type_version
        if module_match and name_match and ver_match:
            return entry['indexer']
    return default_indexer


def default_indexer(obj_data):
    """
    Default indexer fallback, when we don't find a type-specific handler.
    """
    return {'schema': {}, 'data': obj_data}


def index_narrative(obj_data):
    """
    Index a narrative object on save.
    We only the latest narratives for:
        - title and author
        - markdown cell content (non boilerplate)
        - created and updated dates
        - total number cells
    """
    # Narrative name
    # Markdown cells that are not the welcome boilerplate
    # maybes:
    #  - app names
    #  - obj names
    #  - creator
    data = obj_data['data'][0]
    obj_info = data['info']
    cell_text = []
    app_names = []
    cells = data['data']['cells']
    creator = data['creator']
    for cell in cells:
        if (cell.get('cell_type') == 'markdown'
                and cell.get('source')
                and not cell['source'].startswith('## Welcome to KBase')):
            # ---
            cell_text.append(cell['source'])
        # for an app cell the module/method name lives in metadata/kbase/appCell/app/id
        if (cell.get('cell_type') == 'code'
                and cell['metadata']['kbase'].get('appCell')):
            app_names.append(cell['metadata']['kbase']['appCell']['app']['id'])
    metadata = obj_info[-1]  # last elem of obj info is a metadata dict
    narr_name = metadata['name']
    return {
        'mapping': {
            'name': {'type': 'text'},
            'markdown_text': {'type': 'text'},
            'app_names': {'type': 'text'},
            'creator': {'type': 'text'},
            'total_cells': {'type': 'short'},
            'epoch': {'type': 'date'}
        },
        'doc': {
            'name': narr_name,
            'markdown_text': cell_text,
            'app_names': app_names,
            'creator': creator,
            'total_cells': len(cells),
            'epoch': data['epoch']
        }
    }


# Directory of all indexer functions.
# Higher up in the list gets higher precedence.
indexer_directory = [
    {'module': 'KBaseNarrative', 'type': 'Narrative', 'indexer': index_narrative}
]


def _get_upa_from_msg_data(event_data):
    """Get the UPA workspace reference from a Kafka workspace event payload."""
    ws_id = event_data.get('wsid')
    if not ws_id:
        raise RuntimeError(f'Event data missing the "wsid" field for workspace ID: {event_data}')
    obj_id = event_data.get('objid')
    if not ws_id:
        raise RuntimeError(f'Event data missing the "objid" field for object ID: {event_data}')
    obj_ver = event_data.get('ver')
    if not ws_id:
        raise RuntimeError(f'Event data missing the "ver" field for object ID: {event_data}')
    return f"{ws_id}/{obj_id}/{obj_ver}"
