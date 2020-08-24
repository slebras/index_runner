import sys
from bs4 import BeautifulSoup
from markdown2 import Markdown
import logging
from kbase_workspace_client import WorkspaceClient

from src.utils.get_path import get_path
from src.utils.formatting import ts_to_epoch
from src.utils.config import config

logger = logging.getLogger('IR')

_NAMESPACE = "WS"
_MARKDOWNER = Markdown()
_NARRATIVE_INDEX_VERSION = 2
_NARRATIVE_INDEX_NAME = 'narrative_' + str(_NARRATIVE_INDEX_VERSION)


def index_narrative(obj_data, ws_info, obj_data_v1):
    """
    Index a narrative object on save.
    We index the latest narratives for:
        - title and author
        - cell content
        - object names and types
        - created and updated dates
        - total number of cells
    """
    # Reference for the workspace info type:
    #    https://kbase.us/services/ws/docs/Workspace.html#typedefWorkspace.workspace_info
    # Reference for the object info type:
    #    https://kbase.us/services/ws/docs/Workspace.html#typedefWorkspace.object_info
    obj_info = obj_data['info']
    obj_id = obj_info[0]
    obj_metadata = obj_info[-1]
    if not obj_metadata:
        raise RuntimeError(f"Cannot index narrative: no metadata for the narrative object. Obj info: {obj_info}")
    [ws_id, _, owner, moddate, _, _, _, _, ws_metadata] = ws_info
    if not ws_metadata:
        raise RuntimeError(f"Cannot index narrative: no metadata for the workspace. WS info: {ws_info}")
    is_temporary = _narrative_is_temporary(ws_metadata)
    is_narratorial = _narrative_is_narratorial(ws_metadata)
    narrative_title = obj_metadata.get('name')
    creator = obj_data['creator']
    # Get all the types and names of objects in the narrative's workspace.
    narrative_data_objects = _fetch_objects_in_workspace(ws_id)
    # Extract all the data we want to index from the notebook cells
    raw_cells = obj_data['data'].get('cells', [])
    index_cells = _extract_cells(raw_cells, ws_id)
    result = {
        '_action': 'index',
        'doc': {
            'narrative_title': narrative_title,
            'is_temporary': is_temporary,
            'is_narratorial': is_narratorial,
            'data_objects': narrative_data_objects,
            'owner': owner,
            'modified_at': ts_to_epoch(moddate),
            'cells': index_cells,
            'creator': creator,
            'total_cells': len(raw_cells),
            'static_narrative_saved': ws_metadata.get('static_narrative_saved'),
            'static_narrative_ref': ws_metadata.get('static_narrative'),
        },
        'index': _NARRATIVE_INDEX_NAME,
        'id': f'{_NAMESPACE}::{ws_id}:{obj_id}',
    }
    yield result


def _narrative_is_temporary(ws_metadata):
    return ws_metadata.get('is_temporary') == 'true'


def _narrative_is_narratorial(ws_metadata):
    return ws_metadata.get('narratorial') == '1'


def _extract_cells(cells, workspace_id):
    index_cells = []
    for cell in cells:
        if cell.get('cell_type') == 'markdown':
            if not cell.get('source'):
                # Empty markdown cell
                continue
            if cell['source'].startswith('## Welcome to KBase'):
                # Ignore boilerplate markdown cells
                continue
            # Remove all HTML and markdown formatting syntax.
            cell_soup = BeautifulSoup(_MARKDOWNER.convert(cell['source']), 'html.parser')
            index_cell = {'desc': cell_soup.get_text(), 'cell_type': "markdown"}
            index_cell['cell_type'] = "markdown"
            index_cell['desc'] = cell_soup.get_text()
        # For an app cell, the module/method name lives in metadata/kbase/appCell/app/id
        elif cell.get('cell_type') == 'code':
            index_cell = _process_code_cell(cell)
        else:
            cell_type = cell.get('cell_type', "Error: no cell type")
            sys.stderr.write(f"Narrative Indexer: could not resolve cell type \"{cell_type}\"\n")
            sys.stderr.write(str(cell))
            sys.stderr.write('\n' + ('-' * 80) + '\n')
            index_cell = {'desc': 'Narrative Cell', 'cell_type': 'unknown'}
        index_cells.append(index_cell)
    return index_cells


def _process_code_cell(cell):
    # here we want to differentiate between kbase app cells and code cells
    # app_path = get_path(cell, ['metadata', 'kbase', 'appCell', 'newAppName', 'id'])
    index_cell = {'desc': '', 'cell_type': ''}
    widget_data = get_path(cell, ['metadata', 'kbase', 'outputCell', 'widget'])
    app_cell = get_path(cell, ['metadata', 'kbase', 'appCell'])
    data_cell = get_path(cell, ['metadata', 'kbase', 'dataCell'])
    if app_cell:
        # We know that the cell is a KBase app
        # Try to get the app name from a couple possible paths in the object
        app_name1 = get_path(cell, ['metadata', 'kbase', 'attributes', 'title'])
        app_name2 = get_path(app_cell, ['app', 'id'])
        index_cell['desc'] = app_name1 or app_name2 or ''
        index_cell['cell_type'] = 'kbase_app'
    elif widget_data:
        index_cell['cell_type'] = 'widget'
        index_cell['desc'] = widget_data.get('name', '')
    elif data_cell:
        index_cell['cell_type'] = 'data'
        name = get_path(data_cell, ['objectInfo', 'name'])
        obj_type = get_path(data_cell, ['objectInfo', 'cell_type'])
        if name and obj_type:
            index_cell['desc'] = f'{name} ({obj_type})'
        else:
            index_cell['desc'] = name or obj_type or ''
    else:
        # Regular code-cell
        index_cell['cell_type'] = 'code_cell'
        index_cell['desc'] = cell.get('source', '')
    return index_cell


def _fetch_objects_in_workspace(ws_id):
    """
    Get a list of dicts with keys 'obj_type' and 'name' corresponding to all data
    objects in the requested workspace. This discludes the narrative object.
    Args:
        ws_id - a workspace id
    """
    ws_client = WorkspaceClient(url=config()['kbase_endpoint'], token=config()['ws_token'])
    obj_infos = ws_client.generate_obj_infos(ws_id, admin=True)
    return [
        {"name": info[1], "obj_type": info[2]}
        for info in obj_infos
        if 'KBaseNarrative' not in str(info[2])
    ]
