import sys
from bs4 import BeautifulSoup
from markdown2 import Markdown

from utils.get_path import get_path
from . import indexer_utils

_NAMESPACE = "WS"
_MARKDOWNER = Markdown()
_NARRATIVE_INDEX_VERSION = 1
_NARRATIVE_INDEX_NAME = 'narrative:' + str(_NARRATIVE_INDEX_VERSION)


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
    obj_info = obj_data['info']
    obj_id = obj_info[0]
    workspace_id = obj_data['info'][6]
    ws_id = obj_info[6]
    # Get all the types and names of objects in the narrative's workspace.
    narrative_data_objects = indexer_utils.fetch_objects_in_workspace(ws_id)
    index_cells = []
    cells = obj_data['data']['cells']
    creator = obj_data['creator']
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
    metadata = obj_info[-1] or {}  # last elem of obj info is a metadata dict
    narrative_title = metadata.get('name')
    result = {
        'doc': {
            'narrative_title': narrative_title,
            'data_objects': narrative_data_objects,
            'cells': index_cells,
            'creator': creator,
            'total_cells': len(cells),
        },
        'index': _NARRATIVE_INDEX_NAME,
        'id': f'{_NAMESPACE}::{workspace_id}:{obj_id}',
    }
    yield result


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
