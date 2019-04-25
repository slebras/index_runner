from bs4 import BeautifulSoup
from markdown2 import Markdown

from utils.get_path import get_path

from . import indexer_utils


def process_code_cell(cell):
    # here we want to differentiate between kbase app cells and code cells
    # app_path = get_path(cell, ['metadata', 'kbase', 'appCell', 'newAppName', 'id'])
    index_cell = {
        'description': "",
        'type': ""
    }
    app_path = get_path(cell, ['metadata', 'kbase', 'appCell'])
    if app_path:
        # we know the cell is kbase application
        app_path = get_path(cell, ['metadata', 'kbase', 'attributes', 'title'])
        if app_path:
            desc = cell['metadata']['kbase']['attributes']['title']
        else:
            app_path = get_path(cell, ['metadata', 'kbase', 'appCell', 'app', 'id'])
            if app_path:
                desc = cell['metadata']['kbase']['appCell']['app']['id']
            else:
                desc = ""
        index_cell['type'] = "kbase_app"
    else:
        app_path = get_path(cell, ['source'])
        if app_path:
            desc = cell['source']
        else:
            desc = ""
        index_cell['type'] = "code_cell"
    index_cell['description'] = desc

    return index_cell


def index_narrative(obj_data, ws_info, obj_data_v1):
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

    upa = ':'.join([str(data['info'][6]), str(data['info'][0]), str(data['info'][4])])

    ws_id = obj_info[6]

    shared_users = indexer_utils.get_shared_users(ws_id)
    narrative_data_objects = indexer_utils.get_narrative_data(ws_id)

    markdowner = Markdown()
    index_cells = []
    cells = data['data']['cells']
    creator = data['creator']
    for cell in cells:
        if (cell.get('cell_type') == 'markdown'
                and cell.get('source')
                and not cell['source'].startswith('## Welcome to KBase')):
            # format markdown into string
            cell_soup = BeautifulSoup(markdowner.convert(cell['source']), 'html.parser')
            index_cell = {
                'description': cell_soup.get_text(),
                'type': "markdown"
            }
            index_cell['type'] = "markdown"
            index_cell['description'] = cell_soup.get_text()
        # for an app cell the module/method name lives in metadata/kbase/appCell/app/id
        elif cell.get('cell_type') == 'code':
            index_cell = process_code_cell(cell)
        else:
            cell_type = cell.get('cell_type', "Error: no cell type")
            print(f"Narrative Indexer: could not resolve cell type \"{cell_type}\"")
        index_cells.append(index_cell)

    metadata = obj_info[-1] or {}  # last elem of obj info is a metadata dict
    narr_name = metadata.get('name')
    is_public = ws_info[6] == 'r'
    return {
        'doc': {
            'name': narr_name,
            'upa': upa,
            'data_objects': narrative_data_objects,
            'cells': index_cells,
            'creator': creator,
            'shared_users': shared_users,
            'total_cells': len(cells),
            'access_group': data['info'][6],
            'public': is_public,
            'islast': True,
            'shared': False
        },
        'index': 'narrative',
        'id': upa
    }
