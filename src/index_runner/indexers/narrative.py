from bs4 import BeautifulSoup
from markdown2 import Markdown

from utils.get_path import get_path

from . import indexer_utils


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

    shared_users = indexer_utils.get_shared_users(data)

    markdowner = Markdown()
    cell_text = []
    app_names = []
    cells = data['data']['cells']
    creator = data['creator']
    for cell in cells:
        if (cell.get('cell_type') == 'markdown'
                and cell.get('source')
                and not cell['source'].startswith('## Welcome to KBase')):
            # ---
            cell_soup = BeautifulSoup(markdowner.convert(cell['source']), 'html.parser')
            cell_text.append(cell_soup.get_text())
        # for an app cell the module/method name lives in metadata/kbase/appCell/app/id
        if cell.get('cell_type') == 'code':
            app_path = get_path(cell, ['metadata', 'kbase', 'appCell', 'app', 'id'])
            if app_path:
                app_names.append(cell['metadata']['kbase']['appCell']['app']['id'])
    metadata = obj_info[-1] or {}  # last elem of obj info is a metadata dict
    narr_name = metadata.get('name')
    is_public = ws_info[6] == 'r'
    return {
        'doc': {
            'name': narr_name,
            'upa': upa,
            'markdown_text': cell_text,
            'app_names': app_names,
            'creator': creator,
            'shared_users': shared_users,
            'total_cells': len(cells),
            'accgrp': data['info'][6],
            'public': is_public,
            'islast': True,
            'shared': False
        },
        'index': 'narrative',
        'id': upa
    }
