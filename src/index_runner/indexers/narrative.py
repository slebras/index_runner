from utils.get_path import get_path


def index_narrative(obj_data, ws_info):
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
            'total_cells': len(cells),
            'accgrp': data['info'][6],
            'epoch': data['epoch'],
            'public': is_public,
            'islast': True,
            'shared': False
        },
        'index': 'narratives'
    }
