import os
import sys

from bulk_indexers.narrative import (
    # fetch_narrative_objects,
    fetch_narrative_upas,
    fetch_narrative_workspaces,
    generate_narrative_indexes
)


if __name__ == '__main__':
    if 'WORKSPACE_TOKEN' not in os.environ:
        sys.stderr.write('Set the WORKSPACE_TOKEN env var to a workspace admin token.\n')
        sys.exit(1)
    token = os.environ['WORKSPACE_TOKEN']
    # Pipeline:
    #  -> narrative_infos (Narrative info tuples)
    #  -> narrative_upas (UPAs with obj id of each narrative)
    #  -> narrative_data (full narrative data objects)
    #  -> narrative_indexes (index objects for elasticsearch_)
    # Find all workspaces with narrative metadata
    if not os.path.isfile('narrative_infos.json'):
        fetch_narrative_workspaces.main('narrative_infos.json', token)
    # Find the ID of the latest version of the actual narrative object within each workspace
    if not os.path.isfile('narrative_upas.json'):
        fetch_narrative_upas.main('narrative_infos.json', 'narrative_upas.json', token)
    # Fetch the full objects for each narrative, with all the details inside
    # if not os.path.isfile('narrative_data.json'):
    #     fetch_narrative_objects.main('narrative_upas.json', 'narrative_data.json', token)
    # Generate the index data for each narrative
    if not os.path.isfile('narrative_indexes.json'):
        generate_narrative_indexes.main('narrative_upas.json', 'narrative_indexes.json')
