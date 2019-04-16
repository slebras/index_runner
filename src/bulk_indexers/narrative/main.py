import os
import sys

from bulk_indexers.narrative import (
    fetch_narrative_objects,
    fetch_narrative_upas,
    fetch_narrative_workspaces,
    generate_narrative_indexes
)


if __name__ == '__main__':
    if len(sys.argv) != 2 or not sys.argv[1]:
        sys.stderr.write('Pass in your workspace admin token for CI.\n')
        sys.exit(1)
    token = sys.argv[1]
    # Find all workspaces with narrative metadata
    if not os.path.isfile('narratives1.json'):
        fetch_narrative_workspaces.main('narratives1.json', token)
    # Find the ID of the latest version of the actual narrative object within each workspace
    if not os.path.isfile('narratives2.json'):
        fetch_narrative_upas.main('narratives1.json', 'narratives2.json', token)
    # Fetch the full objects for each narrative, with all the details inside
    if not os.path.isfile('narratives3.json'):
        fetch_narrative_objects.main('narratives2.json', 'narratives3.json', token)
    # Generate the index data for each narrative
    if not os.path.isfile('narrative_indexes.json'):
        generate_narrative_indexes.main('narratives3.json', 'narrative_indexes.json')
