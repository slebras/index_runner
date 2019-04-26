import json

from index_runner.indexers.main import index_obj


def main(inpath, outpath):
    """
    For each narrative object data.
    Fetch the object data for each narrative object by the full upa
    (ws_id/obj_id/ver). Writes to ./narrative_data.json
    """
    infd = open(inpath)
    outfd = open(outpath, 'a')
    try:
        for line in infd:
            j = json.loads(line)
            try:
                index = index_obj(j)
            except Exception as err:
                print(err)
                print(j)
            json.dump(index['doc'], outfd)
            outfd.write('\n')
    finally:
        infd.close()
        outfd.close()
        print('..written to narrative_indexes.json')
