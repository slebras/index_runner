import json

from index_runner.indexers.narrative import index_narrative


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
            obj_data = j['full_data']
            ws_info = j['ws_info']
            try:
                index = index_narrative(obj_data, ws_info)
            except Exception as err:
                print(j)
                raise err
            json.dump(index['doc'], outfd)
            outfd.write('\n')
    finally:
        infd.close()
        outfd.close()
        print('..written to narrative_indexes.json')
