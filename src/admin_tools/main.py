import requests
import sys
import re
import json
import argparse
from confluent_kafka import Producer
from kbase_workspace_client import WorkspaceClient
from kbase_workspace_client.exceptions import WorkspaceResponseError

from src.utils.config import config

_ES_URL = config()['elasticsearch_url']
_ERR_IDX_NAME = config()['elasticsearch_index_prefix'] + '.' + config()['error_index_name']
_ERR_SEARCH_URL = f'{_ES_URL}/{_ERR_IDX_NAME}/_search'


def _get_count(args):
    """Get total count of documents in search2.indexing_errors."""
    if args.by_type:
        return _get_count_by_type(args)
    resp = requests.get(_ERR_SEARCH_URL, params={'size': 0})
    rj = resp.json()
    if not resp.ok:
        raise RuntimeError(json.dumps(rj, indent=2))
    print('Total errors:', rj['hits']['total'])


def _get_count_by_type(args):
    """Get count of errors by evtype."""
    resp = requests.post(
        _ERR_SEARCH_URL,
        data=json.dumps({
            'size': 0,
            'aggs': {
                'group_by_evtype': {'terms': {'field': 'evtype'}}
            }
        })
    )
    rj = resp.json()
    if not resp.ok:
        raise RuntimeError(json.dumps(rj, indent=2))
    for count_doc in rj['aggregations']['group_by_evtype']['buckets']:
        print(_pad(count_doc['key'], count_doc['doc_count'], amount=30))


def _get_upas(args):
    resp = requests.post(
        _ERR_SEARCH_URL,
        params={'scroll': '1m'},
        data=json.dumps({
            'size': 100,
            '_source': ['wsid', 'objid', 'error']
        })
    )
    rj = resp.json()
    if not resp.ok:
        raise RuntimeError(json.dumps(rj, indent=2))
    scrolling = True
    scroll_id = rj['_scroll_id']
    _print_upas(rj)
    while scrolling:
        resp = requests.post(
            _ES_URL + '/_search/scroll',
            data=json.dumps({
                'scroll': '1m',
                'scroll_id': scroll_id
            })
        )
        rj = resp.json()
        if not resp.ok:
            raise RuntimeError(json.dumps(rj, indent=2))
        if rj['hits']['hits']:
            _print_upas(rj)
        else:
            scrolling = False


def _print_upas(doc):
    for doc in doc['hits']['hits']:
        s = doc['_source']
        print(f'{s["wsid"]} {s["objid"]} "{s["error"]}"')


def _pad(left, right, amount=17):
    """Left pad a key/val string."""
    pad = ' ' * (amount - len(left))
    return f"{left} {pad} {right}"


def _reindex(args):
    id_pieces = args.ref.split('/')
    if len(id_pieces) > 3:
        raise ValueError("--ref value should be in the format '1/2', '1/2/3', or '1'")
    reindexing_obj = len(id_pieces) >= 2
    if reindexing_obj:
        ev = {'evtype': 'INDEX_NONEXISTENT', 'wsid': id_pieces[0], 'objid': id_pieces[1]}
        if len(id_pieces) == 3:
            ev['ver'] = id_pieces[2]
        if args.overwrite:
            ev['evtype'] = 'REINDEX'
    else:
        ev = {'evtype': 'INDEX_NONEXISTENT_WS', 'wsid': id_pieces[0]}
        if args.overwrite:
            ev['evtype'] = 'REINDEX_WS'
    print('Producing...')
    _produce(ev)


def _reindex_ws_range(args):
    evtype = 'INDEX_NONEXISTENT_WS'
    if args.overwrite:
        evtype = 'REINDEX_WS'
    count = 0
    for wsid in range(args.min, args.max + 1):
        _produce({'evtype': evtype, 'wsid': wsid})
        count += 1
    print(f'Produced {count} total events.')


def _reindex_ws_type(args):
    """
    Reindex all objects in the entire workspace server based on a type name.
    """
    if not re.match(r'^.+\..+-\d+\.\d+$', args.type):
        sys.stderr.write('Enter the full type name, such as "KBaseGenomes.Genome-17.0"')
        sys.exit(1)
    # - Iterate over all workspaces
    #   - For each workspace, list objects
    #   - For each obj matching args.type, produce a reindex event
    ws = WorkspaceClient(url=config()['kbase_endpoint'], token=config()['ws_token'])
    evtype = 'INDEX_NONEXISTENT'
    if args.overwrite:
        evtype = 'REINDEX'
    for wsid in range(args.start, args.stop + 1):
        try:
            infos = ws.admin_req('listObjects', {'ids': [wsid]})
        except WorkspaceResponseError as err:
            print(err.resp_data['error']['message'])
            continue
        for obj_info in infos:
            obj_type = obj_info[2]
            if obj_type == args.type:
                _produce({'evtype': evtype, 'wsid': wsid, 'objid': obj_info[0]})
    print('..done!')


def _produce(data, topic=config()['topics']['admin_events']):
    producer = Producer({'bootstrap.servers': config()['kafka_server']})
    producer.produce(topic, json.dumps(data), callback=_delivery_report)
    producer.poll(60)


def _delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed:', err)
    else:
        print(f"Message delivered to topic '{msg.topic()}': {msg.value()}")


if __name__ == '__main__':
    resp = requests.get(_ES_URL)
    if not resp.ok:
        print(resp.text)
        raise RuntimeError('Elasticsearch is not responding.')
    parser = argparse.ArgumentParser(prog='indexer_admin', description='Admin utilities for the index runner.')
    subparsers = parser.add_subparsers()
    # -- err_count command
    err_count = subparsers.add_parser('err_count')
    err_count.add_argument(
        '--by-type',
        help='Get a count of errors by workspace type.',
        action='store_true',
        default=False
    )
    err_count.set_defaults(func=_get_count)
    # err_upas command
    err_upas = subparsers.add_parser('err_upas', help='Print the UPAs of all logged errors to stdout.')
    err_upas.set_defaults(func=_get_upas)
    # -- reindex command
    reindex = subparsers.add_parser('reindex', help='Reindex an object or workspace.')
    reindex.add_argument(
        '--ref',
        '-r',
        help='Workspace ID or object ID to reindex (eg. "1", "1/2", or "1/2/3")',
        required=True,
        type=str
    )
    reindex.add_argument(
        '--overwrite',
        help='Index and overwrite existing documents. Defaults to false.',
        required=False,
        default=False,
        action='store_true'
    )
    reindex.set_defaults(func=_reindex)
    # -- reindex type command
    reindex_type = subparsers.add_parser(
        'reindex_type',
        help='Reindex all latest versions of objects with a specific type',
    )
    reindex_type.add_argument(
        '--type',
        '-t',
        help='Full type name, such as "KBaseGenomes.Genome-17.0"',
        required=True,
        action='store'
    )
    reindex_type.add_argument(
        '--overwrite',
        help='Index and overwrite existing documents. Defaults to false.',
        required=False,
        default=False,
        action='store_true'
    )
    reindex_type.add_argument(
        '--start',
        help='ID of workspace to start indexing.',
        required=False,
        default=1,
        type=int,
        action='store'
    )
    reindex_type.add_argument(
        '--stop',
        help='ID of workspace to stop indexing (inclusive).',
        required=False,
        type=int,
        default=50000,
        action='store'
    )
    reindex_type.set_defaults(func=_reindex_ws_type)
    # -- reindex range command
    reindex_range = subparsers.add_parser(
        'reindex_range',
        help='Reindex a range of workspace ids with a min and max.'
    )
    reindex_range.add_argument(
        '--min',
        help='Minimum workspace ID to start indexing on. Defaults to 1.',
        default=1,
        type=int,
        required=False,
        action='store'
    )
    reindex_range.add_argument(
        '--max',
        help='Max workspace ID to end indexing on. Inclusive.',
        type=int,
        required=True,
        action='store'
    )
    reindex_range.add_argument(
        '--overwrite',
        help='Index and overwrite existing documents. Defaults to false.',
        required=False,
        default=False,
        action='store_true'
    )
    reindex_range.set_defaults(func=_reindex_ws_range)
    args = parser.parse_args()
    if len(sys.argv) == 1:
        parser.print_help()
    else:
        args.func(args)
