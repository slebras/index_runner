import os
import requests
import sys
import json

_ES_URL = os.environ.get('ES_URL', 'http://localhost:9200')
_IDX_NAME = os.environ.get('IDX_NAME', 'search2.indexing_errors:1')
_SEARCH_URL = f'{_ES_URL}/{_IDX_NAME}/_search'


def _get_count():
    """Get total count of documents in search2.indexing_errors."""
    resp = requests.get(_SEARCH_URL, params={'size': 0})
    rj = resp.json()
    if not resp.ok:
        raise RuntimeError(json.dumps(rj, indent=2))
    print('Total errors:', rj['hits']['total'])


def _get_count_by_type():
    """Get count of errors by evtype."""
    resp = requests.post(
        _SEARCH_URL,
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


def _get_upas():
    resp = requests.post(
        _SEARCH_URL,
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


def _show_help():
    print('Valid commands:')
    for cmd_name in _CMD_HANDLERS:
        print(_pad(cmd_name, _CMD_HANDLERS[cmd_name]['help']))


def _pad(left, right, amount=17):
    """Left pad a key/val string."""
    pad = ' ' * (amount - len(left))
    return f"{left} {pad} - {right}"


_CMD_HANDLERS = {
    'err_count': {'fn': _get_count, 'help': 'Get total count of indexing errors.'},
    'err_count_by_type': {'fn': _get_count_by_type, 'help': 'Get count of errors by evtype.'},
    'err_upas': {'fn': _get_upas, 'help': 'Prints (to stdout) a linebreaked list of UPAs for all errors.'},
    'help': {'fn': _show_help, 'help': 'Show help for this CLI'},
    '--help': {'fn': _show_help, 'help': 'Show help for this CLI'},
    '-h': {'fn': _show_help, 'help': 'Show help for this CLI'}
}

if __name__ == '__main__':
    resp = requests.get(_ES_URL)
    if not resp.ok:
        print(resp.text)
        raise RuntimeError('Elasticsearch is not responding.')
    if len(sys.argv) == 1:
        _show_help()
        sys.exit(0)
    cmd = sys.argv[1]
    if cmd not in _CMD_HANDLERS:
        sys.stderr.write('Invalid command.')
        _show_help()
        sys.exit(1)
    _CMD_HANDLERS[cmd]['fn']()  # type: ignore
