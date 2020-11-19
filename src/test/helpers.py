"""
General testing utilities
"""
import time
import json
import requests
from confluent_kafka import Producer
import src.utils.re_client as re_client
from src.utils.config import config


def assert_subset(testCls, subset, _dict):
    """
    Replacement for the deprecated `assertDictContainsSubset` method.
    Assert that all keys and values in `subset` are present in `_dict`
    """
    for (key, val) in subset.items():
        testCls.assertEqual(subset.get(key), _dict.get(key))


def get_es_doc(_id):
    """Fetch a document from elastic based on ID."""
    prefix = config()['elasticsearch_index_prefix']
    url = f"{config()['elasticsearch_url']}/{prefix}.*/_search?size=1"
    resp = requests.post(
        url,
        data=json.dumps({
            'query': {'term': {'_id': _id}}
        }),
        headers={'Content-Type': 'application/json'}
    )
    if not resp.ok:
        raise RuntimeError(resp.text)
    respj = resp.json()
    if not respj['hits']['total']['value']:
        return None
        # raise RuntimeError(f"Document {_id} not found.")
    return respj['hits']['hits'][0]


def get_es_doc_blocking(_id, timeout=180):
    """Fetch a doc on ES, waiting for it to become available, with a timeout."""
    start_time = int(time.time())
    while True:
        print(f'Waiting for doc {_id}')
        result = get_es_doc(_id)
        if result:
            return result
        if (int(time.time()) - start_time) > timeout:
            raise RuntimeError(f"Document {_id} not found in {timeout}s.")
        time.sleep(5)


def get_es_aliases():
    """Fetch aliases from elasticsearch"""
    url = f"{config()['elasticsearch_url']}/_aliases"
    resp = requests.get(url)
    if not resp.ok:
        raise RuntimeError(resp.text)
    respj = resp.json()
    if len(respj) < 1:
        return None
    aliases = {}  # type: dict
    # we remove prefix from index and alias names (should all be "search2.")
    for index in respj:
        for al in respj[index]['aliases'].keys():
            al = al.split('.')[1]
            if al in aliases:
                aliases[al].append(index.split('.')[1])
            else:
                aliases[al] = [index.split('.')[1]]
    return aliases


def wait_for_re_doc(coll, key, timeout=180):
    """Fetch a doc with the RE API, waiting for it to become available with a 30s timeout."""
    start_time = time.time()
    while True:
        print(f'Waiting for doc {coll}/{key}')
        results = re_client.get_doc(coll, key)
        if results['count'] > 0:
            break
        else:
            if int(time.time() - start_time) > timeout:
                raise RuntimeError('Timed out trying to fetch', key)
            time.sleep(1)
    return results['results'][0]


def _delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to: {msg.topic()}')


def produce(data, topic=config()['topics']['workspace_events']):
    producer = Producer({'bootstrap.servers': config()['kafka_server']})
    producer.produce(topic, json.dumps(data), callback=_delivery_report)
    producer.flush()
    print(f"Produced {data}")
