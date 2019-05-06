import os


def get_config():
    """
    Initialize configuration data.
    """
    if not os.environ.get('WORKSPACE_TOKEN'):
        raise RuntimeError('WORKSPACE_TOKEN env var is not set.')
    kbase_endpoint = os.environ.get('KBASE_ENDPOINT', 'https://ci.kbase.us/services').strip('/')
    workspace_url = os.environ.get(
        'KBASE_WORKSPACE_URL',
        kbase_endpoint + '/ws'
    )
    return {
        'ws_token': os.environ['WORKSPACE_TOKEN'],
        'kbase_endpoint': kbase_endpoint,
        'workspace_url': workspace_url,
        'kafka_indexer_topic': os.environ.get('KAFKA_INDEXER_TOPIC', 'idxevents'),
        'kafka_server': os.environ.get('KAFKA_SERVER', 'kafka'),
        'kafka_clientgroup': os.environ.get('KAFKA_CLIENTGROUP', 'search_indexer'),
        'elasticsearch_host': os.environ.get("ELASTICSEARCH_HOST", 'elasticsearch'),
        'elasticsearch_port': os.environ.get("ELASTICSEARCH_PORT", 9200),
        'elasticsearch_index_prefix': os.environ.get("ELASTICSEARCH_INDEX_PREFIX", "kbaseci"),
        'elasticsearch_data_type': os.environ.get("ELASTICSEARCH_DATA_TYPE", 'default1'),
        'topics': {
            'workspace_events': os.environ.get('KAFKA_WORKSPACE_TOPIC', 'workspaceevents'),
            'elasticsearch_updates': os.environ.get('KAFKA_ES_UPDATE_TOPIC', 'elasticsearch_updates')
        }
    }
