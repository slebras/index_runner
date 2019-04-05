import os


def get_config():
    """
    Initialize configuration data.
    """
    if not os.environ.get('WORKSPACE_TOKEN'):
        raise RuntimeError('WORKSPACE_TOKEN env var is not set.')
    return {
        'ws_token': os.environ['WORKSPACE_TOKEN'],
        'kbase_endpoint': os.environ.get('KBASE_ENDPOINT', 'https://ci.kbase.us/services'),
        'kafka_indexer_topic': os.environ.get('KAFKA_INDEXER_TOPIC', 'idxevents'),
        'kafka_server': os.environ.get('KAFKA_SERVER', 'kafka'),
        'kafka_clientgroup': os.environ.get('KAFKA_CLIENTGROUP', 'search_indexer'),
        'topics': {
            'workspace_events': os.environ.get('KAFKA_WORKSPACE_TOPIC', 'workspaceevents'),
            'generate_idx': os.environ.get('KAFKA_GEN_INDEX_TOPIC', 'elasticsearch_generate_idx'),
            'save_idx': os.environ.get('KAFKA_SAVE_INDEX_TOPIC', 'elasticsearch_save_idx')
        }
    }
