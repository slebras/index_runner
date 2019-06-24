import urllib.request
import yaml
import os
import functools


@functools.lru_cache(maxsize=2)
def get_config():
    """Initialize configuration data from the environment."""
    if not os.environ.get('WORKSPACE_TOKEN'):
        raise RuntimeError('WORKSPACE_TOKEN env var is not set.')
    es_host = os.environ.get("ELASTICSEARCH_HOST", 'elasticsearch')
    es_port = os.environ.get("ELASTICSEARCH_PORT", 9200)
    kbase_endpoint = os.environ.get('KBASE_ENDPOINT', 'https://ci.kbase.us/services').strip('/')
    workspace_url = os.environ.get(
        'KBASE_WORKSPACE_URL',
        kbase_endpoint + '/ws'
    )
    config_url = os.environ.get('GLOBAL_CONFIG_URL', 'https://github.com/kbase/search_config/releases/download/0.0.1/config.yaml')  # noqa
    # Load the global configuration release (non-environment specific, public config)
    if not config_url.startswith('http'):
        raise RuntimeError(f"Invalid global config url: {config_url}")
    with urllib.request.urlopen(config_url) as res:  # nosec
        global_config = yaml.safe_load(res)  # type: ignore
    return {
        # All zeromq-related configuration
        'zmq': {
            'queue_max': int(os.environ.get('QUEUE_MAX', 10000)),
            'num_indexers': int(os.environ.get('NUM_INDEXERS', 4)),
            'num_es_writers': int(os.environ.get('NUM_ES_WRITERS', 1)),
            'logger_port': os.environ.get('LOGGER_PORT', 5561),  # tcp port for the logger
            'socket_name': 'indexrunner'
        },
        'global': global_config,
        'ws_token': os.environ['WORKSPACE_TOKEN'],
        'kbase_endpoint': kbase_endpoint,
        'workspace_url': workspace_url,
        'elasticsearch_host': es_host,
        'elasticsearch_port': es_port,
        'elasticsearch_url': f"http://{es_host}:{es_port}",
        'kafka_server': os.environ.get('KAFKA_SERVER', 'kafka'),
        'kafka_clientgroup': os.environ.get('KAFKA_CLIENTGROUP', 'search_indexer'),
        'error_index_name': os.environ.get('ERROR_INDEX_NAME', 'indexing_errors'),
        'elasticsearch_index_prefix': os.environ.get('ELASTICSEARCH_INDEX_PREFIX', 'search2'),
        'topics': {
            'workspace_events': os.environ.get('KAFKA_WORKSPACE_TOPIC', 'workspaceevents'),
            'indexer_admin_events': os.environ.get('KAFKA_INDEXER_ADMIN_TOPIC', 'indexeradminevents'),
            'elasticsearch_updates': os.environ.get('KAFKA_ES_UPDATE_TOPIC', 'elasticsearch_updates')
        }
    }
