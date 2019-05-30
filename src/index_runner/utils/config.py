import urllib.request
import yaml
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
    config_url = os.environ.get('GLOBAL_CONFIG_URL', 'https://github.com/kbaseIncubator/search_config/releases/download/0.0.1/config.yaml')  # noqa
    # Load the global configuration release (non-environment specific, public config)
    if not config_url.startswith('http'):
        raise RuntimeError(f"Invalid global config url: {config_url}")
    with urllib.request.urlopen(config_url) as res:  # nosec
        global_config = yaml.safe_load(res)  # type: ignore
    return {
        'global': global_config,
        'ws_token': os.environ['WORKSPACE_TOKEN'],
        'kbase_endpoint': kbase_endpoint,
        'workspace_url': workspace_url,
        'kafka_server': os.environ.get('KAFKA_SERVER', 'kafka'),
        'kafka_clientgroup': os.environ.get('KAFKA_CLIENTGROUP', 'search_indexer'),
        'error_index_name': os.environ.get('ERROR_INDEX_NAME', 'indexing_errors'),
        'topics': {
            'workspace_events': os.environ.get('KAFKA_WORKSPACE_TOPIC', 'workspaceevents'),
            'indexer_admin_events': os.environ.get('KAFKA_INDEXER_ADMIN_TOPIC', 'indexeradminevents'),
            'elasticsearch_updates': os.environ.get('KAFKA_ES_UPDATE_TOPIC', 'elasticsearch_updates')
        }
    }
