import urllib.request
import yaml
import os
import time
import functools


def config():
    """wrapper for get config that reloads config every 'config_timeout' seconds"""
    config = get_config()
    if (time.time() - config['last_config_reload']) > config['config_timeout']:
        get_config.cache_clear()
        config = get_config()
    return config


@functools.lru_cache(maxsize=1)
def get_config():
    """Initialize configuration data from the environment."""
    reqs = ['WORKSPACE_TOKEN', 'RE_API_TOKEN', 'MOUNT_DIR']
    for req in reqs:
        if not os.environ.get(req):
            raise RuntimeError(f'{req} env var is not set.')
    es_host = os.environ.get("ELASTICSEARCH_HOST", 'elasticsearch')
    es_port = os.environ.get("ELASTICSEARCH_PORT", 9200)
    kbase_endpoint = os.environ.get('KBASE_ENDPOINT', 'https://ci.kbase.us/services').strip('/')
    workspace_url = os.environ.get('WS_URL', kbase_endpoint + '/ws')
    catalog_url = os.environ.get('CATALOG_URL', kbase_endpoint + '/catalog')
    re_api_url = os.environ.get('RE_URL', kbase_endpoint + '/relation_engine_api').strip('/')
    config_url = os.environ.get(
        'GLOBAL_CONFIG_URL',
        'https://github.com/kbase/search_config/releases/latest/download/config.yaml'
    )
    # Load the global configuration release (non-environment specific, public config)
    if not config_url.startswith('http'):
        raise RuntimeError(f"Invalid global config url: {config_url}")
    with urllib.request.urlopen(config_url) as res:  # nosec
        global_config = yaml.safe_load(res)  # type: ignore
    return {
        # All worker-group subprocess configuration
        'workers': {
            'num_es_indexers': int(os.environ.get('NUM_ES_INDEXERS', 4)),
            'num_es_writers': int(os.environ.get('NUM_ES_WRITERS', 1)),
            'num_re_importers': int(os.environ.get('NUM_RE_IMPORTERS', 4)),
        },
        'global': global_config,
        'ws_token': os.environ['WORKSPACE_TOKEN'],
        'mount_dir': os.environ['MOUNT_DIR'],
        'kbase_endpoint': kbase_endpoint,
        'catalog_url': catalog_url,
        'workspace_url': workspace_url,
        're_api_url': re_api_url,
        're_api_token': os.environ['RE_API_TOKEN'],
        'elasticsearch_host': es_host,
        'elasticsearch_port': es_port,
        'elasticsearch_url': f"http://{es_host}:{es_port}",
        'kafka_server': os.environ.get('KAFKA_SERVER', 'kafka'),
        'kafka_clientgroup': os.environ.get('KAFKA_CLIENTGROUP', 'search_indexer'),
        'error_index_name': os.environ.get('ERROR_INDEX_NAME', 'indexing_errors'),
        'elasticsearch_index_prefix': os.environ.get('ELASTICSEARCH_INDEX_PREFIX', 'search2'),
        'topics': {
            'workspace_events': os.environ.get('KAFKA_WORKSPACE_TOPIC', 'workspaceevents'),
            'admin_events': os.environ.get('KAFKA_ADMIN_TOPIC', 'indexeradminevents'),
            'elasticsearch_updates': os.environ.get('KAFKA_ES_UPDATE_TOPIC', 'elasticsearch_updates')
        },
        'config_timeout': 600,  # 10 minutes in seconds.
        'last_config_reload': time.time(),
    }
