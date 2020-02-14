import urllib.request
import yaml
import os
import time
import functools
import logging
import requests

logger = logging.getLogger('IR')


def config(force_reload=False):
    """wrapper for get config that reloads config every 'config_timeout' seconds"""
    config = get_config()
    expired = (time.time() - config['last_config_reload']) > config['config_timeout']
    if force_reload or expired:
        get_config.cache_clear()
        config = get_config()
    return config


@functools.lru_cache(maxsize=1)
def get_config():
    """Initialize configuration data from the environment."""
    reqs = ['WORKSPACE_TOKEN', 'RE_API_TOKEN']
    for req in reqs:
        if not os.environ.get(req):
            raise RuntimeError(f'{req} env var is not set.')
    es_host = os.environ.get("ELASTICSEARCH_HOST", 'elasticsearch')
    es_port = os.environ.get("ELASTICSEARCH_PORT", 9200)
    kbase_endpoint = os.environ.get('KBASE_ENDPOINT', 'https://ci.kbase.us/services').strip('/')
    workspace_url = os.environ.get('WS_URL', kbase_endpoint + '/ws')
    catalog_url = os.environ.get('CATALOG_URL', kbase_endpoint + '/catalog')
    re_api_url = os.environ.get('RE_URL', kbase_endpoint + '/relation_engine_api').strip('/')
    config_url = os.environ.get('GLOBAL_CONFIG_URL')
    github_release_url = os.environ.get(
        'GITHUB_RELEASE_URL',
        'https://api.github.com/repos/kbase/index_runner_spec/releases/latest'
    )
    # Load the global configuration release (non-environment specific, public config)
    if config_url and not config_url.startswith('http'):
        raise RuntimeError(f"Invalid global config url: {config_url}")
    if not github_release_url.startswith('http'):
        raise RuntimeError(f"Invalid global github release url: {github_release_url}")
    gh_token = os.environ.get('GITHUB_TOKEN')
    global_config = _fetch_global_config(config_url, github_release_url, gh_token)
    return {
        'skip_releng': os.environ.get('SKIP_RELENG'),
        'skip_features': os.environ.get('SKIP_FEATURES'),
        'global': global_config,
        'github_release_url': github_release_url,
        'github_token': gh_token,
        'global_config_url': config_url,
        'ws_token': os.environ['WORKSPACE_TOKEN'],
        'mount_dir': os.environ.get('MOUNT_DIR', os.getcwd()),
        'scratch': os.environ.get('SCRATCH', '/scratch'),
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
            'admin_events': os.environ.get('KAFKA_ADMIN_TOPIC', 'indexeradminevents')
        },
        'config_timeout': 600,  # 10 minutes in seconds.
        'last_config_reload': time.time(),
    }


def _fetch_global_config(config_url, github_release_url, gh_token):
    """
    Fetch the index_runner_spec configuration file from the Github release
    using either the direct URL to the file or by querying the repo's release
    info using the GITHUB API.
    """
    if config_url:
        print('Fetching config from the direct url')
        # Fetch the config directly from config_url
        with urllib.request.urlopen(config_url) as res:  # nosec
            return yaml.safe_load(res)  # type: ignore
    else:
        print('Fetching config from the release info')
        # Fetch the config url from the release info
        if gh_token:
            headers = {'Authorization': f'token {gh_token}'}
        else:
            headers = {}
        release_info = requests.get(github_release_url, headers=headers).json()
        for asset in release_info['assets']:
            if asset['name'] == 'config.yaml':
                download_url = asset['browser_download_url']
                with urllib.request.urlopen(download_url) as res:  # nosec
                    return yaml.safe_load(res)
        raise RuntimeError("Unable to load the config.yaml file from index_runner_spec")
