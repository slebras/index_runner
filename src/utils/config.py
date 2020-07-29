import urllib.request
import yaml
import os
import json
import time
import logging
import requests

logger = logging.getLogger('IR')
_FETCH_CONFIG_RETRIES = 5

# Defines a process-wide instance of the config.
# Alternative is passing the configuration through the entire stack, which is feasible,
# but we consider that YAGNI for now.
_CONFIG_SINGLETON = None


def get_sample_service_url(sw_url, ss_release):
    """"""
    payload = {
        "method": "ServiceWizard.get_service_status",
        "id": '',
        "params": [{"module_name": "SampleService", "version": ss_release}],
        "version": "1.1"
    }
    headers = {'Content-Type': 'application/json'}

    sw_resp = requests.post(url=sw_url, headers=headers, data=json.dumps(payload))
    if not sw_resp.ok:
        raise RuntimeError(f"ServiceWizard error, with code {sw_resp.status_code}. \n{sw_resp.text}")
    wiz_resp = sw_resp.json()
    if wiz_resp.get('error'):
        raise RuntimeError(f"ServiceWizard {sw_url} with params"
                           f" {json.dumps(payload)} Error - " + str(wiz_resp['error']))
    return wiz_resp['result'][0]['url']


def config(force_reload=False):
    """wrapper for get config that reloads config every 'config_timeout' seconds"""
    global _CONFIG_SINGLETON
    if not _CONFIG_SINGLETON:
        # could do this on module load, but let's delay until actually requested
        _CONFIG_SINGLETON = Config()
    _CONFIG_SINGLETON.reload(force_reload=force_reload)
    return _CONFIG_SINGLETON


class Config:
    """
    A class containing the configuration for the for the search indexer. Supports dict like
    configuration item access, e.g. config['ws-token'].

    Not thread-safe.
    """

    def __init__(self):
        """Initialize configuration data from the environment."""
        self._cfg = {}
        self.reload()

    def reload(self, force_reload=False):
        """
        Reload the configuration data from the environment.

        Only reloads if the configuration has expired or force_reload is true.
        """
        if self._cfg:
            expired = (time.time() - self._cfg['last_config_reload']) > self._cfg['config_timeout']
            if not expired and not force_reload:
                # can remove force_reload once all reload logic is handled here
                return

        reqs = ['WORKSPACE_TOKEN', 'RE_API_TOKEN']
        for req in reqs:
            if not os.environ.get(req):
                raise RuntimeError(f'{req} env var is not set.')
        es_host = os.environ.get("ELASTICSEARCH_HOST", 'elasticsearch')
        es_port = os.environ.get("ELASTICSEARCH_PORT", 9200)
        kbase_endpoint = os.environ.get(
            'KBASE_ENDPOINT', 'https://ci.kbase.us/services').strip('/')
        workspace_url = os.environ.get('WS_URL', kbase_endpoint + '/ws')
        catalog_url = os.environ.get('CATALOG_URL', kbase_endpoint + '/catalog')
        re_api_url = os.environ.get('RE_URL', kbase_endpoint + '/relation_engine_api').strip('/')
        service_wizard_url = os.environ.get('SW_URL', kbase_endpoint + '/service_wizard').strip('/')
        sample_service_release = os.environ.get('SAMPLE_SERVICE_RELEASE', 'dev')
        sample_service_url = get_sample_service_url(service_wizard_url, sample_service_release)
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
        skip_indices = _get_comma_delimited_env('SKIP_INDICES')
        allow_indices = _get_comma_delimited_env('ALLOW_INDICES')
        # Use a tempfile to indicate that the service is done booting up
        proc_ready_path = '/tmp/IR_READY'  # nosec
        # Set the indexer log messages index name from a configured index name or alias
        msg_log_index_name = os.environ.get('MSG_LOG_INDEX_NAME', 'indexer_messages')
        if msg_log_index_name in global_config['latest_versions']:
            msg_log_index_name = global_config['latest_versions'][msg_log_index_name]
        self._cfg = {
            'skip_releng': os.environ.get('SKIP_RELENG'),
            'skip_features': os.environ.get('SKIP_FEATURES'),
            'skip_indices': skip_indices,
            'allow_indices': allow_indices,
            'global': global_config,
            'github_release_url': github_release_url,
            'github_token': gh_token,
            'global_config_url': config_url,
            'ws_token': os.environ['WORKSPACE_TOKEN'],
            'mount_dir': os.environ.get('MOUNT_DIR', os.getcwd()),
            'kbase_endpoint': kbase_endpoint,
            'catalog_url': catalog_url,
            'workspace_url': workspace_url,
            're_api_url': re_api_url,
            're_api_token': os.environ['RE_API_TOKEN'],
            'sample_service_url': sample_service_url,
            'elasticsearch_host': es_host,
            'elasticsearch_port': es_port,
            'elasticsearch_url': f"http://{es_host}:{es_port}",
            'kafka_server': os.environ.get('KAFKA_SERVER', 'kafka'),
            'kafka_clientgroup': os.environ.get('KAFKA_CLIENTGROUP', 'search_indexer'),
            'error_index_name': os.environ.get('ERROR_INDEX_NAME', 'indexing_errors'),
            'msg_log_index_name': msg_log_index_name,
            'elasticsearch_index_prefix': os.environ.get('ELASTICSEARCH_INDEX_PREFIX', 'search2'),
            'topics': {
                'workspace_events': os.environ.get('KAFKA_WORKSPACE_TOPIC', 'workspaceevents'),
                'admin_events': os.environ.get('KAFKA_ADMIN_TOPIC', 'indexeradminevents')
            },
            'config_timeout': 600,  # 10 minutes in seconds.
            'last_config_reload': time.time(),
            'proc_ready_path': proc_ready_path,  # File indicating the daemon is booted and ready
            'generic_shard_count': os.environ.get('GENERIC_SHARD_COUNT', 2),
            'generic_replica_count': os.environ.get('GENERIC_REPLICA_COUNT', 1),
        }

    def __getitem__(self, key):
        return self._cfg[key]


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
        tries = 0
        # Sometimes Github returns usage errors and a retry will solve it
        while True:
            release_info = requests.get(github_release_url, headers=headers).json()
            if release_info.get('assets'):
                break
            if tries == _FETCH_CONFIG_RETRIES:
                raise RuntimeError(f"Cannot fetch config from {github_release_url}: {release_info}")
            tries += 1
        for asset in release_info['assets']:
            if asset['name'] == 'config.yaml':
                download_url = asset['browser_download_url']
                with urllib.request.urlopen(download_url) as res:  # nosec
                    return yaml.safe_load(res)
        raise RuntimeError("Unable to load the config.yaml file from index_runner_spec")


def _get_comma_delimited_env(key):
    """
    Fetch a comma-delimited list of strings from an environment variable as a set.
    """
    ret = set()
    for piece in os.environ.get(key, '').split(','):
        piece = piece.strip()
        if piece:
            ret.add(piece)
    return ret
