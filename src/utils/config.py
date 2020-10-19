from kbase_workspace_client import WorkspaceClient
from typing import Optional
import json
import os
import requests
import time
import urllib.request
import yaml

from src.utils.logger import logger

_FETCH_CONFIG_RETRIES = 5

# Defines a process-wide instance of the config.
# Alternative is passing the configuration through the entire stack, which is feasible,
# but we consider that YAGNI for now.
_CONFIG_SINGLETON = None


def _get_sample_service_url(sw_url, ss_release):
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
        ws_token = os.environ['WORKSPACE_TOKEN']
        es_host = os.environ.get("ELASTICSEARCH_HOST", 'elasticsearch')
        es_port = os.environ.get("ELASTICSEARCH_PORT", 9200)
        kbase_endpoint = os.environ.get(
            'KBASE_ENDPOINT', 'https://ci.kbase.us/services').strip('/')
        workspace_url = os.environ.get('WS_URL', kbase_endpoint + '/ws')
        catalog_url = os.environ.get('CATALOG_URL', kbase_endpoint + '/catalog')
        re_api_url = os.environ.get('RE_URL', kbase_endpoint + '/relation_engine_api').strip('/')
        sample_service_url = os.environ.get("SAMPLE_SERVICE_URL")
        if sample_service_url is None:
            service_wizard_url = os.environ.get('SW_URL', kbase_endpoint + '/service_wizard').strip('/')
            sample_service_release = os.environ.get('SAMPLE_SERVICE_RELEASE', 'dev')
            sample_service_url = _get_sample_service_url(service_wizard_url, sample_service_release)
        config_url = os.environ.get('GLOBAL_CONFIG_URL', f"file://{os.getcwd()}/spec/config.yaml")
        global_config = _fetch_global_config(config_url)
        skip_indices = _get_comma_delimited_env('SKIP_INDICES')
        allow_indices = _get_comma_delimited_env('ALLOW_INDICES')
        # Use a tempfile to indicate that the service is done booting up
        proc_ready_path = '/tmp/IR_READY'  # nosec
        # Set the indexer log messages index name from a configured index name or alias
        msg_log_index_name = os.environ.get('MSG_LOG_INDEX_NAME', 'indexer_messages')
        if msg_log_index_name in global_config['latest_versions']:
            msg_log_index_name = global_config['latest_versions'][msg_log_index_name]
        self._cfg = {
            'service_wizard_url': service_wizard_url,
            'skip_releng': os.environ.get('SKIP_RELENG'),
            'skip_features': os.environ.get('SKIP_FEATURES'),
            'skip_indices': skip_indices,
            'allow_indices': allow_indices,
            'global': global_config,
            'global_config_url': config_url,
            'ws_token': ws_token,
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
            'es_batch_writes': int(os.environ.get('ES_BATCH_WRITES', 10000)),
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
            'skip_types': _get_comma_delimited_env('SKIP_TYPES'),
            'allow_types': _get_comma_delimited_env('ALLOW_TYPES'),
            'max_handler_failures': int(os.environ.get('MAX_HANDLER_FAILURES', 3)),
            'ws_client': WorkspaceClient(url=kbase_endpoint, token=ws_token),
        }

    def __getitem__(self, key):
        return self._cfg[key]

    def __str__(self):
        return json.dumps(self._cfg, indent=2)


def _fetch_global_config(config_url):
    """
    Fetch the index_runner_spec configuration file from a URL to a yaml file.
    """
    logger.info(f'Fetching config from url: {config_url}')
    # Fetch the config directly from config_url
    with urllib.request.urlopen(config_url) as res:  # nosec
        return yaml.safe_load(res.read())


def _get_comma_delimited_env(key: str) -> Optional[set]:
    """
    Fetch a comma-delimited list of strings from an environment variable as a set.
    """
    if key not in os.environ:
        return None
    ret = set()
    for piece in os.environ.get(key, '').split(','):
        piece = piece.strip()
        if piece:
            ret.add(piece)
    return ret
