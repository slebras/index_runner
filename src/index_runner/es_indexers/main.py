"""
Indexer logic based on type
"""
from kbase_workspace_client.exceptions import WorkspaceResponseError

from src.index_runner.es_indexers import indexer_utils
from src.index_runner.es_indexers.annotated_metagenome_assembly import index_annotated_metagenome_assembly
from src.index_runner.es_indexers.assembly import index_assembly
from src.index_runner.es_indexers.from_sdk import index_from_sdk
from src.index_runner.es_indexers.genome import index_genome
from src.index_runner.es_indexers.narrative import index_narrative
from src.index_runner.es_indexers.pangenome import index_pangenome
from src.index_runner.es_indexers.reads import index_reads
from src.index_runner.es_indexers.sample_set import index_sample_set
from src.index_runner.es_indexers.taxon import index_taxon
from src.index_runner.es_indexers.tree import index_tree
from src.utils import ws_utils
from src.utils.config import config
from src.utils.get_upa_from_msg import get_upa_from_msg_data
from src.utils.logger import logger


def index_obj(obj_data, ws_info, msg_data):
    """
    For a newly created object, generate the index document for it and push to
    the elasticsearch topic on Kafka.
    Args:
        obj_data - in-memory parsed data from the workspace object
        msg_data - json event data received from the kafka workspace events
            stream. Must have keys for `wsid` and `objid`
    """
    obj_type = obj_data['info'][2]
    (type_module, type_name, type_version) = ws_utils.get_type_pieces(obj_type)
    if (type_module + '.' + type_name) in config()['global']['ws_type_blacklist']:
        # Blacklisted type, so we don't index it
        return
    # check if this particular object has the tag "noindex"
    metadata = ws_info[-1]
    # If the workspace's object metadata contains a "nosearch" tag, skip it
    if metadata.get('searchtags'):
        if 'noindex' in metadata['searchtags']:
            return
    # Get the info of the first object to get the creation date of the object.
    upa = get_upa_from_msg_data(msg_data)
    try:
        obj_data_v1 = config()['ws_client'].admin_req('getObjects', {
            'objects': [{'ref': upa + '/1'}],
            'no_data': 1
        })
    except WorkspaceResponseError as err:
        logger.error('Workspace response error:', err.resp_data)
        raise err
    obj_data_v1 = obj_data_v1['data'][0]
    # Dispatch to a specific type handler to produce the search document
    indexer = _find_indexer(type_module, type_name, type_version)
    # All indexers are generators that yield document data for ES.
    defaults = indexer_utils.default_fields(obj_data, ws_info, obj_data_v1)
    for indexer_ret in indexer(obj_data, ws_info, obj_data_v1):
        if indexer_ret['_action'] == 'index':
            allow_indices = config()['allow_indices']
            skip_indices = config()['skip_indices']
            if allow_indices is not None and indexer_ret.get('index') not in allow_indices:
                # This index name is not in the indexing whitelist from the config, so we skip
                logger.debug(f"Index '{indexer_ret['index']}' is not in ALLOW_INDICES, skipping")
                continue
            if skip_indices is not None and indexer_ret.get('index') in skip_indices:
                # This index name is in the indexing blacklist in the config, so we skip
                logger.debug(f"Index '{indexer_ret['index']}' is in SKIP_INDICES, skipping")
                continue
            if '_no_defaults' not in indexer_ret:
                # Inject all default fields into the index document.
                indexer_ret['doc'].update(defaults)
        yield indexer_ret


def _find_indexer(type_module, type_name, type_version):
    """
    Find the indexer function for the given object type within the indexer_directory list.
    """
    last_match = None
    for entry in _INDEXER_DIRECTORY:
        module_match = ('module' not in entry) or entry['module'] == type_module
        name_match = ('type' not in entry) or entry['type'] == type_name
        ver_match = ('version' not in entry) or entry['version'] == type_version
        if module_match and name_match and ver_match:
            last_match = entry.get('indexer', generic_indexer())
    if last_match:
        return last_match
    # No indexer found for this type, check if there is a sdk indexer app
    if type_module + '.' + type_name in config()['global']['sdk_indexer_apps']:
        return index_from_sdk
    return generic_indexer()


def generic_indexer():
    """
    Indexes any type based on a common set of generic fields.
    """
    def fn(obj_data, ws_info, obj_data_v1):
        workspace_id = obj_data['info'][6]
        object_id = obj_data['info'][0]
        obj_type = obj_data['info'][2]
        # Send an event to the elasticsearch_writer to initialize an index for this
        # type, if it does not exist.
        yield {
            '_action': 'init_generic_index',
            'full_type_name': obj_type
        }
        obj_type_name = ws_utils.get_type_pieces(obj_type)[1]
        yield {
            '_action': 'index',
            'doc': indexer_utils.default_fields(obj_data, ws_info, obj_data_v1),
            'index': obj_type_name.lower() + "_0",
            'id': f"WS::{workspace_id}:{object_id}",
            'no_defaults': True,
            # 'namespace': "WS"
        }
    return fn


# Directory of all indexer functions.
#    If a datatype has a special deleter, can be found here as well.
# Higher up in the list gets higher precedence.
_INDEXER_DIRECTORY = [
    {'module': 'KBaseNarrative', 'type': 'Narrative', 'indexer': index_narrative},
    {'module': 'KBaseFile', 'type': 'PairedEndLibrary', 'indexer': index_reads},
    {'module': 'KBaseFile', 'type': 'SingleEndLibrary', 'indexer': index_reads},
    {'module': 'KBaseGenomeAnnotations', 'type': 'Assembly', 'indexer': index_assembly},
    {'module': 'KBaseGenomes', 'type': 'Genome', 'indexer': index_genome},
    {'module': 'KBaseTrees', 'type': 'Tree', 'indexer': index_tree},
    {'module': 'KBaseGenomeAnnotations', 'type': 'Taxon', 'indexer': index_taxon},
    {'module': 'KBaseGenomes', 'type': 'Pangenome', 'indexer': index_pangenome},
    {
        'module': 'KBaseMetagenomes',
        'type': 'AnnotatedMetagenomeAssembly',
        'indexer': index_annotated_metagenome_assembly
    },
    {'module': 'KBaseSets', 'type': 'SampleSet', 'indexer': index_sample_set}
]
