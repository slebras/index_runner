"""
Indexer logic based on type
"""
import logging
from kbase_workspace_client import WorkspaceClient
from kbase_workspace_client.exceptions import WorkspaceResponseError

from src.utils.config import config
from src.utils import ws_utils
from src.index_runner.es_indexers import indexer_utils
from src.index_runner.es_indexers.narrative import index_narrative
from src.index_runner.es_indexers.reads import index_reads
from src.index_runner.es_indexers.genome import index_genome
from src.index_runner.es_indexers.assembly import index_assembly
from src.index_runner.es_indexers.tree import index_tree
from src.index_runner.es_indexers.taxon import index_taxon
from src.index_runner.es_indexers.pangenome import index_pangenome
from src.index_runner.es_indexers.from_sdk import index_from_sdk
from src.index_runner.es_indexers.annotated_metagenome_assembly import index_annotated_metagenome_assembly
from src.index_runner.es_indexers.sample_set import index_sample_set
from src.utils.get_upa_from_msg import get_upa_from_msg_data

logger = logging.getLogger('IR')
ws_client = WorkspaceClient(url=config()['kbase_endpoint'], token=config()['ws_token'])


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
    if (type_module + '.' + type_name) in _TYPE_BLACKLIST:
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
        obj_data_v1 = ws_client.admin_req('getObjects', {
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
            if config()['allow_indices'] and indexer_ret.get('index') not in config()['allow_indices']:
                # This index name is not in the indexing whitelist from the config, so we skip
                logger.debug(f"Index '{indexer_ret['index']}' is not in ALLOW_INDICES, skipping")
                continue
            if indexer_ret.get('index') in config()['skip_indices']:
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

# All types we don't want to index
_TYPE_BLACKLIST = [
    "KBaseExperiments.AmpliconSet",
    "KBaseExperiments.AttributeMapping",
    "KBaseExperiments.ClusterSet",
    "KBaseExperiments.CorrelationMatrix",
    "KBaseExperiments.Network",
    "KBaseExperiments.PCAMatrix",
    "KBaseGwasData.Associations",
    "KBaseGwasData.Variations",
    "KBaseGenomes.ContigSet",
    "KBaseGenomes.Feature",
    "KBaseGenomes.GenomeComparison",
    "KBaseGenomes.GenomeDomainData",
    "KBaseClassifier.GenomeCategorizer",
    "KBaseClassifier.GenomeClassifier",
    "KBaseClassifier.GenomeClassifierTrainingSet",
    "KBasePhenotypes.PhenotypeSet",
    "KBasePhenotypes.PhenotypeSimulationSet",
    "KBaseFeatureValues.AnalysisReport",
    "KBaseFeatureValues.DifferentialExpressionMatrix",
    "KBaseFeatureValues.EstimateKResult",
    "KBaseFeatureValues.ExpressionMatrix",
    "KBaseFeatureValues.FeatureClusters",
    "KBaseFeatureValues.SingleKnockoutFitnessMatrix",
    "KBaseGenomeAnnotations.TaxonLookup",
    "KBaseGenomeAnnotations.TaxonSet",
    "KBaseMetagenomes.BinnedContigs",
    "KBaseReport.Report",
    "DataPalette.DataPalette",
    "DataPalette.DataReference",
    "KBaseFile.AnnotationFile",
    "KBaseFile.AssemblyFile",
    "KBaseFile.FileRef",
    "KBaseRBTnSeq.Delta",
    "KBaseRBTnSeq.MappedReads",
    "KBaseRBTnSeq.Pool",
    "KBaseRBTnSeq.Strain",
    "ComparativeGenomics.DNAdiffOutput",
    "ComparativeGenomics.SeqCompOutput",
    "ComparativeGenomics.WholeGenomeAlignment",
    "KBaseNarrative.Cell",
    "KBaseNarrative.Metadata",
    "KBaseCollections.FBAModelList",
    "KBaseCollections.FBAModelSet",
    "KBaseCollections.FeatureList",
    "KBaseCollections.FeatureSet",
    "KBaseCollections.GenomeList",
    "KBaseCollections.GenomeSet",
    "KBaseAssembly.AssemblyInput",
    "KBaseAssembly.AssemblyReport",
    "KBaseAssembly.Handle",
    "KBaseAssembly.ReferenceAssembly",
    "KBaseGeneFamilies.DomainAnnotation",
    "KBaseGeneFamilies.DomainLibrary",
    "KBaseGeneFamilies.DomainModelSet",
    "Communities.Biom",
    "Communities.BiomAnnotationEntry",
    "Communities.BiomMatrix",
    "Communities.BiomMatrixEntry",
    "Communities.BiomMetagenome",
    "Communities.BiomMetagenomeEntry",
    "Communities.Collection",
    "Communities.Data",
    "Communities.DataHandle",
    "Communities.Drisee",
    "Communities.FunctionalMatrix",
    "Communities.FunctionalProfile",
    "Communities.Heatmap",
    "Communities.List",
    "Communities.Metadata",
    "Communities.Metagenome",
    "Communities.MetagenomeMatrix",
    "Communities.MetagenomeProfile",
    "Communities.MetagenomeSet",
    "Communities.MetagenomeSetElement",
    "Communities.PCoA",
    "Communities.PCoAMember",
    "Communities.Profile",
    "Communities.Project",
    "Communities.SequenceFile",
    "Communities.Statistics",
    "Communities.StatList",
    "Communities.StatMatrix",
    "Communities.StatsQC",
    "Communities.TaxonomicMatrix",
    "Communities.TaxonomicProfile",
    "KBaseSearch.Contig",
    "KBaseSearch.ContigSet",
    "KBaseSearch.Feature",
    "KBaseSearch.FeatureSet",
    "KBaseSearch.Genome",
    "KBaseSearch.GenomeSet",
    "KBaseSearch.IndividualFeature",
    "KBaseSearch.SearchFeatureSet",
    "KBaseSearch.Type2CommandConfig",
    "Empty.AHandle",
    "Empty.AType",
    "KBaseCommon.Location",
    "KBaseCommon.SourceInfo",
    "KBaseCommon.StrainInfo",
    "KBaseExpression.ExpressionPlatform",
    "KBaseExpression.ExpressionReplicateGroup",
    "KBaseExpression.ExpressionSample",
    "KBaseExpression.ExpressionSeries",
    "KBaseExpression.RNASeqDifferentialExpression",
    "KBaseExpression.RNASeqSample",
    "KBaseExpression.RNASeqSampleAlignment",
    "MAK.FloatDataTable",
    "MAK.FloatDataTableContainer",
    "MAK.MAKBicluster",
    "MAK.MAKBiclusterSet",
    "MAK.MAKInputData",
    "MAK.MAKParameters",
    "MAK.MAKResult",
    "MAK.StringDataTable",
    "MAK.StringDataTableContainer",
    "KBasePPI.Interaction",
    "KBasePPI.InteractionDataset",
    "GenomeComparison.ProteomeComparison",
    "KBaseTrees.MSA",
    "KBaseTrees.MSASet",
    "KBaseRegulation.Regulome",
    "Inferelator.GeneList",
    "Inferelator.InferelatorRunResult",
    "Cmonkey.CmonkeyRunResult",
    "BAMBI.BambiRunResult",
    "ProbabilisticAnnotation.ProbAnno",
    "ProbabilisticAnnotation.RxnProbs",
    "KBaseCommunities.Metagenome",
    "MEME.MastHit",
    "MEME.MastRunResult",
    "MEME.MemePSPM",
    "MEME.MemePSPMCollection",
    "MEME.MemeRunResult",
    "MEME.MemeSite",
    "MEME.TomtomRunResult",
    "KBaseNetworks.InteractionSet",
    "KBaseNetworks.Network",
    "KBaseSequences.SequenceSet"
]
