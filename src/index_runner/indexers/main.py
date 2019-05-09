"""
Indexer logic based on type
"""
from kbase_workspace_client import WorkspaceClient
from kbase_workspace_client.exceptions import WorkspaceResponseError

from . import indexer_utils
from ..utils.config import get_config
from ..utils import ws_type
from .narrative import index_narrative
from .reads import index_reads
from .genome import index_genome
from .assembly import index_assembly


def index_obj(msg_data):
    """
    For a newly created object, generate the index document for it and push to
    the elasticsearch topic on Kafka.
    Args:
        msg_data - json event data received from the kafka workspace events
        stream. Must have keys for `wsid` and `objid`
    """
    # Fetch the object data from the workspace API
    upa = _get_upa_from_msg_data(msg_data)
    config = get_config()
    ws_url = config['workspace_url']
    ws_client = WorkspaceClient(url=ws_url, token=config['ws_token'])
    try:
        obj_data = ws_client.admin_req('getObjects', {
            'objects': [{'ref': upa}]
        })
    except WorkspaceResponseError as err:
        print('Workspace response error:', err.resp_data)
        raise err
    obj_data = obj_data['data'][0]
    obj_type = obj_data['info'][2]
    (type_module, type_name, type_version) = ws_type.get_pieces(obj_type)
    if type_name in _TYPE_BLACKLIST:
        # Blacklisted type, so we don't index it
        return
    try:
        ws_info = ws_client.admin_req('getWorkspaceInfo', {
            'id': msg_data['wsid']
        })
    except WorkspaceResponseError as err:
        print('Workspace response error:', err.resp_data)
        raise err
    # Get the info of the first object to get the origin creation date of the
    # object.
    try:
        obj_data_v1 = ws_client.admin_req('getObjects', {
            'objects': [{'ref': upa + '/1'}],
            'no_data': 1
        })
    except WorkspaceResponseError as err:
        print('Workspace response error:', err.resp_data)
        raise err
    obj_data_v1 = obj_data_v1['data'][0]
    # Dispatch to a specific type handler to produce the search document
    indexer = _find_indexer(type_module, type_name, type_version)
    # all indexers should be generators.
    for indexer_ret in indexer(obj_data, ws_info, obj_data_v1):
        if indexer_ret.get('no_defaults'):
            del indexer_ret['no_defaults']
        else:
            defaults = indexer_utils.default_fields(obj_data, ws_info, obj_data_v1)
            indexer_ret['doc'].update(defaults)
        yield indexer_ret


def _find_indexer(type_module, type_name, type_version):
    """
    Find the indexer function for the given object type within the indexer_directory list.
    """
    for entry in _INDEXER_DIRECTORY:
        module_match = ('module' not in entry) or entry['module'] == type_module
        name_match = ('type' not in entry) or entry['type'] == type_name
        ver_match = ('version' not in entry) or entry['version'] == type_version
        if module_match and name_match and ver_match:
            return entry['indexer']
    # No indexer found for this type
    return generic_indexer


def generic_indexer(obj_data, ws_info, obj_data_v1):
    workspace_id = obj_data['info'][6]
    version = obj_data['info'][4]
    upa = f"{workspace_id}:{version}"
    obj_type = obj_data['info'][2]
    obj_type_name = ws_type.get_pieces(obj_type)[1]
    return {
        'doc': indexer_utils.default_fields(obj_data, ws_info, obj_data_v1),
        'index': obj_type_name,
        'id': upa
    }


# Directory of all indexer functions.
# Higher up in the list gets higher precedence.
_INDEXER_DIRECTORY = [
    {'module': 'KBaseNarrative', 'type': 'Narrative', 'indexer': index_narrative},
    {'module': 'KBaseFile', 'type': 'PairedEndLibrary', 'indexer': index_reads},
    {'module': 'KBaseFile', 'type': 'SingleEndLibrary', 'indexer': index_reads},
    {'module': 'KBaseGenomeAnnotations', 'type': 'Assembly', 'indexer': index_assembly},
    {'module': 'KBaseGenomes', 'type': 'Genome', 'indexer': index_genome}
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
    "KBaseGenomeAnnotations.Taxon",
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
    "KBaseTrees.Tree",
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


def _get_upa_from_msg_data(msg_data):
    """Get the UPA workspace reference from a Kafka workspace event payload."""
    ws_id = msg_data.get('wsid')
    if not ws_id:
        raise RuntimeError(f'Event data missing the "wsid" field for workspace ID: {msg_data}')
    obj_id = msg_data.get('objid')
    if not obj_id:
        raise RuntimeError(f'Event data missing the "objid" field for object ID: {msg_data}')
    return f"{ws_id}/{obj_id}"
