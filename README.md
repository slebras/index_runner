# KBase Index Runner / Knowledge Engine

This is a background service that listens to events from the KBase Workspace
and automatically generates graph data and search indexes to enrich the
workspace object data.

It consumes events from a Kafka topic and sends data into ArangoDB and
Elasticsearch. It is designed to have the service replicated in Rancher. Be
sure to partition the topic to at least the number of running workers.

## Development

Start the servers:

```sh
docker-compose up
```

Run the tests (servers need not be running, and will be shut down if they are):

```sh
scripts/run_tests
```

Other test commands:

* Run unit tests only (fast): `scripts/run_unit_tests`
* Run integration tests only: `scripts/run_integration_tests`
* Run linters only: `scripts/lint`

Note that `docker-compose run`, and therefore `scripts/run_tests`, will only
build a Docker image for the application if no image exists, and therefore
`Dockerfile` changes will not be included in any tests. To include `Dockerfile`
changes in tests, do one of the following, in order of
destructiveness:

1. `docker-compose build` will build any images with changed `Dockerfile`s using the build cache.
2. `docker-compose build --no-cache app` will rebuild the application image from scratch.
3. `scripts/docker_reset`. This will remove all test images and rebuild them from scratch.

## Config

You can set the following env vars:

* `SKIP_RELENG` - skip imports into the relation engine (ArangoDB)
* `SKIP_FEATURES` - skip any importing or indexing of genome features
* `SKIP_INDICES` - comma-separated list of index names that the service will not write into.
* `ELASTICSEARCH_HOST` - host name of the elasticsearch server to use (do not prepend protocol)
* `ELASTICSEARCH_PORT` - port to use for the elasticsearch server
* `KBASE_ENDPOINT` - URL of kbase API services (default is "https://ci.kbase.us/services")
* `WS_URL` - URL of the workspace API (default is to append "/ws" to KBASE_ENDPOINT)
* `CATALOG_URL` - URL of the catalog API (default is to append "/catalog" to KBASE_ENDPOINT)
* `RE_URL` - URL of the relation engine API (default is to append "/relation_engine_api" to KBASE_ENDPOINT)
* `GLOBAL_CONFIG_URL` - Optional URL of a specific index_runner_spec configuration file to use. Set this to use a specific config release that will not automatically update.
* `GITHUB_RELEASE_URL` - Optional URL of the latest release information for the index_runner_spec. Defaults to "https://api.github.com/repos/kbase/index_runner_spec/releases/latest". Use this setting to have the config file automatically keep up-to-date with the latest config changes. Ignored if GLOBAL_CONFIG_URL is provided.
* `GITHUB_TOKEN` - Optional Github token (https://github.com/settings/tokens) to use when fetching config updates. Avoids any Github API usage limit errors.
* `WORKSPACE_TOKEN` - Required KBase authentication token for accessing the workspace API
* `MOUNT_DIR` - Directory that can be used for local files when running SDK indexer apps (defaults to current working directory).
* `RE_API_TOKEN` - Required KBase auth token for accessing the relation engine API
* `KAFKA_SERVER` - URL of the Kafka server
* `KAFKA_CLIENTGROUP` - Name of the Kafka client group that the consumer will join
* `ERROR_INDEX_NAME` - Name of the index in which we store errors (defaults to "indexing_errors")
* `ELASTICSEARCH_INDEX_PREFIX` - Name of the prefix to use for all indexes (defaults to "search2")
* `KAFKA_WORKSPACE_TOPIC` - Name of the topic to consume workspace events from (defaults to "workspaceevents")
* `KAFKA_ADMIN_TOPIC` - Name of the topic to consume indexer admin events from (defaults to "indexeradminevents")
* `ALLOW_TYPES` - optional comma-delimited strings - list of KBase Workspace types that we want to whitelist when indexing into ES and importing into RE
* `SKIP_TYPES` - optional comma-delimited strings - list of KBase Worksapce types that we want to blacklist when indexing into ES and importing into RE
* `MAX_HANDLER_FAILURES` - optional int - Number of times our consumer can fail processing a message before it commits the offset and moves on


## Admininstration

Some CLI admin tools are provided in the `indexer_admin` script. For a running container:

```sh
docker exec container_name indexer_admin
```

Examples:

Show command help: `indexer_admin -h`

_Reindex a specific object_ 

```sh
# Reindex only if the doc does not exist
indexer_admin reindex --ref 11/22

# Reindex and overwrite any existing doc
indexer_admin reindex --ref 11/22 --overwrite
```

_Reindex a workspace_

```sh
# Reindex only objects that do not exist
indexer_admin reindex --ref 11

# Reindex and overwrite any existing docs
indexer_admin reindex --ref 11 --overwrite
```

_Reindex a range of workspaces_

```sh
# Index workspaces 1 through 10
indexer_admin reindex_range --min 1 --max 10

# Index workspaces 1 through 10, overwriting existing docs
indexer_admin reindex_range --min 1 --max 10 --overwrite

# Index from workspace 1 through 1000
indexer_admin reindex_range  --max 1000
```

_Get a count of indexing errors_

```sh
# Total error count
indexer_admin err_count

# Error count by type
indexer_admin err_count --by-type
```

_Get all the upas that had an error_

Note: this outputs ALL upas to stdout. You might want to pipe this into a file.

```sh
indexer_admin err_upas
```

_Index all objects of a certain type_

```sh
indexer_admin reindex_type --type "KBaseNarrative.Narrative-4.0"
```

### Deployment

First, increment the versions found in `VERSION` and in `pyproject.toml`.

Then, build the image and push to docker hub with `scripts/docker_deploy`.

### Project anatomy

* The main process and entrypoint for the app lives in `./src/index_runner/main.py`
* The entrypoint for ES indexing is in `./src/index_runner/es_indexer.py`
  * Local elasticsearch indexers live in `./src/index_runner/es_indexers`
* The entrypoint for Arango imports is in `./src/index_runner/releng_importer.py`
  * Type-specific RE importers are found in `./src/index_runner/releng`

## How to add a new indexer

* In `spec/config.yaml`:
  * Create an entry in `ws_type_to_indices` mapping the KBase type name to an index's alias name
  * Create an entry under `ws_subobjects`, if necessary, indicating that you are indexing additional sub-indexes (like features in a genome)
  * Create an entry under `latest_versions`, mapping an unversioned alias name to a versioned index name
  * Create an entry under `aliases` for the actual alias-to-index mapping
  * Create an entry under `mappings` with a versioned index name corresponding to the thing you are indexing. Check if you want to use any `global_mappings` here, which serve as type mapping mix-ins, pulled from the `global_mappings` section above
* Create a new module under `src/index_runner/es_indexers`
  * Add some top-level constants, such as index name, version, and prefix. If you are indexing a workspace object, use the "WS" namespace
  * Add a function that receives the workspace object data, workspace info, and the original kafka message data
  * This function should generate (with `yield`) new index documents
  * `yield` a dictionary with keys for `"index"` (index name), `"_action"` (should be "index" to create/update a document), and `"doc"` with the actual document data.
* Add your module's indexing function to the `_INDEXER_DIRECTORY` variable inside `src/index_runner/es_indexers/main.py`

## Creating an SDK indexer application

The index runner has the ability to use SDK modules for indexing. Modules
should use the following as input:

```
typedef structure {
	string obj_data_path;
	string ws_info_path;
	string obj_data_v1_path;
} inputParams;
```

Each of the above fields are paths to JSON files that contain relevant object
data (written to disk to avoid using too much memory).

The output for the indexer module is expected to be written to a file.
Each line of the returned file corresponds to one JSON document to be saved in Elasticsearch.

_Example output:_

```json
{ "doc": { "key1": "value1", "key2": "value2" } }
```

If the module outputs more than one Elasticsearch document, the `sub_type` and `sub_id` fields must be specified for all but the 'parent' document. The `sub_type` field should be the same for all the sub objects and should only contain alphanumeric characters, excluding whitespaces. The `sub_id` field MUST be unique for each subobject. If the `sub_id` field is not unique, only one of the objects will be indexed.

_Example output:_

```
{"doc": {"key1": 1, "key2": "string of interest"}}
{"doc": {"sub_key1": 3, "sub_key2": "value"}, "sub_type": "feature", "sub_id": "LKJCID"}
{"doc": {"sub_key1": 6, "sub_key2": "different value"}, "sub_type": "feature", "sub_id": "OIMQME"}
```

The output from your SDK module should contain the filepath for the above data. The file must be written to the scratch space of the sdk application, whose path can be accessed from `config['scratch']`.

```
# Example output from your SDK module's indexer method:
result = {
	"filepath": os.path.join(config['scratch'], "my_output.json")
}
```

NOTE: Before you can use an SDK module as an indexer, it must be registered in the KBase environment you are using.

### Registering your SDK indexer module

Add your entry in `spec/config.yaml` under the `sdk_indexer_apps` key.

* `sdk_app` is the name of your SDK module
* `sdk_func` is the name of your module's indexer method
* `sdk_obj_index` sub-object index name

Example:

```
  KBaseMatrices.MetaboliteMatrix:
    sdk_app: kbasematrices_indexer # required
    sdk_func: run_kbasematrices_indexer # required
    sub_obj_index: attribute_mapping # required for indexers that have subobjects
```

NOTE: `attribute_mapping` is the string name of the sub_obj_index for
`KBaseMatrices.MetaboliteMatrix`.

You may also specify `sdk_version`, which corresponds to the version of the SDK
module you would like to use.

An Elasticsearch type mapping is also required. This can be added to the
`mappings` field in the config. Several examples are available in the
`spec/config.yaml` file.
