# KBase Knowledge Engine

This is a background service that listens to events from the KBase Workspace and automatically generates graph data and search indexes to enrich the workspace object data.

## Development

Start the servers:

```sh
docker-compose up
```

Run the tests (servers must be running):

```sh
make test
```

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

### Deployment

Build the image:

```sh
IMAGE_NAME=kbase/index_runner2:{VERSION} sh hooks/build
```

Push to docker hub

```sh
docker push kbase/index_runner2:{VERSION}
```

### Project anatomy

* The main process and entrypoint for the app lives in `./src/index_runner/main.py`
* The workspace events consumer is in `./src/index_runner/workspace_consumer.py`
* The elasticsearch updates consumer is in `./src/index_runner/elasticsearch_consumer.py`

## KBase Search Stack

* [Index Runner](https://github.com/kbaseIncubator/index_runner_deluxe) - Kafka consumer to construct indexes and documents.
* [Elasticsearch Writer](https://github.com/kbaseIncubator/elasticsearch_writer<Paste>) - Kafka consumer to bulk update documents in ES.
* [Search API](https://github.com/kbaseIncubator/search_api_deluxe) - HTTP API for performing search queries.
* [Search Config](https://github.com/kbaseIncubator/search_config) - Global search configuration.

## Creating a SDK Indexer application

The index_runner has the ability to use sdk applications for indexing.

All apps should use the following as input:
```
typedef structure {
	string obj_data_path;
	string ws_info_path;
	string obj_data_v1_path;
} inputParams;
```
Each of the above are paths to json files that contain relevant object data (written to disk to avoid using too much memory) and can be read/parsed directly using the python json library.

The output for the indexer applications is expected to be written to a file. Each line of the returned file corresponds to one document in elasticsearch to be indexed and should be valid json.

```python
{
	'doc': {
		# this dictionary contains all the fields you want indexed in elasticsearch
		'key1': value,
		'key2': value
	},
}
```

If an object should result in more than one elasticsearch document, the `sub_type` and `sub_id` fields must be specified for all but the 'parent' document. The `sub_type` field should be the same for all the sub objects and should only contain alphanumeric characters excluding whitespaces. The `sub_id` field, MUST be unique for each subobject. If the `sub_id` field is not unique, only one of the objects will be indexed.
```python
{
	'doc': {#...
	},
	'sub_type': "sub_type",
	'sub_id': "UNIQID123" # unique id for sub object,
}
```

The Resulting output file should look something like:
```
{"doc": {"key1": 1, "key2": "string of interest"}}
{"doc": {"sub_key1": 3, "sub_key2": "value"}, "sub_type": "feature", "sub_id": "LKJCID"}
{"doc": {"sub_key1": 6, "sub_key2": "different value"}, "sub_type": "feature", "sub_id": "OIMQME"}
```
The return value from the SDK indexer application should contain the filepath. NOTE: all other output, is not utilized. The file must be written to the scratch space of the sdk application, whose path can be accessed from `config['scratch']`.
```
{
	"filepath": "/path/to/file.json"
}
```

NOTE: The application must be registered, just as any other KBase application, in the KBase environment you would like it to be used in.

## Registering your SDK Indexer application

To Register your application with the KBase Index Runner, you must open a pull request against the [Search Config](https://github.com/kbaseIncubator/search_config). In the `config.yaml` file the `sdk_indexer_apps` section is a mapping from KBase object types (excluding version) to their appropriate sdk applications and corresponding functions within those SDK applications.

Below is an example:
```
  KBaseMatrices.MetaboliteMatrix:
    sdk_app: kbasematrices_indexer # required
    sdk_func: run_kbasematrices_indexer # required
    sub_obj_index: attribute_mapping # required for indexers that have subobjects
```
NOTE: `attribute_mapping` is the string name of the sub_obj_index for `KBaseMatrices.MetaboliteMatrix`.

You may also specify `sdk_version`, which corresponds to the version of the sdk application you would like to use.
A Elasticsearch type mapping is also required. This can be added to the `mappings` field. Several examples are available in the `config.yaml` file.
