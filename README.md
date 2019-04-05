# KBase Elasticsearch Indexer

## Development

Start the servers:

```sh
docker-compose up
```

Run the tests (servers must be running):

```sh
make test
```

### Project anatomy

* The main process and entrypoint for the app lives in `./src/index_runner/main.py`
* The workspace events consumer is in `./src/index_runner/workspace_consumer.py`
* The elasticsearch updates consumer is in `./src/index_runner/elasticsearch_consumer.py`
