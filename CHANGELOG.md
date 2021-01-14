# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.9.14] - 2021-01-12
### Fixed
- Bugfixes related to setting default field values for new ES docs.

## [1.9.13] - 2021-01-05
### Added
- Sample relation engine indexer. indexes based on SampleSet.
- introducing bug fixes

## [1.9.12] - 2020-12-03
### Added
- Centralize the configuration for Elasticsearch indexer modules into
  `spec/elasticsearch_modules.yaml`

## [1.9.11] - 2020-11-23
### Fixed
- Fix the permissions errors associated with copying a SampleSet object in a narrative. Updates how default fields are merged on index

## [1.9.10] - 2020-11-02
### Added
- Save the index runner app version in every Elasticsearch document we save

### Fixed
- Fix the signal handlers in the main index runner process

## [1.9.9] - 2020-10-08
### Changed
- Reindex the narrative (if present) on any indexing of an object
- Testing now uses CI as an effort to migrate away from server response mocks

## [1.9.8] - 2020-09-16
### Changed/Fixed
- Removing AMA information from config and adding ama_config file.
- AMA features are now only indexed as versioned objects.

## [1.9.7] - 2020-09-16
### Changed
- Added `sample_set`, `sample_set_version`, and sample indices to config.yaml
- Updating the sample indexer to include support for multiple source WS objects

### Fixed
- Fixed a bug in a bulk update Elasticsearch request function

## [1.9.5] - 2020-09-14
### Changed
- Moved the workspace type blacklist into spec/config.yaml
- RE importer now checks against the type blacklist as well

## [1.9.5] - 2020-09-14
### Changed
- Default indexes will automatically create an alias to `default_search`

## [1.9.4] - 2020-09-11
### Changed
- Skip indexing of temporary narratives

## [1.9.3] - 2020-09-08
### Fixed
- Updated configuration aliases to include most recent indexes under `default_search`

## [1.9.2] - 2020-09-03
### Changed
- No longer copying publication title/author to agg_fields for genome_2
- Add more thorough spec validation and testing

### Fixed
- Fix some latest version alias names in the spec
- Fix a typo in the spec

## [1.9.1] - 2020-08-25
### Changed
- Using Github Actions for CI instead of Travis
- Added docker build and deployment to the github action

## [1.9.0] - 2020-08-24
### Fixed
- Fetch the object type from the workspace when it is not provided by the kafka message

### Changed
- Clean up logger and WorkspaceClient imports and initialization

### Added
- Index static narrative data from the workspace info

## [1.8.1] - 2020-08-12
### Added
- Add workspace type whitelist/blacklist options (`ALLOW_TYPES` and `SKIP_TYPES`)
- Add unit testing setup with pytest
- Add a failure count limit in the consumer
- Move the `config.yaml` from `index_runner_spec` to `spec/config.yaml`
- Centralize app version under `VERSION`

### Changed
- Increase batch write size and make configurable
- Consumer commits using the current message offset/partition
- Docs updated

### Fixed
- Fixed permissions errors for sample sets

## [1.5.7] - 2020-07-24
### Added
- Sample and SampleSet indexers

## [1.5.6] - 2020-05-04
### Fixed
- Prevent crash on workspace errors in the admin CLI
