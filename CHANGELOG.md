# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.9.1] - 2020-08-25
## Changed
- Using Github Actions for CI instead of Travis
- Added docker build and deployment to the github action

## [1.9.0] - 2020-08-24
## Fixed
- Fetch the object type from the workspace when it is not provided by the kafka message

## Changed
- Clean up logger and WorkspaceClient imports and initialization

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
