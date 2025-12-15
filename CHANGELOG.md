# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Planned
- dbt artifact parsing (run_results.json, manifest.json)
- OpenLineage event emission with dataQualityAssertions facet
- CLI implementation for `dbt test` integration
- Configuration file support (.dbt-correlator.yml)
- Integration testing with Correlator

## [0.0.1] - 2025-12-04

### Added
- Initial project structure and skeleton implementation
- CLI framework with `dbt-correlator test` command
- Configuration management (environment variables)
- Development tooling (Makefile, pre-commit hooks, CI/CD pipelines)
- Documentation (README, CONTRIBUTING, DEVELOPMENT, DEPLOYMENT)

### Note
This is a skeleton release for pipeline testing and PyPI name reservation. All functionality returns placeholder messages. Implementation begins in v0.1.0.

[Unreleased]: https://github.com/correlator-io/correlator-dbt/compare/v0.0.1...HEAD
[0.0.1]: https://github.com/correlator-io/correlator-dbt/releases/tag/v0.0.1
