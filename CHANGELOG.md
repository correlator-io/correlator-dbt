# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-01-06

First functional release of dbt-correlator. This release provides complete
integration between dbt and Correlator (or any OpenLineage-compatible backend)
for automated incident correlation.

### Added

#### CLI Commands
- `dbt-correlator test` - Run dbt test and emit OpenLineage events with test results
- `dbt-correlator run` - Run dbt run and emit lineage events with runtime metrics
- `dbt-correlator build` - Run dbt build and emit both lineage and test events

#### dbt Artifact Parsing
- Parse `run_results.json` for test execution results and model metrics
- Parse `manifest.json` for node metadata, lineage, and dataset information
- Extract model lineage (inputs/outputs) from dbt dependency graph
- Support for dbt 1.0+ artifact schemas

#### OpenLineage Event Emission
- Construct OpenLineage v2 events following the official specification
- `dataQualityAssertions` facet for test results (pass/fail per dataset)
- `outputStatistics` facet for runtime metrics (row counts when available)
- Batch emission via single HTTP POST
- Support for any OpenLineage-compatible backend

#### Configuration
- YAML config file support (`.dbt-correlator.yml`)
- Environment variable interpolation (`${VAR_NAME}` syntax)
- Configuration priority: CLI args > env vars > config file > defaults
- dbt-ol compatible env vars (`OPENLINEAGE_URL`, `OPENLINEAGE_API_KEY`)

#### Developer Experience
- `--skip-dbt-run` flag to emit events from existing artifacts
- `--dataset-namespace` override for strict OpenLineage compliance
- `--job-name` override for custom job naming
- Propagate dbt exit codes (0=success, 1=test fail, 2=compile error)
- Fire-and-forget emission (lineage failures don't affect dbt exit code)

#### Build & Packaging
- Single-source versioning via `importlib.metadata`
- Python 3.9-3.13 support
- Wheel and sdist distribution

### Changed
- CLI now executes real dbt commands (previously placeholder only)

## [0.0.1] - 2025-12-04

### Added
- Initial project structure and skeleton implementation
- CLI framework with `dbt-correlator test` command
- Configuration management (environment variables)
- Development tooling (Makefile, pre-commit hooks, CI/CD pipelines)
- Documentation (README, CONTRIBUTING, DEVELOPMENT, DEPLOYMENT)

### Note
This was a skeleton release for pipeline testing and PyPI name reservation.
All functionality returned placeholder messages.

[Unreleased]: https://github.com/correlator-io/correlator-dbt/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/correlator-io/correlator-dbt/compare/v0.0.1...v0.1.0
[0.0.1]: https://github.com/correlator-io/correlator-dbt/releases/tag/v0.0.1
