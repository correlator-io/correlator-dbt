# Architecture

This document describes the internal architecture of `dbt-correlator`, a CLI tool that wraps dbt commands and emits
OpenLineage events for incident correlation.

## High-Level Architecture

```
                            ┌─────────────────────────────────────┐
                            │      cli.py (orchestrates)          │
                            │  WorkflowConfig + execute_workflow  │
                            └──────────────┬──────────────────────┘
                                           │
      ┌────────────────────────────────────┼────────────────────────────────────┐
      │                                    │                                    │
      ▼                                    ▼                                    ▼
┌───────────┐                      ┌──────────────┐                     ┌──────────────┐
│ config.py │                      │  parser.py   │                     │  emitter.py  │
│           │                      │              │                     │              │
│ YAML file │                      │ run_results  │                     │ OpenLineage  │
│ env vars  │                      │ manifest     │                     │ events       │
│ CLI args  │                      │ lineage      │                     │ HTTP POST    │
└───────────┘                      └──────────────┘                     └──────────────┘
                                           │                                    │
                                           │                                    │
                                           ▼                                    ▼
                                   ┌──────────────┐                     ┌──────────────┐
                                   │     dbt      │                     │  Correlator  │
                                   │   artifacts  │                     │   Backend    │
                                   │              │                     │              │
                                   │ target/      │                     │ /api/v1/     │
                                   │  ├ manifest  │                     │ lineage/     │
                                   │  └ results   │                     │ events       │
                                   └──────────────┘                     └──────────────┘
```

## Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              Event Flow Timeline                                    │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  1. START Event              2. dbt Execution         3. Parse Artifacts            │
│  ─────────────               ───────────────          ─────────────────             │
│  Create START event     ──►  subprocess.run()    ──►  parse_run_results()           │
│  (not emitted yet)           dbt test/run/build       parse_manifest()              │
│                                                       extract_model_lineage()       │
│                                                                                     │
│  4. Construct Events    ──►  5. Batch Emit (all events in single HTTP POST)         │
│  ──────────────────          ───────────────────────────────────────────────        │
│  construct_test_events()     emit_events([START, ...lineage/test events,            │
│  construct_lineage_events()               COMPLETE/FAIL])                           │
│  Create COMPLETE/FAIL        POST to /api/v1/lineage/events                         │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

## Module Overview

### `cli.py` - Command Line Interface

The orchestration layer that coordinates all other modules.

**Key Components:**

| Component                   | Purpose                                           |
|-----------------------------|---------------------------------------------------|
| `WorkflowConfig`            | Dataclass encapsulating all workflow parameters   |
| `execute_workflow()`        | Unified function handling test/run/build commands |
| `@click.command` decorators | CLI entry points for each dbt command             |

**Workflow Steps:**

1. Load configuration (CLI args > env vars > config file > defaults)
2. Emit START event
3. Execute dbt command via subprocess
4. Parse resulting artifacts
5. Construct OpenLineage events (lineage + test results)
6. Emit COMPLETE/FAIL event with all events in single batch

**Commands:**

- `dbt-correlator test` - Run tests, emit lineage + dataQualityAssertions
- `dbt-correlator run` - Run models, emit lineage + runtime metrics (outputStatistics)
- `dbt-correlator build` - Run both, emit combined events with single runId

---

### `parser.py` - Artifact Parser

Parses dbt artifacts to extract test results, model metadata, and lineage information.

**Key Data Classes:**

| Class                  | Purpose                                                 |
|------------------------|---------------------------------------------------------|
| `TestResult`           | Single test execution result (status, timing, failures) |
| `RunResults`           | Parsed run_results.json with metadata                   |
| `Manifest`             | Parsed manifest.json with nodes and sources             |
| `ModelLineage`         | Input/output datasets for a model                       |
| `ModelExecutionResult` | Runtime metrics (rows affected, timing)                 |
| `DatasetInfo`          | OpenLineage dataset namespace and name                  |

**Key Functions:**

```python
# Parsing
parse_run_results(path) -> RunResults
parse_manifest(path) -> Manifest

# Lineage extraction
extract_all_model_lineage(manifest, model_filter, namespace_override) -> list[ModelLineage]
build_dataset_info(node, manifest, namespace_override) -> DatasetInfo

# Test/model resolution
resolve_test_to_model_node(test_unique_id, manifest) -> str
get_models_with_tests(run_results) -> set[str]
get_executed_models(run_results) -> list[str]

# Runtime metrics
extract_model_results(run_results, manifest) -> list[ModelExecutionResult]
```

**Dataset URN Format:**

- **Namespace**: `{adapter}://{database}` (e.g., `postgres://analytics_db`)
- **Name**: `{schema}.{alias or identifier}` (e.g., `marts.dim_customers`)
- **Override**: Use `--dataset-namespace` for strict OpenLineage compliance

---

### `emitter.py` - OpenLineage Emitter

Constructs and emits OpenLineage events to the Correlator backend.

**Key Functions:**

| Function                     | Purpose                                       |
|------------------------------|-----------------------------------------------|
| `create_wrapping_event()`    | Create START/COMPLETE/FAIL lifecycle events   |
| `construct_test_events()`    | Build events with dataQualityAssertions facet |
| `construct_lineage_events()` | Build events with inputs/outputs and metrics  |
| `group_tests_by_dataset()`   | Group test results by target dataset          |
| `emit_events()`              | HTTP POST batch of events to endpoint         |

**Event Types Emitted:**

1. **Wrapping Events** (START/COMPLETE/FAIL)
    - Mark job lifecycle boundaries
    - Same `runId` across all events in a workflow

2. **Test Events** (dataQualityAssertions facet)
    - One event per dataset with tests
    - Assertions include: name, success, column (if applicable)

3. **Lineage Events** (inputs/outputs)
    - One event per model execution
    - Includes outputStatistics facet (rowCount) when available

**HTTP Behavior:**

- Single batch POST (all events in one request)
- Fire-and-forget (failures logged, don't affect dbt exit code)
- Supports API key authentication via header

---

### `config.py` - Configuration Management

Handles configuration from multiple sources with clear priority order.

**Priority Order (highest to lowest):**

1. CLI arguments
2. Environment variables
3. Config file (`.dbt-correlator.yml`)
4. Default values

**Key Functions:**

| Function                  | Purpose                            |
|---------------------------|------------------------------------|
| `load_yaml_config()`      | Load and parse YAML config file    |
| `flatten_config()`        | Convert nested YAML to flat dict   |
| `_interpolate_env_vars()` | Expand `${VAR}` patterns in values |
| `get_run_results_path()`  | Resolve path to run_results.json   |
| `get_manifest_path()`     | Resolve path to manifest.json      |

**Environment Variable Fallbacks (dbt-ol compatibility):**

- `OPENLINEAGE_URL` → `--correlator-endpoint`
- `OPENLINEAGE_API_KEY` → `--correlator-api-key`

---

### `__init__.py` - Package Entry Point

Exports the public API and version information for programmatic usage.

**Exports:**

| Export | Purpose |
|--------|---------|
| `__version__` | Package version string |
| `parse_run_results` | Parse dbt run_results.json |
| `parse_manifest` | Parse dbt manifest.json |
| `construct_test_events` | Build test events with assertions |
| `create_wrapping_event` | Build START/COMPLETE/FAIL events |
| `emit_events` | Send events to OpenLineage endpoint |
| `group_tests_by_dataset` | Group test results by dataset |

---

## OpenLineage Event Structure

### Wrapping Event (START)

```json
{
  "eventTime": "2024-01-15T10:30:00Z",
  "eventType": "START",
  "producer": "https://github.com/correlator-io/dbt-correlator/0.1.0",
  "schemaURL": "https://openlineage.io/spec/2-0-0/OpenLineage.json",
  "run": {
    "runId": "550e8400-e29b-41d4-a716-446655440000"
  },
  "job": {
    "namespace": "dbt",
    "name": "jaffle_shop.test"
  },
  "inputs": [],
  "outputs": []
}
```

### Test Event (dataQualityAssertions)

```json
{
  "eventTime": "2024-01-15T10:30:05Z",
  "eventType": "COMPLETE",
  "producer": "https://github.com/correlator-io/dbt-correlator/0.1.0",
  "schemaURL": "https://openlineage.io/spec/2-0-0/OpenLineage.json",
  "run": {
    "runId": "550e8400-e29b-41d4-a716-446655440000"
  },
  "job": {
    "namespace": "dbt",
    "name": "jaffle_shop.test"
  },
  "inputs": [
    {
      "namespace": "postgres://analytics_db",
      "name": "marts.orders",
      "inputFacets": {
        "dataQualityAssertions": {
          "_producer": "https://github.com/correlator-io/dbt-correlator/0.1.0",
          "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/DataQualityAssertionsDatasetFacet.json",
          "assertions": [
            {
              "assertion": "not_null_orders_order_id",
              "success": true,
              "column": "order_id"
            },
            {
              "assertion": "unique_orders_order_id",
              "success": true,
              "column": "order_id"
            }
          ]
        }
      }
    }
  ],
  "outputs": []
}
```

### Lineage Event (with runtime metrics)

```json
{
  "eventTime": "2024-01-15T10:30:10Z",
  "eventType": "COMPLETE",
  "producer": "https://github.com/correlator-io/dbt-correlator/0.1.0",
  "schemaURL": "https://openlineage.io/spec/2-0-0/OpenLineage.json",
  "run": {
    "runId": "550e8400-e29b-41d4-a716-446655440000"
  },
  "job": {
    "namespace": "dbt",
    "name": "jaffle_shop.run"
  },
  "inputs": [
    {
      "namespace": "postgres://analytics_db",
      "name": "staging.stg_orders"
    },
    {
      "namespace": "postgres://analytics_db",
      "name": "staging.stg_customers"
    }
  ],
  "outputs": [
    {
      "namespace": "postgres://analytics_db",
      "name": "marts.orders",
      "outputFacets": {
        "outputStatistics": {
          "_producer": "https://github.com/correlator-io/dbt-correlator/0.1.0",
          "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/OutputStatisticsOutputDatasetFacet.json",
          "rowCount": 99
        }
      }
    }
  ]
}
```

---

## Design Decisions

### 1. Batch Emission (Single HTTP POST)

**Decision:** All events emitted in a single HTTP POST request.

**Rationale:**

- 50x more efficient than dbt-ol's per-event emission
- Reduces network overhead and connection setup costs
- Atomic delivery - all events arrive together or none do
- Correlator backend optimized for batch ingestion

### 2. Fire-and-Forget Emission

**Decision:** Emission failures are logged but don't affect dbt exit code.

**Rationale:**

- dbt execution is the primary concern; lineage is secondary
- Failed emission shouldn't break CI/CD pipelines
- Users can retry emission manually if needed
- Correlator backend handles idempotency via event deduplication

### 3. Execution Wrapping Pattern

**Decision:** START event emitted before dbt runs, COMPLETE/FAIL after.

**Rationale:**

- Matches OpenLineage run lifecycle specification
- Enables accurate timing measurement
- Provides clear job boundaries in Correlator
- Compatible with dbt-ol pattern for migration

### 4. Dataset URN Format

**Decision:** `{adapter}://{database}` namespace with `{schema}.{table}` name.

**Rationale:**

- Derived directly from dbt manifest (no guessing)
- Consistent across all dbt adapters
- `--dataset-namespace` override for strict compliance
- Correlator normalizes URNs server-side

### 5. Dynamic Job Names

**Decision:** Job name defaults to `{project_name}.{command}`.

**Rationale:**

- Automatic derivation from manifest (zero config)
- Clear identification of what ran (test vs run vs build)
- Compatible with dbt-ol naming convention
- Override via `--job-name` for custom naming

### 6. dbt-ol Compatibility

**Decision:** Support `OPENLINEAGE_URL` and `OPENLINEAGE_API_KEY` env vars.

**Rationale:**

- Drop-in replacement for existing dbt-ol users
- No config changes needed for migration
- Follows principle of least surprise
- Native `CORRELATOR_*` vars take precedence

---

## File Locations

| File               | Location                |
|--------------------|-------------------------|
| Source modules     | `src/dbt_correlator/`   |
| Tests              | `tests/`                |
| Test fixtures      | `tests/fixtures/`       |
| Configuration docs | `docs/CONFIGURATION.md` |
| Development guide  | `docs/DEVELOPMENT.md`   |

---

## Related Documentation

- [Configuration Guide](CONFIGURATION.md) - All configuration options
- [Development Guide](DEVELOPMENT.md) - Setup and contributing
- [Correlator](https://github.com/correlator-io/correlator) - Backend that receives OpenLineage events
