# Configuration Guide

This document explains how `dbt-correlator` configuration works, including the priority order and how to troubleshoot
configuration issues.

## Commands

`dbt-correlator` provides three commands that wrap dbt and emit OpenLineage events:

| Command | Description                                      | Events Emitted                          |
|---------|--------------------------------------------------|-----------------------------------------|
| `test`  | Run `dbt test`, emit test results                | Lineage + dataQualityAssertions facet   |
| `run`   | Run `dbt run`, emit lineage with runtime metrics | Lineage + outputStatistics facet        |
| `build` | Run `dbt build`, emit both                       | Lineage + both facets (single runId)    |

All commands share the same configuration options.

## Configuration Sources

`dbt-correlator` accepts configuration from four sources (in priority order):

| Priority    | Source                | Example                            |
|-------------|-----------------------|------------------------------------|
| 1 (highest) | CLI arguments         | `--correlator-endpoint http://...` |
| 2           | Environment variables | `CORRELATOR_ENDPOINT=http://...`   |
| 3           | Config file           | `.dbt-correlator.yml`              |
| 4 (lowest)  | Default values        | Hardcoded in code                  |

**Rule: The most specific source wins.**

## Config File Format

Create `.dbt-correlator.yml` in your project root:

```yaml
# .dbt-correlator.yml

correlator:
  endpoint: http://localhost:8080/api/v1/lineage/events
  namespace: production
  api_key: ${CORRELATOR_API_KEY}  # Environment variable interpolation

dbt:
  project_dir: .
  profiles_dir: ~/.dbt

job:
  name: my_dbt_job  # Optional: defaults to {project_name}.{command}
```

### Minimal Example

Only `correlator.endpoint` is required:

```yaml
correlator:
  endpoint: http://localhost:8080/api/v1/lineage/events
```

### Environment Variable Interpolation

Use `${VAR_NAME}` syntax to reference environment variables:

```yaml
correlator:
  api_key: ${CORRELATOR_API_KEY}
  endpoint: ${CORRELATOR_ENDPOINT:-http://default:8080}  # Not supported - use plain ${VAR}
```

**Note:** Missing environment variables are replaced with empty string.

## Config File Discovery

When no `--config` flag is provided, `dbt-correlator` searches for config files in this order:

1. `.dbt-correlator.yml` in current working directory
2. `.dbt-correlator.yaml` in current working directory
3. `.dbt-correlator.yml` in home directory (`~/.dbt-correlator.yml`)
4. `.dbt-correlator.yaml` in home directory

The first file found is used. `.yml` takes precedence over `.yaml`.

## Priority Order Explained

### Why This Order?

```
CLI args       →  "I want THIS right now, just for this run"
    ↓
Env vars       →  "Use this for my shell session / CI pipeline"
    ↓
Config file    →  "Project-level defaults for the team"
    ↓
Defaults       →  "Sensible fallback if nothing else is set"
```

### Environment Variable Fallback Chain

For dbt-ol compatibility, some options support fallback environment variables:

```
Endpoint:  CLI arg → CORRELATOR_ENDPOINT → OPENLINEAGE_URL → Config file → Error
API Key:   CLI arg → CORRELATOR_API_KEY → OPENLINEAGE_API_KEY → Config file → None
```

This allows `dbt-correlator` to work as a drop-in replacement for `dbt-ol`.

### Real-World Example

Your team has a shared `.dbt-correlator.yml`:

```yaml
correlator:
  endpoint: http://prod-correlator:8080
  namespace: production
```

**Local development override:**

```bash
# Override just for this run
dbt-correlator test --correlator-endpoint http://localhost:8080
```

**CI pipeline override:**

```bash
# Set via env var for all commands in the pipeline
export CORRELATOR_ENDPOINT=http://staging-correlator:8080
dbt-correlator test
```

**Using dbt-ol environment variables:**

```bash
# Works with existing dbt-ol setup
export OPENLINEAGE_URL=http://localhost:8080/api/v1/lineage/events
export OPENLINEAGE_NAMESPACE=production
dbt-correlator test
```

## Configuration Options Reference

| Config File Path       | CLI Option                | Environment Variable                            | Default                     |
|------------------------|---------------------------|-------------------------------------------------|-----------------------------|
| `correlator.endpoint`  | `--correlator-endpoint`   | `CORRELATOR_ENDPOINT` or `OPENLINEAGE_URL`      | (required)                  |
| `correlator.namespace` | `--openlineage-namespace` | `OPENLINEAGE_NAMESPACE`                         | `dbt`                       |
| `correlator.api_key`   | `--correlator-api-key`    | `CORRELATOR_API_KEY` or `OPENLINEAGE_API_KEY`   | `None`                      |
| `dbt.project_dir`      | `--project-dir`           | -                                               | `.`                         |
| `dbt.profiles_dir`     | `--profiles-dir`          | -                                               | `~/.dbt`                    |
| `job.name`             | `--job-name`              | -                                               | `{project_name}.{command}`  |
| -                      | `--dataset-namespace`     | `DBT_CORRELATOR_NAMESPACE`                      | `{adapter}://{database}`    |
| -                      | `--skip-dbt-run`          | -                                               | `False`                     |

### Option Details

**`--correlator-endpoint`** (required)

OpenLineage API endpoint URL. Works with Correlator or any OpenLineage-compatible backend.

**`--openlineage-namespace`**

Job namespace for OpenLineage events. This identifies the job, not the dataset.

**`--dataset-namespace`**

Override the dataset namespace. By default, `dbt-correlator` derives the namespace from the dbt adapter
as `{adapter}://{database}` (e.g., `duckdb://jaffle_shop`). Use this option for strict OpenLineage compliance
or when your backend expects a specific namespace format.

**`--job-name`**

Override the job name. By default, derived from manifest as `{project_name}.{command}`
(e.g., `jaffle_shop.test`, `jaffle_shop.run`, `jaffle_shop.build`).

**`--skip-dbt-run`**

Skip dbt command execution and use existing artifacts. Useful for testing or re-emitting events.

## Running Alongside dbt-ol

If you want to try `dbt-correlator` without changing your existing `dbt-ol` setup, you can run both tools
side by side. The key insight: `dbt-correlator` has its own environment variables that don't conflict
with `dbt-ol`.

### Why This Works

| Tool            | Primary Env Vars                                  |
|-----------------|---------------------------------------------------|
| `dbt-ol`        | `OPENLINEAGE_URL`, `OPENLINEAGE_API_KEY`          |
| `dbt-correlator`| `CORRELATOR_ENDPOINT`, `CORRELATOR_API_KEY`       |

Since these are different variable names, both tools can coexist with independent configurations.

### Side-by-Side Setup

Keep your existing `dbt-ol` environment unchanged, and configure `dbt-correlator` separately:

```bash
# Your existing dbt-ol setup (unchanged)
export OPENLINEAGE_URL=http://prod-openlineage:8080/api/v1/lineage
export OPENLINEAGE_NAMESPACE=production
export OPENLINEAGE_API_KEY=your-prod-key

# Add dbt-correlator pointing to a different backend (e.g., Correlator)
export CORRELATOR_ENDPOINT=http://correlator:8080/api/v1/lineage/events
export CORRELATOR_API_KEY=your-correlator-key

# Both tools work independently
dbt-ol run                  # Sends to prod-openlineage (your existing setup)
dbt-correlator test         # Sends to correlator (new setup)
```

### Testing dbt-correlator Without Risk

This approach lets you:

1. **Evaluate without disruption** - Your production `dbt-ol` pipeline continues unchanged
2. **Compare outputs** - Run both tools and compare the events they emit
3. **Gradual rollout** - Start with non-critical pipelines before full migration
4. **Easy rollback** - If issues arise, just stop using `dbt-correlator`

### CI Pipeline Example

```yaml
# .github/workflows/dbt.yml
env:
  # Existing dbt-ol config (production)
  OPENLINEAGE_URL: ${{ secrets.OPENLINEAGE_URL }}
  OPENLINEAGE_API_KEY: ${{ secrets.OPENLINEAGE_API_KEY }}
  OPENLINEAGE_NAMESPACE: production

  # New dbt-correlator config (evaluation)
  CORRELATOR_ENDPOINT: ${{ secrets.CORRELATOR_ENDPOINT }}
  CORRELATOR_API_KEY: ${{ secrets.CORRELATOR_API_KEY }}

jobs:
  dbt-run:
    steps:
      - name: Run dbt with dbt-ol (existing)
        run: dbt-ol run

      - name: Run dbt with dbt-correlator (evaluation)
        run: dbt-correlator run --skip-dbt-run  # Reuse artifacts from dbt-ol run
```

**Note:** The `--skip-dbt-run` flag reuses existing dbt artifacts, so you don't run dbt twice.

## Migrating from dbt-ol

If you're migrating from `dbt-ol` to `dbt-correlator`:

### Environment Variables

| dbt-ol Variable        | dbt-correlator Equivalent | Notes                           |
|------------------------|---------------------------|---------------------------------|
| `OPENLINEAGE_URL`      | `CORRELATOR_ENDPOINT`     | Both work, Correlator preferred |
| `OPENLINEAGE_API_KEY`  | `CORRELATOR_API_KEY`      | Both work, Correlator preferred |
| `OPENLINEAGE_NAMESPACE`| `OPENLINEAGE_NAMESPACE`   | Same variable, same behavior    |

### Migration Steps

1. **No changes required** - Your existing `OPENLINEAGE_*` env vars work out of the box
2. **Optionally rename** - Use `CORRELATOR_*` vars for clarity (takes precedence)
3. **Replace command** - Change `dbt-ol run` to `dbt-correlator run`

**Before (dbt-ol):**

```bash
export OPENLINEAGE_URL=http://localhost:8080/api/v1/lineage/events
export OPENLINEAGE_NAMESPACE=production
dbt-ol run
```

**After (dbt-correlator):**

```bash
export OPENLINEAGE_URL=http://localhost:8080/api/v1/lineage/events
export OPENLINEAGE_NAMESPACE=production
dbt-correlator run  # Just change the command
```

## How It Works Internally

Understanding the implementation helps with troubleshooting.

### Click's Priority Mechanism

The CLI uses [Click](https://click.palletsprojects.com/), which has built-in support for:

- CLI arguments (highest priority)
- Environment variables (via `envvar=` parameter)
- Default values (via `default=` parameter)

Config file support is added via Click's `default_map` mechanism.

### The Flow

```
User runs: dbt-correlator test

┌─────────────────────────────────────────────────────────────┐
│  1. --config callback runs first (is_eager=True)            │
│     └─→ Loads .dbt-correlator.yml (if exists)               │
│     └─→ Sets ctx.default_map = {"correlator_endpoint": ...} │
│                                                             │
│  2. Each option is processed by Click:                      │
│     └─→ CLI arg provided?        → Use it, stop             │
│     └─→ Env var set?             → Use it, stop             │
│     └─→ In ctx.default_map?      → Use it, stop (config!)   │
│     └─→ Use default= value       → Fallback                 │
│                                                             │
│  3. For endpoint/api_key, additional fallback check:        │
│     └─→ resolve_credentials() checks OPENLINEAGE_* vars     │
└─────────────────────────────────────────────────────────────┘
```

### Key Code Locations

| File        | Location                            | Purpose                              |
|-------------|-------------------------------------|--------------------------------------|
| `cli.py`    | `load_config_callback()`            | Loads config file into `default_map` |
| `cli.py`    | `resolve_credentials()`             | Handles dbt-ol env var fallbacks     |
| `cli.py`    | `@click.option(..., is_eager=True)` | Ensures config loads first           |
| `cli.py`    | `@click.option(..., envvar=...)`    | Enables env var support              |
| `config.py` | `CONFIG_TO_CLI_MAPPING`             | Maps config keys to CLI option names |
| `config.py` | `load_yaml_config()`                | File discovery and YAML parsing      |

## Troubleshooting

### Debug Mode

Add debug output to see resolved configuration:

```python
# In cli.py load_config_callback(), temporarily add:
click.echo(f"DEBUG: Loaded config from {config_path}")
click.echo(f"DEBUG: default_map = {default_map}")
```

### Common Issues

| Symptom                    | Likely Cause                  | Solution                                     |
|----------------------------|-------------------------------|----------------------------------------------|
| Config file ignored        | File not in expected location | Check cwd, use `--config` explicitly         |
| Env var not working        | Wrong variable name           | Check `CORRELATOR_ENDPOINT` (exact case)     |
| CLI doesn't override       | Typo in option name           | Check `--correlator-endpoint` spelling       |
| `${VAR}` not expanded      | Env var not set               | Export the variable first                    |
| Wrong file loaded          | Multiple config files exist   | Use `--config` to specify exact file         |
| dbt-ol vars not working    | Correlator vars take priority | Unset `CORRELATOR_*` vars if using dbt-ol    |
| Wrong dataset namespace    | Adapter detection failed      | Use `--dataset-namespace` to override        |

### Verify Configuration

To see what configuration will be used:

```bash
# Check if config file exists
ls -la .dbt-correlator.yml

# Check environment variables
env | grep -E 'CORRELATOR|OPENLINEAGE'

# Run with explicit config
dbt-correlator test --config .dbt-correlator.yml --help
```

## Best Practices

1. **Commit config file to repo** - Use `.dbt-correlator.yml` for team defaults
2. **Use env vars for secrets** - Never commit `api_key` values, use `${CORRELATOR_API_KEY}`
3. **Use CLI for one-off overrides** - Don't modify config file for temporary changes
4. **Use `.yml` extension** - It takes precedence and is more common
5. **Prefer Correlator env vars** - Use `CORRELATOR_*` over `OPENLINEAGE_*` for clarity
6. **Let job name auto-derive** - The default `{project_name}.{command}` is usually best
