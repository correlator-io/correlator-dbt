# Configuration Guide

This document explains how `dbt-correlator` configuration works, including the priority order and how to troubleshoot
configuration issues.

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
  name: dbt_test_run
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

## Configuration Options Reference

| Config File Path       | CLI Option                | Environment Variable    | Default        |
|------------------------|---------------------------|-------------------------|----------------|
| `correlator.endpoint`  | `--correlator-endpoint`   | `CORRELATOR_ENDPOINT`   | (required)     |
| `correlator.namespace` | `--openlineage-namespace` | `OPENLINEAGE_NAMESPACE` | `dbt`          |
| `correlator.api_key`   | `--correlator-api-key`    | `CORRELATOR_API_KEY`    | `None`         |
| `dbt.project_dir`      | `--project-dir`           | -                       | `.`            |
| `dbt.profiles_dir`     | `--profiles-dir`          | -                       | `~/.dbt`       |
| `job.name`             | `--job-name`              | -                       | `dbt_test_run` |

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
└─────────────────────────────────────────────────────────────┘
```

### Key Code Locations

| File        | Location                            | Purpose                              |
|-------------|-------------------------------------|--------------------------------------|
| `cli.py`    | `load_config_callback()`            | Loads config file into `default_map` |
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

| Symptom               | Likely Cause                  | Solution                                 |
|-----------------------|-------------------------------|------------------------------------------|
| Config file ignored   | File not in expected location | Check cwd, use `--config` explicitly     |
| Env var not working   | Wrong variable name           | Check `CORRELATOR_ENDPOINT` (exact case) |
| CLI doesn't override  | Typo in option name           | Check `--correlator-endpoint` spelling   |
| `${VAR}` not expanded | Env var not set               | Export the variable first                |
| Wrong file loaded     | Multiple config files exist   | Use `--config` to specify exact file     |

### Verify Configuration

To see what configuration will be used:

```bash
# Check if config file exists
ls -la .dbt-correlator.yml

# Check environment variables
env | grep CORRELATOR

# Run with explicit config
dbt-correlator test --config .dbt-correlator.yml --help
```

## Best Practices

1. **Commit config file to repo** - Use `.dbt-correlator.yml` for team defaults
2. **Use env vars for secrets** - Never commit `api_key` values, use `${CORRELATOR_API_KEY}`
3. **Use CLI for one-off overrides** - Don't modify config file for temporary changes
4. **Use `.yml` extension** - It takes precedence and is more common
