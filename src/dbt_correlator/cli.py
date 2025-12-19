"""Command-line interface for dbt-correlator.

This module provides the CLI entry point using Click framework. The CLI allows
users to run dbt tests and automatically emit OpenLineage events to correlator server with test
results for incident correlation.

Usage:
    $ dbt-correlator test --correlator-endpoint http://localhost:8080/api/v1/lineage/events
    $ dbt-correlator test --help
    $ dbt-correlator --version

The CLI follows Click conventions and integrates with the correlator ecosystem
using the same patterns as other correlator plugins.
"""

import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import click

from . import __version__
from .config import (
    CONFIG_TO_CLI_MAPPING,
    flatten_config,
    get_manifest_path,
    get_run_results_path,
    load_yaml_config,
)
from .emitter import construct_events, create_wrapping_event, emit_events
from .parser import parse_manifest, parse_run_results


def load_config_callback(
    ctx: click.Context, param: click.Parameter, value: Optional[str]
) -> Optional[str]:
    """Load config file and set defaults for unset options.

    This callback loads configuration from YAML file and sets up Click's
    default_map so that config file values are used as defaults. This enables
    the priority order: CLI args > env vars > config file > defaults.

    Args:
        ctx: Click context object.
        param: Click parameter (the --config option).
        value: Path to config file (or None for auto-discovery).

    Returns:
        The config file path for reference.

    Raises:
        click.BadParameter: If config file exists but is invalid YAML.
        click.BadParameter: If explicitly specified config file doesn't exist.
    """
    config_path = Path(value) if value else None

    # If explicit path provided and file doesn't exist, fail early
    if config_path is not None and not config_path.exists():
        raise click.BadParameter(
            f"Config file not found: {config_path}", param=param, param_hint="--config"
        )

    try:
        yaml_config = load_yaml_config(config_path)
    except ValueError as e:
        raise click.BadParameter(str(e), param=param, param_hint="--config") from e

    if yaml_config:
        # Flatten nested YAML to match Click option names
        flat_config = flatten_config(yaml_config)

        # Map flat config keys to Click option names using shared mapping
        default_map: dict[str, Any] = {}
        for config_key, click_key in CONFIG_TO_CLI_MAPPING.items():
            if config_key in flat_config:
                default_map[click_key] = flat_config[config_key]

        # Set default_map on context for this command
        # Merge existing default_map with config file values
        if ctx.default_map:
            merged = dict(ctx.default_map)
            merged.update(default_map)
            ctx.default_map = merged
        else:
            ctx.default_map = default_map

    return value


def run_dbt_test(
    project_dir: str,
    profiles_dir: str,
    dbt_args: tuple[str, ...],
) -> subprocess.CompletedProcess[bytes]:
    """Execute dbt test as subprocess.

    Runs dbt test with the specified project and profiles directories,
    passing through any additional arguments. Output is streamed directly
    to the console (not captured).

    Args:
        project_dir: Path to dbt project directory.
        profiles_dir: Path to dbt profiles directory.
        dbt_args: Additional arguments to pass to dbt test.

    Returns:
        CompletedProcess with returncode from dbt execution.

    Raises:
        FileNotFoundError: If dbt executable is not found in PATH.

    Example:
        >>> result = run_dbt_test(".", "~/.dbt", ("--select", "my_model"))
        >>> result.returncode
        0
    """
    # Expand ~ in paths for shell-like behavior
    expanded_profiles_dir = str(Path(profiles_dir).expanduser())

    cmd = [
        "dbt",
        "test",
        "--project-dir",
        project_dir,
        "--profiles-dir",
        expanded_profiles_dir,
        *dbt_args,
    ]

    try:
        # Stream output to console (no capture)
        return subprocess.run(
            cmd,
            check=False,  # Don't raise on non-zero exit
            cwd=project_dir,
        )
    except FileNotFoundError as e:
        raise FileNotFoundError(
            f"dbt executable not found. Please install dbt-core: {e}"
        ) from e


def execute_test_workflow(
    project_dir: str,
    profiles_dir: str,
    correlator_endpoint: str,
    namespace: str,
    job_name: str,
    api_key: Optional[str],
    skip_dbt_run: bool,
    dbt_args: tuple[str, ...],
) -> int:
    """Execute complete test workflow with OpenLineage event emission.

    This is the main workflow function that:
    1. Creates START wrapping event
    2. Runs dbt test (unless skip_dbt_run)
    3. Parses dbt artifacts
    4. Constructs test events with dataQualityAssertions
    5. Creates terminal event (COMPLETE/FAIL)
    6. Batch emits all events to Correlator
    7. Returns dbt exit code

    Args:
        project_dir: Path to dbt project directory.
        profiles_dir: Path to dbt profiles directory.
        correlator_endpoint: Correlator API endpoint URL.
        namespace: OpenLineage namespace (e.g., "dbt").
        job_name: Job name for OpenLineage events.
        api_key: Optional API key for Correlator authentication.
        skip_dbt_run: If True, skip dbt execution and use existing artifacts.
        dbt_args: Additional arguments to pass to dbt test.

    Returns:
        Exit code from dbt test (0=success, 1=test failures, 2=error).
        If skip_dbt_run, returns 0 unless artifact parsing fails.

    Note:
        Emission failures are logged as warnings but don't affect the exit code.
        This ensures lineage is "fire-and-forget" - dbt execution is primary.
    """
    run_id = str(uuid.uuid4())
    dbt_exit_code = 0

    # 1. Create START event
    start_timestamp = datetime.now(timezone.utc)
    start_event = create_wrapping_event(
        "START", run_id, job_name, namespace, start_timestamp
    )

    # 2. Run dbt test (unless skip)
    if not skip_dbt_run:
        try:
            result = run_dbt_test(project_dir, profiles_dir, dbt_args)
            dbt_exit_code = result.returncode
        except FileNotFoundError:
            click.echo(
                "Error: dbt executable not found. Please install dbt-core.",
                err=True,
            )
            return 127  # Command not found exit code

    # 3. Parse dbt artifacts
    try:
        run_results = parse_run_results(str(get_run_results_path(project_dir)))
        manifest = parse_manifest(str(get_manifest_path(project_dir)))
    except FileNotFoundError as e:
        click.echo(f"Error: {e}", err=True)
        return 1

    # 4. Construct test events
    test_events = construct_events(run_results, manifest, namespace, job_name, run_id)

    # 5. Create terminal event (COMPLETE or FAIL)
    terminal_timestamp = datetime.now(timezone.utc)
    terminal_type = "COMPLETE" if dbt_exit_code == 0 else "FAIL"
    terminal_event = create_wrapping_event(
        terminal_type, run_id, job_name, namespace, terminal_timestamp
    )

    # 6. Batch emit all events
    all_events = [start_event, *test_events, terminal_event]

    try:
        emit_events(all_events, correlator_endpoint, api_key)
        click.echo(f"Emitted {len(all_events)} events to Correlator")
    except (ConnectionError, TimeoutError, ValueError) as e:
        click.echo(f"Warning: Failed to emit events to Correlator: {e}", err=True)
        # Don't fail - lineage is fire-and-forget

    return dbt_exit_code


@click.group()
@click.version_option(version=__version__, prog_name="dbt-correlator")
def cli() -> None:
    """dbt-correlator: Emit dbt test results as OpenLineage events.

    Automatically connects dbt test failures to their root cause through
    automated correlation. Works with your existing OpenLineage infrastructure.

    For more information: https://github.com/correlator-io/correlator-dbt
    """
    pass


@cli.command()
@click.option(
    "--config",
    "-c",
    callback=load_config_callback,
    is_eager=True,
    expose_value=False,
    help="Path to config file (default: .dbt-correlator.yml)",
    type=click.Path(dir_okay=False),
)
@click.option(
    "--project-dir",
    default=".",
    help="Path to dbt project directory (default: current directory)",
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
)
@click.option(
    "--profiles-dir",
    default="~/.dbt",
    help="Path to dbt profiles directory (default: ~/.dbt)",
    type=click.Path(file_okay=False, dir_okay=True),
)
@click.option(
    "--correlator-endpoint",
    envvar="CORRELATOR_ENDPOINT",
    required=True,
    help="Correlator API endpoint URL (env: CORRELATOR_ENDPOINT)",
    type=str,
)
@click.option(
    "--openlineage-namespace",
    envvar="OPENLINEAGE_NAMESPACE",
    default="dbt",
    help="Namespace for OpenLineage events (default: dbt, env: OPENLINEAGE_NAMESPACE)",
    type=str,
)
@click.option(
    "--correlator-api-key",
    envvar="CORRELATOR_API_KEY",
    default=None,
    help="Optional API key for authentication (env: CORRELATOR_API_KEY)",
    type=str,
)
@click.option(
    "--job-name",
    default="dbt_test_run",
    help="Job name for OpenLineage events (default: dbt_test_run)",
    type=str,
)
@click.option(
    "--skip-dbt-run",
    is_flag=True,
    default=False,
    help="Skip running dbt test, only emit OpenLineage event from existing artifacts",
)
@click.argument("dbt_args", nargs=-1, type=click.UNPROCESSED)
def test(
    project_dir: str,
    profiles_dir: str,
    correlator_endpoint: str,
    openlineage_namespace: str,
    correlator_api_key: Optional[str],
    job_name: str,
    skip_dbt_run: bool,
    dbt_args: tuple[str, ...],
) -> None:
    """Run dbt test and emit OpenLineage events with test results.

    This command:
        1. Runs dbt test with provided arguments
        2. Parses dbt artifacts (run_results.json, manifest.json)
        3. Constructs OpenLineage event with dataQualityAssertions facet
        4. Emits event to Correlator for automated correlation

    Example:
        \b
        # Run dbt test and emit to Correlator
        $ dbt-correlator test --correlator-endpoint http://localhost:8080/api/v1/lineage/events

        \b
        # Pass arguments to dbt test
        $ dbt-correlator test --correlator-endpoint $CORRELATOR_ENDPOINT -- --select my_model

        \b
        # Skip dbt run, only emit from existing artifacts
        $ dbt-correlator test --skip-dbt-run --correlator-endpoint http://localhost:8080/api/v1/lineage/events

        \b
        # Use environment variables
        $ export CORRELATOR_ENDPOINT=http://localhost:8080/api/v1/lineage/events
        $ export OPENLINEAGE_NAMESPACE=production
        $ dbt-correlator test
    """
    exit_code = execute_test_workflow(
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        correlator_endpoint=correlator_endpoint,
        namespace=openlineage_namespace,
        job_name=job_name,
        api_key=correlator_api_key,
        skip_dbt_run=skip_dbt_run,
        dbt_args=dbt_args,
    )
    sys.exit(exit_code)


if __name__ == "__main__":
    cli()
