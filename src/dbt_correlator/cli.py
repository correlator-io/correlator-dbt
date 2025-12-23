"""Command-line interface for dbt-correlator.

This module provides the CLI entry point using Click framework. The CLI allows
users to run dbt commands and automatically emit OpenLineage events with test
results and lineage information for incident correlation.

Commands:
    test  - Run dbt test, emit test results with dataQualityAssertions facet
    run   - Run dbt run, emit lineage events with runtime metrics
    build - Run dbt build, emit both lineage and test results

Usage:
    $ dbt-correlator test --correlator-endpoint http://localhost:8080/api/v1/lineage/events
    $ dbt-correlator run --correlator-endpoint http://localhost:8080/api/v1/lineage/events
    $ dbt-correlator build --correlator-endpoint http://localhost:8080/api/v1/lineage/events
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
from .emitter import (
    PRODUCER,
    construct_events,
    construct_lineage_events,
    create_wrapping_event,
    emit_events,
)
from .parser import (
    extract_all_model_lineage,
    get_executed_models,
    parse_manifest,
    parse_model_results,
    parse_run_results,
)


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


def run_dbt_command(
    command: str,
    project_dir: str,
    profiles_dir: str,
    dbt_args: tuple[str, ...] = (),
) -> subprocess.CompletedProcess[bytes]:
    """Execute a dbt command as subprocess.

    Runs dbt with the specified command (test, run, build) using the
    project and profiles directories, passing through any additional
    arguments. Output is streamed directly to the console (not captured).

    Args:
        command: dbt command to run (e.g., "test", "run", "build").
        project_dir: Path to dbt project directory.
        profiles_dir: Path to dbt profiles directory.
        dbt_args: Additional arguments to pass to dbt command.

    Returns:
        CompletedProcess with returncode from dbt execution.

    Raises:
        FileNotFoundError: If dbt executable is not found in PATH.

    Example:
        >>> result = run_dbt_command("test", ".", "~/.dbt", ("--select", "my_model"))
        >>> result.returncode
        0
    """
    # Expand ~ in paths for shell-like behavior
    expanded_profiles_dir = str(Path(profiles_dir).expanduser())

    cmd = [
        "dbt",
        command,
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


def get_default_job_name(manifest: Any, command: str) -> str:
    """Get default job name matching dbt-ol convention.

    Format: {project_name}.{command}
    Example: jaffle_shop.test, jaffle_shop.run, jaffle_shop.build

    This matches dbt-ol naming convention for migration compatibility.

    Args:
        manifest: Parsed Manifest object with metadata.
        command: dbt command name (test, run, build).

    Returns:
        Job name in format "{project_name}.{command}".
        Falls back to "dbt.{command}" if project_name not found.

    Example:
        >>> manifest = parse_manifest("target/manifest.json")
        >>> get_default_job_name(manifest, "run")
        'jaffle_shop.run'
    """
    project_name = manifest.metadata.get("project_name", "dbt")
    return f"{project_name}.{command}"


def execute_test_workflow(
    project_dir: str,
    profiles_dir: str,
    correlator_endpoint: str,
    namespace: str,
    job_name: Optional[str],
    api_key: Optional[str],
    skip_dbt_run: bool,
    dbt_args: tuple[str, ...],
) -> int:
    """Execute complete test workflow with OpenLineage event emission.

    This is the main workflow function that:
    1. Parses manifest for job name (if not provided)
    2. Creates START wrapping event
    3. Runs dbt test (unless skip_dbt_run)
    4. Parses dbt artifacts
    5. Constructs test events with dataQualityAssertions
    6. Creates terminal event (COMPLETE/FAIL)
    7. Batch emits all events to OpenLineage backend
    8. Returns dbt exit code

    Args:
        project_dir: Path to dbt project directory.
        profiles_dir: Path to dbt profiles directory.
        correlator_endpoint: OpenLineage API endpoint URL.
        namespace: OpenLineage namespace (e.g., "dbt").
        job_name: Job name for OpenLineage events. If None, uses
            {project_name}.test format (dbt-ol compatible).
        api_key: Optional API key for authentication.
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
    manifest = None  # Will be parsed once and reused

    # 1. Parse manifest for job name if not provided
    # Manifest should exist from previous dbt commands (compile, run, etc.)
    if not job_name:
        try:
            manifest = parse_manifest(str(get_manifest_path(project_dir)))
            job_name = get_default_job_name(manifest, "test")
        except FileNotFoundError:
            job_name = "dbt.test"  # Fallback if no manifest yet

    # 2. Create START event
    start_timestamp = datetime.now(timezone.utc)
    start_event = create_wrapping_event(
        "START", run_id, job_name, namespace, start_timestamp
    )

    # 3. Run dbt test (unless skip)
    if not skip_dbt_run:
        try:
            result = run_dbt_command("test", project_dir, profiles_dir, dbt_args)
            dbt_exit_code = result.returncode
        except FileNotFoundError:
            click.echo(
                "Error: dbt executable not found. Please install dbt-core.",
                err=True,
            )
            return 127  # Command not found exit code

    # 4. Parse dbt artifacts (reuse manifest if already parsed for job_name)
    try:
        run_results = parse_run_results(str(get_run_results_path(project_dir)))
        if manifest is None:
            manifest = parse_manifest(str(get_manifest_path(project_dir)))
    except FileNotFoundError as e:
        click.echo(f"Error: {e}", err=True)
        return 1

    # 5. Construct test events
    test_events = construct_events(run_results, manifest, namespace, job_name, run_id)

    # 6. Create terminal event (COMPLETE or FAIL)
    terminal_timestamp = datetime.now(timezone.utc)
    terminal_type = "COMPLETE" if dbt_exit_code == 0 else "FAIL"
    terminal_event = create_wrapping_event(
        terminal_type, run_id, job_name, namespace, terminal_timestamp
    )

    # 7. Batch emit all events
    all_events = [start_event, *test_events, terminal_event]

    try:
        emit_events(all_events, correlator_endpoint, api_key)
        click.echo(f"Emitted {len(all_events)} events")
    except (ConnectionError, TimeoutError, ValueError) as e:
        click.echo(f"Warning: Failed to emit events: {e}", err=True)
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
    help="OpenLineage API endpoint URL. Works with Correlator or any OL-compatible "
    "backend (env: CORRELATOR_ENDPOINT)",
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
    default=None,
    help="Job name for OpenLineage events (default: {project_name}.test)",
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
    job_name: Optional[str],
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


def execute_run_workflow(
    project_dir: str,
    profiles_dir: str,
    correlator_endpoint: str,
    namespace: str,
    job_name: Optional[str],
    api_key: Optional[str],
    dataset_namespace: Optional[str],
    skip_dbt_run: bool,
    dbt_args: tuple[str, ...],
) -> int:
    """Execute complete run workflow with lineage emission.

    This is the main workflow function for `dbt run` that:
    1. Parses manifest for job name (if not provided)
    2. Creates START wrapping event
    3. Runs dbt run (unless skip_dbt_run)
    4. Parses dbt artifacts
    5. Extracts model lineage with runtime metrics
    6. Creates terminal event (COMPLETE/FAIL)
    7. Batch emits all events to OpenLineage backend
    8. Returns dbt exit code

    Args:
        project_dir: Path to dbt project directory.
        profiles_dir: Path to dbt profiles directory.
        correlator_endpoint: OpenLineage API endpoint URL.
        namespace: OpenLineage namespace (e.g., "dbt").
        job_name: Job name for OpenLineage events. If None, uses
            {project_name}.run format (dbt-ol compatible).
        api_key: Optional API key for authentication.
        dataset_namespace: Optional namespace override for datasets.
        skip_dbt_run: If True, skip dbt execution and use existing artifacts.
        dbt_args: Additional arguments to pass to dbt run.

    Returns:
        Exit code from dbt run (0=success, 1=error).
        If skip_dbt_run, returns 0 unless artifact parsing fails.

    Note:
        Emission failures are logged as warnings but don't affect the exit code.
        This ensures lineage is "fire-and-forget" - dbt execution is primary.
    """
    run_id = str(uuid.uuid4())
    dbt_exit_code = 0
    manifest = None  # Will be parsed once and reused

    # 1. Parse manifest for job name if not provided
    if not job_name:
        try:
            manifest = parse_manifest(str(get_manifest_path(project_dir)))
            job_name = get_default_job_name(manifest, "run")
        except FileNotFoundError:
            job_name = "dbt.run"  # Fallback if no manifest yet

    # 2. Create START event
    start_timestamp = datetime.now(timezone.utc)
    start_event = create_wrapping_event(
        "START", run_id, job_name, namespace, start_timestamp
    )

    # Emit START event immediately
    try:
        emit_events([start_event], correlator_endpoint, api_key)
    except (ConnectionError, TimeoutError, ValueError) as e:
        click.echo(f"Warning: Failed to emit START event: {e}", err=True)

    # 3. Run dbt run (unless skip)
    if not skip_dbt_run:
        try:
            result = run_dbt_command("run", project_dir, profiles_dir, dbt_args)
            dbt_exit_code = result.returncode
        except FileNotFoundError:
            click.echo(
                "Error: dbt executable not found. Please install dbt-core.",
                err=True,
            )
            return 127  # Command not found exit code

    # 4. Parse dbt artifacts (reuse manifest if already parsed for job_name)
    try:
        run_results = parse_run_results(str(get_run_results_path(project_dir)))
        if manifest is None:
            manifest = parse_manifest(str(get_manifest_path(project_dir)))
    except FileNotFoundError as e:
        click.echo(f"Error: {e}", err=True)
        return 1

    # 5. Get executed models from run_results (handles --select filtering)
    executed_models = get_executed_models(run_results)

    # 5. Extract lineage ONLY for executed models
    model_lineages = extract_all_model_lineage(
        manifest,
        model_ids=executed_models,
        namespace_override=dataset_namespace,
    )

    # 6. Parse runtime metrics from run results
    execution_results = parse_model_results(run_results)

    # 7. Construct lineage events with runtime metrics
    event_time = datetime.now(timezone.utc).isoformat()
    lineage_events = construct_lineage_events(
        model_lineages=model_lineages,
        run_id=run_id,
        job_namespace=namespace,
        producer=PRODUCER,
        event_time=event_time,
        execution_results=execution_results,
    )

    # 8. Create terminal event (COMPLETE or FAIL)
    terminal_timestamp = datetime.now(timezone.utc)
    terminal_type = "COMPLETE" if dbt_exit_code == 0 else "FAIL"
    terminal_event = create_wrapping_event(
        terminal_type, run_id, job_name, namespace, terminal_timestamp
    )

    # 9. Batch emit lineage + terminal events
    all_events = [*lineage_events, terminal_event]

    try:
        emit_events(all_events, correlator_endpoint, api_key)
        click.echo(
            f"Emitted {len(lineage_events)} lineage events + terminal event "
            f"({len(executed_models)} models)"
        )
    except (ConnectionError, TimeoutError, ValueError) as e:
        click.echo(f"Warning: Failed to emit events: {e}", err=True)
        # Don't fail - lineage is fire-and-forget

    return dbt_exit_code


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
    help="OpenLineage API endpoint URL. Works with Correlator or any OL-compatible "
    "backend (env: CORRELATOR_ENDPOINT)",
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
    default=None,
    help="Job name for OpenLineage events (default: {project_name}.run)",
    type=str,
)
@click.option(
    "--dataset-namespace",
    envvar="DBT_CORRELATOR_NAMESPACE",
    default=None,
    help="Dataset namespace override (default: {adapter}://{database})",
    type=str,
)
@click.option(
    "--skip-dbt-run",
    is_flag=True,
    default=False,
    help="Skip running dbt, only emit OpenLineage events from existing artifacts",
)
@click.argument("dbt_args", nargs=-1, type=click.UNPROCESSED)
def run(
    project_dir: str,
    profiles_dir: str,
    correlator_endpoint: str,
    openlineage_namespace: str,
    correlator_api_key: Optional[str],
    job_name: Optional[str],
    dataset_namespace: Optional[str],
    skip_dbt_run: bool,
    dbt_args: tuple[str, ...],
) -> None:
    """Run dbt run and emit OpenLineage lineage events with runtime metrics.

    This command:
        1. Runs dbt run with provided arguments
        2. Parses dbt artifacts (run_results.json, manifest.json)
        3. Extracts model lineage (inputs/outputs) for executed models
        4. Includes runtime metrics (row counts) in outputStatistics facet
        5. Emits events to OpenLineage backend for correlation

    Example:
        \b
        # Run dbt and emit lineage to OpenLineage backend
        $ dbt-correlator run --correlator-endpoint http://localhost:8080/api/v1/lineage/events

        \b
        # Pass arguments to dbt run
        $ dbt-correlator run --correlator-endpoint $CORRELATOR_ENDPOINT -- --select my_model

        \b
        # Skip dbt run, only emit from existing artifacts
        $ dbt-correlator run --skip-dbt-run --correlator-endpoint http://localhost:8080/api/v1/lineage/events

        \b
        # Use custom dataset namespace for strict OpenLineage compliance
        $ dbt-correlator run --dataset-namespace postgresql://localhost:5432/mydb
    """
    exit_code = execute_run_workflow(
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        correlator_endpoint=correlator_endpoint,
        namespace=openlineage_namespace,
        job_name=job_name,
        api_key=correlator_api_key,
        dataset_namespace=dataset_namespace,
        skip_dbt_run=skip_dbt_run,
        dbt_args=dbt_args,
    )
    sys.exit(exit_code)


def execute_build_workflow(
    project_dir: str,
    profiles_dir: str,
    correlator_endpoint: str,
    namespace: str,
    job_name: Optional[str],
    api_key: Optional[str],
    dataset_namespace: Optional[str],
    skip_dbt_run: bool,
    dbt_args: tuple[str, ...],
) -> int:
    """Execute complete build workflow with lineage + test result emission.

    This is the main workflow function for `dbt build` that:
    1. Parses manifest for job name (if not provided)
    2. Creates START wrapping event
    3. Runs dbt build (unless skip_dbt_run)
    4. Parses dbt artifacts
    5. Extracts model lineage with runtime metrics
    6. Extracts test results with dataQualityAssertions
    7. Creates terminal event (COMPLETE/FAIL)
    8. Batch emits all events to OpenLineage backend
    9. Returns dbt exit code

    The key difference from running `run` + `test` separately is that all
    events share a single runId, enabling better correlation in the backend.

    Args:
        project_dir: Path to dbt project directory.
        profiles_dir: Path to dbt profiles directory.
        correlator_endpoint: OpenLineage API endpoint URL.
        namespace: OpenLineage namespace (e.g., "dbt").
        job_name: Job name for OpenLineage events. If None, uses
            {project_name}.build format (dbt-ol compatible).
        api_key: Optional API key for authentication.
        dataset_namespace: Optional namespace override for datasets.
        skip_dbt_run: If True, skip dbt execution and use existing artifacts.
        dbt_args: Additional arguments to pass to dbt build.

    Returns:
        Exit code from dbt build (0=success, 1=test failures, 2=error).
        If skip_dbt_run, returns 0 unless artifact parsing fails.

    Note:
        Emission failures are logged as warnings but don't affect the exit code.
        This ensures lineage is "fire-and-forget" - dbt execution is primary.
    """
    run_id = str(uuid.uuid4())
    dbt_exit_code = 0
    manifest = None  # Will be parsed once and reused

    # 1. Parse manifest for job name if not provided
    if not job_name:
        try:
            manifest = parse_manifest(str(get_manifest_path(project_dir)))
            job_name = get_default_job_name(manifest, "build")
        except FileNotFoundError:
            job_name = "dbt.build"  # Fallback if no manifest yet

    # 2. Create START event
    start_timestamp = datetime.now(timezone.utc)
    start_event = create_wrapping_event(
        "START", run_id, job_name, namespace, start_timestamp
    )

    # Emit START event immediately
    try:
        emit_events([start_event], correlator_endpoint, api_key)
    except (ConnectionError, TimeoutError, ValueError) as e:
        click.echo(f"Warning: Failed to emit START event: {e}", err=True)

    # 3. Run dbt build (unless skip)
    if not skip_dbt_run:
        try:
            result = run_dbt_command("build", project_dir, profiles_dir, dbt_args)
            dbt_exit_code = result.returncode
        except FileNotFoundError:
            click.echo(
                "Error: dbt executable not found. Please install dbt-core.",
                err=True,
            )
            return 127  # Command not found exit code

    # 4. Parse dbt artifacts (reuse manifest if already parsed for job_name)
    try:
        run_results = parse_run_results(str(get_run_results_path(project_dir)))
        if manifest is None:
            manifest = parse_manifest(str(get_manifest_path(project_dir)))
    except FileNotFoundError as e:
        click.echo(f"Error: {e}", err=True)
        return 1

    # 5. Get executed models from run_results (handles --select filtering)
    executed_models = get_executed_models(run_results)

    # 5. Extract lineage ONLY for executed models
    model_lineages = extract_all_model_lineage(
        manifest,
        model_ids=executed_models,
        namespace_override=dataset_namespace,
    )

    # 6. Parse runtime metrics from run results
    execution_results = parse_model_results(run_results)

    # 7. Construct lineage events with runtime metrics
    event_time = datetime.now(timezone.utc).isoformat()
    lineage_events = construct_lineage_events(
        model_lineages=model_lineages,
        run_id=run_id,
        job_namespace=namespace,
        producer=PRODUCER,
        event_time=event_time,
        execution_results=execution_results,
    )

    # 8. Construct test events with dataQualityAssertions
    test_events = construct_events(
        run_results=run_results,
        manifest=manifest,
        namespace=namespace,
        job_name=job_name,
        run_id=run_id,
    )
    # Remove wrapping events from test_events (we'll add our own terminal event)
    # construct_events returns [test_event] (single event, no wrapping)
    # So we can use them directly

    # 9. Create terminal event (COMPLETE or FAIL)
    terminal_timestamp = datetime.now(timezone.utc)
    terminal_type = "COMPLETE" if dbt_exit_code == 0 else "FAIL"
    terminal_event = create_wrapping_event(
        terminal_type, run_id, job_name, namespace, terminal_timestamp
    )

    # 10. Batch emit all events: lineage + tests + terminal
    all_events = [*lineage_events, *test_events, terminal_event]

    try:
        emit_events(all_events, correlator_endpoint, api_key)
        click.echo(
            f"Emitted {len(lineage_events)} lineage events + "
            f"{len(test_events)} test events + terminal event "
            f"({len(executed_models)} models)"
        )
    except (ConnectionError, TimeoutError, ValueError) as e:
        click.echo(f"Warning: Failed to emit events: {e}", err=True)
        # Don't fail - lineage is fire-and-forget

    return dbt_exit_code


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
    help="OpenLineage API endpoint URL. Works with Correlator or any OL-compatible "
    "backend (env: CORRELATOR_ENDPOINT)",
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
    default=None,
    help="Job name for OpenLineage events (default: {project_name}.build)",
    type=str,
)
@click.option(
    "--dataset-namespace",
    envvar="DBT_CORRELATOR_NAMESPACE",
    default=None,
    help="Dataset namespace override (default: {adapter}://{database})",
    type=str,
)
@click.option(
    "--skip-dbt-run",
    is_flag=True,
    default=False,
    help="Skip running dbt, only emit OpenLineage events from existing artifacts",
)
@click.argument("dbt_args", nargs=-1, type=click.UNPROCESSED)
def build(
    project_dir: str,
    profiles_dir: str,
    correlator_endpoint: str,
    openlineage_namespace: str,
    correlator_api_key: Optional[str],
    job_name: Optional[str],
    dataset_namespace: Optional[str],
    skip_dbt_run: bool,
    dbt_args: tuple[str, ...],
) -> None:
    """Run dbt build and emit both lineage events and test results.

    This command combines `dbt run` and `dbt test` into a single operation,
    emitting both lineage events (with runtime metrics) and test results
    (with dataQualityAssertions) under a shared runId for better correlation.

    This command:
        1. Runs dbt build with provided arguments
        2. Parses dbt artifacts (run_results.json, manifest.json)
        3. Extracts model lineage with runtime metrics (row counts)
        4. Extracts test results with dataQualityAssertions facet
        5. Emits all events to OpenLineage backend under shared runId

    Example:
        \b
        # Run dbt build and emit both lineage + test results
        $ dbt-correlator build --correlator-endpoint http://localhost:8080/api/v1/lineage/events

        \b
        # Pass arguments to dbt build
        $ dbt-correlator build --correlator-endpoint $CORRELATOR_ENDPOINT -- --select my_model

        \b
        # Skip dbt build, only emit from existing artifacts
        $ dbt-correlator build --skip-dbt-run --correlator-endpoint http://localhost:8080/api/v1/lineage/events

        \b
        # Use custom dataset namespace for strict OpenLineage compliance
        $ dbt-correlator build --dataset-namespace postgresql://localhost:5432/mydb
    """
    exit_code = execute_build_workflow(
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        correlator_endpoint=correlator_endpoint,
        namespace=openlineage_namespace,
        job_name=job_name,
        api_key=correlator_api_key,
        dataset_namespace=dataset_namespace,
        skip_dbt_run=skip_dbt_run,
        dbt_args=dbt_args,
    )
    sys.exit(exit_code)


if __name__ == "__main__":
    cli()
