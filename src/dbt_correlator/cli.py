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

import os
import subprocess
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, Optional

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
    construct_lineage_events,
    construct_test_events,
    create_wrapping_event,
    emit_events,
)
from .parser import (
    extract_all_model_lineage,
    extract_model_results,
    get_executed_models,
    get_models_with_tests,
    parse_manifest,
    parse_run_results,
)


@dataclass
class WorkflowConfig:
    """Configuration for unified dbt workflow execution.

    This dataclass encapsulates all parameters needed to execute a dbt workflow
    and emit OpenLineage events. It enables a single unified workflow function
    to handle test, run, and build commands.

    OpenLineage Naming Conventions:
        **Job** (the dbt command execution):
            - namespace: User-provided via `job_namespace` (e.g., "dbt")
            - name: Derived as "{project_name}.{command}" or user-provided

        **Dataset** (the tables/views being read/written):
            - namespace: Derived from manifest as "{adapter}://{database}",
                         OR user-provided via `dataset_namespace`
            - name: Derived from node as "{schema}.{table}"

    Attributes:
        command: The dbt command to execute ("test", "run", or "build").
        project_dir: Path to dbt project directory.
        profiles_dir: Path to dbt profiles directory.
        endpoint: OpenLineage API endpoint URL.
        job_namespace: OpenLineage job namespace (e.g., "dbt", "dbt-prod").
        job_name: Optional job name override. If None, derived from manifest.
        api_key: Optional API key for authentication.
        dataset_namespace: Optional dataset namespace override for strict OL compliance.
        skip_dbt_run: If True, skip dbt execution and use existing artifacts.
        dbt_args: Additional arguments to pass to dbt command.
        emit_test_events: Whether to emit dataQualityAssertions events.
        include_runtime_metrics: Whether to include outputStatistics facet.
    """

    command: Literal["test", "run", "build"]
    project_dir: str
    profiles_dir: str
    endpoint: str
    job_namespace: str
    job_name: Optional[str] = None
    api_key: Optional[str] = None
    dataset_namespace: Optional[str] = None
    skip_dbt_run: bool = False
    dbt_args: tuple[str, ...] = ()

    # Workflow-specific flags derived from command type
    emit_test_events: bool = False
    include_runtime_metrics: bool = False

    @classmethod
    def for_test(
        cls,
        project_dir: str,
        profiles_dir: str,
        endpoint: str,
        job_namespace: str,
        job_name: Optional[str] = None,
        api_key: Optional[str] = None,
        dataset_namespace: Optional[str] = None,
        skip_dbt_run: bool = False,
        dbt_args: tuple[str, ...] = (),
    ) -> "WorkflowConfig":
        """Create configuration for dbt test workflow.

        Test workflow: emits test events, no runtime metrics.
        Uses get_models_with_tests() to filter models.
        """
        return cls(
            command="test",
            project_dir=project_dir,
            profiles_dir=profiles_dir,
            endpoint=endpoint,
            job_namespace=job_namespace,
            job_name=job_name,
            api_key=api_key,
            dataset_namespace=dataset_namespace,
            skip_dbt_run=skip_dbt_run,
            dbt_args=dbt_args,
            emit_test_events=True,
            include_runtime_metrics=False,
        )

    @classmethod
    def for_run(
        cls,
        project_dir: str,
        profiles_dir: str,
        endpoint: str,
        job_namespace: str,
        job_name: Optional[str] = None,
        api_key: Optional[str] = None,
        dataset_namespace: Optional[str] = None,
        skip_dbt_run: bool = False,
        dbt_args: tuple[str, ...] = (),
    ) -> "WorkflowConfig":
        """Create configuration for dbt run workflow.

        Run workflow: emits runtime metrics, no test events.
        Uses get_executed_models() to filter models.
        """
        return cls(
            command="run",
            project_dir=project_dir,
            profiles_dir=profiles_dir,
            endpoint=endpoint,
            job_namespace=job_namespace,
            job_name=job_name,
            api_key=api_key,
            dataset_namespace=dataset_namespace,
            skip_dbt_run=skip_dbt_run,
            dbt_args=dbt_args,
            emit_test_events=False,
            include_runtime_metrics=True,
        )

    @classmethod
    def for_build(
        cls,
        project_dir: str,
        profiles_dir: str,
        endpoint: str,
        job_namespace: str,
        job_name: Optional[str] = None,
        api_key: Optional[str] = None,
        dataset_namespace: Optional[str] = None,
        skip_dbt_run: bool = False,
        dbt_args: tuple[str, ...] = (),
    ) -> "WorkflowConfig":
        """Create configuration for dbt build workflow.

        Build workflow: emits both test events AND runtime metrics.
        Uses get_executed_models() to filter models.
        Single runId shared across all events for better correlation.
        """
        return cls(
            command="build",
            project_dir=project_dir,
            profiles_dir=profiles_dir,
            endpoint=endpoint,
            job_namespace=job_namespace,
            job_name=job_name,
            api_key=api_key,
            dataset_namespace=dataset_namespace,
            skip_dbt_run=skip_dbt_run,
            dbt_args=dbt_args,
            emit_test_events=True,
            include_runtime_metrics=True,
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


def get_endpoint_with_fallback() -> Optional[str]:
    """Get endpoint from env vars with dbt-ol compatible fallback.

    Priority: CORRELATOR_ENDPOINT > OPENLINEAGE_URL

    Returns:
        Endpoint URL or None if neither env var is set.
    """
    return os.environ.get("CORRELATOR_ENDPOINT") or os.environ.get("OPENLINEAGE_URL")


def get_api_key_with_fallback() -> Optional[str]:
    """Get API key from env vars with dbt-ol compatible fallback.

    Priority: CORRELATOR_API_KEY > OPENLINEAGE_API_KEY

    Returns:
        API key or None if neither env var is set.
    """
    return os.environ.get("CORRELATOR_API_KEY") or os.environ.get("OPENLINEAGE_API_KEY")


def resolve_credentials(
    endpoint: Optional[str],
    api_key: Optional[str],
) -> tuple[str, Optional[str]]:
    """Resolve endpoint and API key with dbt-ol compatible fallbacks.

    Applies environment variable fallbacks and validates that endpoint is set.
    This consolidates the credential resolution logic used by all CLI commands.

    Priority for endpoint: CLI arg > CORRELATOR_ENDPOINT > OPENLINEAGE_URL
    Priority for API key: CLI arg > CORRELATOR_API_KEY > OPENLINEAGE_API_KEY

    Args:
        endpoint: Endpoint from CLI option (maybe None).
        api_key: API key from CLI option (maybe None).

    Returns:
        Tuple of (resolved_endpoint, resolved_api_key).

    Raises:
        click.UsageError: If endpoint cannot be resolved from any source.

    Example:
        >>> endpoint, api_key = resolve_credentials(None, None)
        # Uses CORRELATOR_ENDPOINT or OPENLINEAGE_URL from environment
    """
    resolved_endpoint = endpoint or get_endpoint_with_fallback()
    if not resolved_endpoint:
        raise click.UsageError(
            "Missing --correlator-endpoint. "
            "Set via CLI, CORRELATOR_ENDPOINT, or OPENLINEAGE_URL env var."
        )

    resolved_api_key = api_key or get_api_key_with_fallback()
    return resolved_endpoint, resolved_api_key


def execute_workflow(config: WorkflowConfig) -> int:  # noqa: PLR0912, PLR0915
    """Execute unified dbt workflow with OpenLineage event emission.

    This is the main workflow function that handles test, run, and build
    commands. It consolidates the common workflow logic:

    1. Parses manifest for job name (if not provided)
    2. Emits START wrapping event immediately
    3. Runs dbt command (unless skip_dbt_run)
    4. Parses dbt artifacts
    5. Extracts model lineage (filtered by command type)
    6. Optionally extracts runtime metrics (run/build only)
    7. Constructs lineage events
    8. Optionally constructs test events (test/build only)
    9. Creates terminal event (COMPLETE/FAIL)
    10. Batch emits all events to OpenLineage backend
    11. Returns dbt exit code

    Args:
        config: WorkflowConfig with all workflow parameters.

    Returns:
        Exit code from dbt command (0=success, 1=test failures/error, 2=compile error).
        Returns 127 if dbt executable not found.
        If skip_dbt_run, returns 0 unless artifact parsing fails.

    Note:
        Emission failures are logged as warnings but don't affect the exit code.
        This ensures lineage is "fire-and-forget" - dbt execution is primary.
    """
    run_id = str(uuid.uuid4())
    dbt_exit_code = 0
    manifest = None  # Will be parsed once and reused

    # 1. Resolve job name (parse manifest if needed)
    job_name = config.job_name
    if not job_name:
        try:
            manifest = parse_manifest(str(get_manifest_path(config.project_dir)))
            job_name = get_default_job_name(manifest, config.command)
        except FileNotFoundError:
            job_name = f"dbt.{config.command}"  # Fallback if no manifest yet

    # 2. Create and emit START event immediately
    start_timestamp = datetime.now(timezone.utc)
    start_event = create_wrapping_event(
        "START", run_id, job_name, config.job_namespace, start_timestamp
    )
    try:
        emit_events([start_event], config.endpoint, config.api_key)
    except (ConnectionError, TimeoutError, ValueError) as e:
        click.echo(f"Warning: Failed to emit START event: {e}", err=True)

    # 3. Run dbt command (unless skip)
    if not config.skip_dbt_run:
        try:
            result = run_dbt_command(
                config.command, config.project_dir, config.profiles_dir, config.dbt_args
            )
            dbt_exit_code = result.returncode
        except FileNotFoundError:
            click.echo(
                "Error: dbt executable not found. Please install dbt-core.",
                err=True,
            )
            return 127  # Command not found exit code

    # 4. Parse dbt artifacts (reuse manifest if already parsed for job_name)
    try:
        run_results = parse_run_results(str(get_run_results_path(config.project_dir)))
        if manifest is None:
            manifest = parse_manifest(str(get_manifest_path(config.project_dir)))
    except FileNotFoundError as e:
        click.echo(f"Error: {e}", err=True)
        return 1

    # 5. Determine which models to emit lineage for
    # test command: only models that have tests executed
    # run/build commands: only models that were executed
    if config.command == "test":
        model_ids = get_models_with_tests(run_results, manifest)
    else:
        model_ids = get_executed_models(run_results)

    # 6. Extract model lineage for filtered models
    model_lineages = extract_all_model_lineage(
        manifest,
        model_ids=model_ids,
        namespace_override=config.dataset_namespace,
    )

    # 7. Extract runtime metrics (only for run/build commands)
    execution_results = None
    if config.include_runtime_metrics:
        execution_results = extract_model_results(run_results)

    # 8. Construct lineage events
    event_time = datetime.now(timezone.utc).isoformat()
    lineage_events = construct_lineage_events(
        model_lineages=model_lineages,
        run_id=run_id,
        job_namespace=config.job_namespace,
        producer=PRODUCER,
        event_time=event_time,
        execution_results=execution_results,
    )

    # 9. Construct test events (only for test/build commands)
    test_events: list[Any] = []
    if config.emit_test_events:
        test_events = construct_test_events(
            run_results=run_results,
            manifest=manifest,
            job_namespace=config.job_namespace,
            job_name=job_name,
            run_id=run_id,
        )

    # 10. Create terminal event (COMPLETE or FAIL)
    terminal_timestamp = datetime.now(timezone.utc)
    terminal_type = "COMPLETE" if dbt_exit_code == 0 else "FAIL"
    terminal_event = create_wrapping_event(
        terminal_type, run_id, job_name, config.job_namespace, terminal_timestamp
    )

    # 11. Batch emit all events: lineage + tests (if any) + terminal
    all_events = [*lineage_events, *test_events, terminal_event]

    try:
        emit_events(all_events, config.endpoint, config.api_key)
        # Build success message based on what was emitted
        parts = [f"{len(lineage_events)} lineage events"]
        if test_events:
            parts.append(f"{len(test_events)} test events")
        parts.append("terminal event")
        click.echo(f"Emitted {' + '.join(parts)} ({len(model_ids)} models)")
    except (ConnectionError, TimeoutError, ValueError) as e:
        click.echo(f"Warning: Failed to emit events: {e}", err=True)
        # Don't fail - lineage is fire-and-forget

    return dbt_exit_code


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
    # Convert paths to absolute for consistent behavior regardless of cwd
    # This ensures the command works when run from any directory
    abs_project_dir = str(Path(project_dir).resolve())
    abs_profiles_dir = str(Path(profiles_dir).expanduser().resolve())

    cmd = [
        "dbt",
        command,
        "--project-dir",
        abs_project_dir,
        "--profiles-dir",
        abs_profiles_dir,
        *dbt_args,
    ]

    try:
        # Stream output to console (no capture)
        # Use absolute project dir as cwd for dbt to find dbt_project.yml
        return subprocess.run(
            cmd,
            check=False,  # Don't raise on non-zero exit
            cwd=abs_project_dir,
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
    default=None,
    help="OpenLineage API endpoint URL. Works with Correlator or any OL-compatible "
    "backend (env: CORRELATOR_ENDPOINT or OPENLINEAGE_URL)",
    type=str,
)
@click.option(
    "--openlineage-namespace",
    envvar="OPENLINEAGE_NAMESPACE",
    default="dbt",
    help="Job namespace for OpenLineage events (default: dbt, env: OPENLINEAGE_NAMESPACE)",
    type=str,
)
@click.option(
    "--correlator-api-key",
    envvar="CORRELATOR_API_KEY",
    default=None,
    help="Optional API key for authentication (env: CORRELATOR_API_KEY or OPENLINEAGE_API_KEY)",
    type=str,
)
@click.option(
    "--job-name",
    default=None,
    help="Job name for OpenLineage events (default: {project_name}.test)",
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
    help="Skip running dbt test, only emit OpenLineage event from existing artifacts",
)
@click.argument("dbt_args", nargs=-1, type=click.UNPROCESSED)
def test(
    project_dir: str,
    profiles_dir: str,
    correlator_endpoint: Optional[str],
    openlineage_namespace: str,  # job namespace
    correlator_api_key: Optional[str],
    job_name: Optional[str],
    dataset_namespace: Optional[str],
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
        # Use environment variables (dbt-ol compatible)
        $ export OPENLINEAGE_URL=http://localhost:8080/api/v1/lineage/events
        $ export OPENLINEAGE_NAMESPACE=production
        $ dbt-correlator test
    """
    # Resolve credentials with dbt-ol compatible env var fallbacks
    endpoint, api_key = resolve_credentials(correlator_endpoint, correlator_api_key)

    config = WorkflowConfig.for_test(
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        endpoint=endpoint,
        job_namespace=openlineage_namespace,
        job_name=job_name,
        api_key=api_key,
        dataset_namespace=dataset_namespace,
        skip_dbt_run=skip_dbt_run,
        dbt_args=dbt_args,
    )
    sys.exit(execute_workflow(config))


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
    default=None,
    help="OpenLineage API endpoint URL. Works with Correlator or any OL-compatible "
    "backend (env: CORRELATOR_ENDPOINT or OPENLINEAGE_URL)",
    type=str,
)
@click.option(
    "--openlineage-namespace",
    envvar="OPENLINEAGE_NAMESPACE",
    default="dbt",
    help="Job namespace for OpenLineage events (default: dbt, env: OPENLINEAGE_NAMESPACE)",
    type=str,
)
@click.option(
    "--correlator-api-key",
    envvar="CORRELATOR_API_KEY",
    default=None,
    help="Optional API key for authentication (env: CORRELATOR_API_KEY or OPENLINEAGE_API_KEY)",
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
    correlator_endpoint: Optional[str],
    openlineage_namespace: str,  # job namespace
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
    # Resolve credentials with dbt-ol compatible env var fallbacks
    endpoint, api_key = resolve_credentials(correlator_endpoint, correlator_api_key)

    config = WorkflowConfig.for_run(
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        endpoint=endpoint,
        job_namespace=openlineage_namespace,
        job_name=job_name,
        api_key=api_key,
        dataset_namespace=dataset_namespace,
        skip_dbt_run=skip_dbt_run,
        dbt_args=dbt_args,
    )
    sys.exit(execute_workflow(config))


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
    default=None,
    help="OpenLineage API endpoint URL. Works with Correlator or any OL-compatible "
    "backend (env: CORRELATOR_ENDPOINT or OPENLINEAGE_URL)",
    type=str,
)
@click.option(
    "--openlineage-namespace",
    envvar="OPENLINEAGE_NAMESPACE",
    default="dbt",
    help="Job namespace for OpenLineage events (default: dbt, env: OPENLINEAGE_NAMESPACE)",
    type=str,
)
@click.option(
    "--correlator-api-key",
    envvar="CORRELATOR_API_KEY",
    default=None,
    help="Optional API key for authentication (env: CORRELATOR_API_KEY or OPENLINEAGE_API_KEY)",
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
    correlator_endpoint: Optional[str],
    openlineage_namespace: str,  # job namespace
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
    # Resolve credentials with dbt-ol compatible env var fallbacks
    endpoint, api_key = resolve_credentials(correlator_endpoint, correlator_api_key)

    config = WorkflowConfig.for_build(
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        endpoint=endpoint,
        job_namespace=openlineage_namespace,
        job_name=job_name,
        api_key=api_key,
        dataset_namespace=dataset_namespace,
        skip_dbt_run=skip_dbt_run,
        dbt_args=dbt_args,
    )
    sys.exit(execute_workflow(config))


if __name__ == "__main__":
    cli()
