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
from typing import Optional

import click

from . import __version__
from .config import CorrelatorConfig
from .emitter import construct_events, create_wrapping_event, emit_events
from .parser import parse_manifest, parse_run_results


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

    # 3. Parse artifacts using CorrelatorConfig for path resolution
    config = CorrelatorConfig(
        correlator_endpoint=correlator_endpoint,
        dbt_project_dir=project_dir,
    )

    try:
        run_results = parse_run_results(str(config.get_run_results_path()))
        manifest = parse_manifest(str(config.get_manifest_path()))
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
