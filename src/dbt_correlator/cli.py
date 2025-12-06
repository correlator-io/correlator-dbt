"""Command-line interface for dbt-correlator.

This module provides the CLI entry point using Click framework. The CLI allows
users to run dbt tests and automatically emit OpenLineage events with test
results for incident correlation.

Usage:
    $ dbt-correlator test --openlineage-url http://localhost:8080/api/v1/lineage
    $ dbt-correlator test --help
    $ dbt-correlator --version

The CLI follows Click conventions and integrates with the correlator ecosystem
using the same patterns as other correlator plugins.
"""

import sys
from typing import Optional

import click

from . import __version__


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
    "--openlineage-url",
    envvar="OPENLINEAGE_URL",
    required=True,
    help="OpenLineage API endpoint URL (env: OPENLINEAGE_URL)",
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
    "--openlineage-api-key",
    envvar="OPENLINEAGE_API_KEY",
    default=None,
    help="Optional API key for authentication (env: OPENLINEAGE_API_KEY)",
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
    openlineage_url: str,
    openlineage_namespace: str,
    openlineage_api_key: Optional[str],  # noqa: ARG001
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
        $ dbt-correlator test --openlineage-url http://localhost:8080/api/v1/lineage

        \b
        # Pass arguments to dbt test
        $ dbt-correlator test --openlineage-url $CORRELATOR_URL -- --select my_model

        \b
        # Skip dbt run, only emit from existing artifacts
        $ dbt-correlator test --skip-dbt-run --openlineage-url http://localhost:8080/api/v1/lineage

        \b
        # Use environment variables
        $ export OPENLINEAGE_URL=http://localhost:8080/api/v1/lineage
        $ export OPENLINEAGE_NAMESPACE=production
        $ dbt-correlator test

    Args:
        project_dir: Path to dbt project directory.
        profiles_dir: Path to dbt profiles directory.
        openlineage_url: OpenLineage API endpoint URL.
        openlineage_namespace: Namespace for OpenLineage events.
        openlineage_api_key: Optional API key for authentication.
        job_name: Job name for OpenLineage events.
        skip_dbt_run: Skip running dbt test, only emit from existing artifacts.
        dbt_args: Additional arguments passed to dbt test.

    Note:
        Implementation in Task 1.4: CLI Implementation.
        Will integrate parser, emitter, and config modules.
    """
    click.echo("🚧 dbt-correlator is under development")
    click.echo("")
    click.echo("Configuration received:")
    click.echo(f"  Project dir: {project_dir}")
    click.echo(f"  Profiles dir: {profiles_dir}")
    click.echo(f"  OpenLineage URL: {openlineage_url}")
    click.echo(f"  Namespace: {openlineage_namespace}")
    click.echo(f"  Job name: {job_name}")
    click.echo(f"  Skip dbt run: {skip_dbt_run}")
    if dbt_args:
        click.echo(f"  dbt args: {' '.join(dbt_args)}")
    click.echo("")
    click.echo(
        "✨ This command will be fully implemented in Task 1.4: CLI Implementation"
    )
    click.echo("")
    click.echo("What it will do:")
    click.echo("  1. Run dbt test (unless --skip-dbt-run)")
    click.echo("  2. Parse dbt artifacts (run_results.json, manifest.json)")
    click.echo("  3. Construct OpenLineage event with test results")
    click.echo("  4. Emit event to Correlator")
    click.echo("")
    click.echo("For now, this is a functional skeleton for pipeline testing.")

    # Exit with success so pipeline tests pass
    sys.exit(0)


if __name__ == "__main__":
    cli()
