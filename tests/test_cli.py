"""Tests for CLI module.

This module tests the dbt-correlator CLI commands, including:
    - Command structure and options
    - Execution flow (subprocess calls)
    - Event generation
    - Emission to Correlator
    - Exit code propagation
    - Skip dbt run mode
    - Error handling

Uses Click's CliRunner for CLI testing and unittest.mock for
subprocess and HTTP mocking.
"""

import subprocess
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner
from openlineage.client.event_v2 import RunEvent

from dbt_correlator import __version__
from dbt_correlator.cli import cli, get_default_job_name, get_parent_run_metadata
from dbt_correlator.emitter import ParentRunMetadata
from dbt_correlator.parser import Manifest, RunResults, RunResultsMetadata, TestResult

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mock_run_results() -> RunResults:
    """Create minimal mock RunResults for testing."""
    return RunResults(
        metadata=RunResultsMetadata(
            generated_at=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            invocation_id="test-invocation-id",
            dbt_version="1.10.0",
            elapsed_time=5.5,
        ),
        results=[
            TestResult(
                unique_id="test.my_project.unique_users_id",
                status="pass",
                execution_time_seconds=1.2,
                failures=0,
            ),
            TestResult(
                unique_id="test.my_project.not_null_users_email",
                status="fail",
                execution_time_seconds=0.8,
                failures=5,
                message="5 records failed",
            ),
        ],
    )


@pytest.fixture
def mock_manifest() -> Manifest:
    """Create minimal mock Manifest for testing."""
    return Manifest(
        nodes={
            "test.my_project.unique_users_id": {
                "unique_id": "test.my_project.unique_users_id",
                "test_metadata": {"name": "unique", "kwargs": {"column_name": "id"}},
                "refs": [{"name": "users"}],
            },
            "test.my_project.not_null_users_email": {
                "unique_id": "test.my_project.not_null_users_email",
                "test_metadata": {
                    "name": "not_null",
                    "kwargs": {"column_name": "email"},
                },
                "refs": [{"name": "users"}],
            },
            "model.my_project.users": {
                "unique_id": "model.my_project.users",
                "database": "analytics",
                "schema": "public",
                "name": "users",
            },
        },
        sources={},
        metadata={"dbt_version": "1.10.0"},
    )


@pytest.fixture
def mock_run_event() -> RunEvent:
    """Create mock RunEvent for testing."""
    mock_event = MagicMock(spec=RunEvent)
    mock_event.eventType = "COMPLETE"
    return mock_event


@pytest.fixture
def mock_completed_process_success() -> subprocess.CompletedProcess[bytes]:
    """Mock successful dbt test subprocess result."""
    return subprocess.CompletedProcess(
        args=["dbt", "test"],
        returncode=0,
        stdout=b"Completed successfully",
        stderr=b"",
    )


@pytest.fixture
def mock_completed_process_failure() -> subprocess.CompletedProcess[bytes]:
    """Mock failed dbt test subprocess result (test failures)."""
    return subprocess.CompletedProcess(
        args=["dbt", "test"],
        returncode=1,
        stdout=b"1 of 5 tests failed",
        stderr=b"",
    )


@pytest.fixture
def mock_completed_process_error() -> subprocess.CompletedProcess[bytes]:
    """Mock dbt test subprocess error (compilation error)."""
    return subprocess.CompletedProcess(
        args=["dbt", "test"],
        returncode=2,
        stdout=b"",
        stderr=b"Compilation error",
    )


@pytest.fixture
def cli_mocks(
    mock_run_results: RunResults,
    mock_manifest: Manifest,
    mock_run_event: RunEvent,
    mock_completed_process_success: subprocess.CompletedProcess[bytes],
) -> Iterator[dict[str, Any]]:
    """Consolidated fixture that mocks all CLI dependencies.

    This fixture eliminates the need to repeat multiple @patch decorators on every test.
    It sets up sensible defaults that can be overridden in individual tests.

    Returns:
        Dictionary with all mock objects for assertion access.
    """
    with (
        patch("dbt_correlator.cli.subprocess.run") as mock_subprocess,
        patch("dbt_correlator.cli.emit_events") as mock_emit,
        patch("dbt_correlator.cli.construct_test_events") as mock_construct,
        patch("dbt_correlator.cli.construct_lineage_events") as mock_lineage_events,
        patch("dbt_correlator.cli.create_wrapping_event") as mock_wrapping,
        patch("dbt_correlator.cli.parse_manifest") as mock_parse_manifest,
        patch("dbt_correlator.cli.parse_run_results") as mock_parse_results,
        patch("dbt_correlator.cli.extract_all_model_lineage") as mock_extract_lineage,
        patch("dbt_correlator.cli.get_executed_models") as mock_get_executed,
        patch("dbt_correlator.cli.extract_model_results") as mock_extract_model_results,
    ):
        # Set default return values
        mock_subprocess.return_value = mock_completed_process_success
        mock_parse_results.return_value = mock_run_results
        mock_parse_manifest.return_value = mock_manifest
        mock_wrapping.return_value = mock_run_event
        mock_construct.return_value = [mock_run_event]
        # construct_lineage_events returns list of events
        mock_lineage_events.return_value = [mock_run_event]
        mock_extract_lineage.return_value = []  # Empty list of ModelLineage
        mock_get_executed.return_value = {"model.my_project.users"}
        mock_extract_model_results.return_value = {}

        yield {
            "subprocess": mock_subprocess,
            "emit": mock_emit,
            "construct": mock_construct,
            "construct_lineage": mock_lineage_events,
            "wrapping": mock_wrapping,
            "parse_manifest": mock_parse_manifest,
            "parse_results": mock_parse_results,
            "extract_lineage": mock_extract_lineage,
            "get_executed": mock_get_executed,
            "extract_model_results": mock_extract_model_results,
            "run_results": mock_run_results,
            "manifest": mock_manifest,
            "run_event": mock_run_event,
        }


# =============================================================================
# A. Command Structure Tests
# =============================================================================


@pytest.mark.unit
class TestCommandStructure:
    """Tests for CLI command structure and options."""

    def test_cli_version_option(self, runner: CliRunner) -> None:
        """Test that --version option shows correct version."""
        result = runner.invoke(cli, ["--version"])

        assert result.exit_code == 0
        assert __version__ in result.output
        assert "dbt-correlator" in result.output

    def test_cli_help_option(self, runner: CliRunner) -> None:
        """Test that --help option shows help text."""
        result = runner.invoke(cli, ["--help"])

        assert result.exit_code == 0
        assert "dbt-correlator" in result.output
        assert "test" in result.output  # test command listed

    def test_test_command_help(self, runner: CliRunner) -> None:
        """Test that 'dbt-correlator test --help' shows test command help."""
        result = runner.invoke(cli, ["test", "--help"])

        assert result.exit_code == 0
        assert "--correlator-endpoint" in result.output
        assert "--project-dir" in result.output
        assert "--profiles-dir" in result.output
        assert "--skip-dbt-run" in result.output

    def test_test_command_requires_correlator_endpoint(self, runner: CliRunner) -> None:
        """Test that test command fails without --correlator-endpoint."""
        result = runner.invoke(cli, ["test"])

        assert result.exit_code != 0
        assert "correlator-endpoint" in result.output.lower()


# =============================================================================
# B. Execution Flow Tests
# =============================================================================


@pytest.mark.unit
class TestExecutionFlow:
    """Tests for dbt test execution flow."""

    def test_test_command_runs_dbt_test(
        self, runner: CliRunner, cli_mocks: dict[str, Any]
    ) -> None:
        """Test that test command calls subprocess.run with dbt test."""
        runner.invoke(
            cli,
            [
                "test",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
            ],
        )

        cli_mocks["subprocess"].assert_called_once()
        call_args = cli_mocks["subprocess"].call_args
        assert "dbt" in call_args[0][0]
        assert "test" in call_args[0][0]

    def test_test_command_passes_dbt_args(
        self, runner: CliRunner, cli_mocks: dict[str, Any]
    ) -> None:
        """Test that pass-through args (--select) are passed to dbt test."""
        runner.invoke(
            cli,
            [
                "test",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
                "--",
                "--select",
                "my_model",
            ],
        )

        call_args = cli_mocks["subprocess"].call_args[0][0]
        assert "--select" in call_args
        assert "my_model" in call_args

    def test_test_command_uses_project_dir(
        self, runner: CliRunner, cli_mocks: dict[str, Any], tmp_path: Path
    ) -> None:
        """Test that --project-dir is passed to dbt."""
        runner.invoke(
            cli,
            [
                "test",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
                "--project-dir",
                str(tmp_path),
            ],
        )

        call_args = cli_mocks["subprocess"].call_args[0][0]
        assert "--project-dir" in call_args
        assert str(tmp_path) in call_args

    def test_test_command_uses_profiles_dir(
        self, runner: CliRunner, cli_mocks: dict[str, Any], tmp_path: Path
    ) -> None:
        """Test that --profiles-dir is passed to dbt."""
        profiles_dir = tmp_path / "profiles"
        profiles_dir.mkdir()

        runner.invoke(
            cli,
            [
                "test",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
                "--profiles-dir",
                str(profiles_dir),
            ],
        )

        call_args = cli_mocks["subprocess"].call_args[0][0]
        assert "--profiles-dir" in call_args
        assert str(profiles_dir) in call_args


# =============================================================================
# C. Event Generation Tests
# =============================================================================


@pytest.mark.unit
class TestEventGeneration:
    """Tests for OpenLineage event generation."""

    def test_test_command_generates_start_event(
        self, runner: CliRunner, cli_mocks: dict[str, Any]
    ) -> None:
        """Test that START event is created before dbt test runs."""
        runner.invoke(
            cli,
            [
                "test",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
            ],
        )

        start_call = cli_mocks["wrapping"].call_args_list[0]
        assert start_call[0][0] == "START"

    def test_test_command_generates_complete_event_on_success(
        self, runner: CliRunner, cli_mocks: dict[str, Any]
    ) -> None:
        """Test that COMPLETE event is created when dbt test succeeds."""
        runner.invoke(
            cli,
            [
                "test",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
            ],
        )

        complete_call = cli_mocks["wrapping"].call_args_list[1]
        assert complete_call[0][0] == "COMPLETE"

    def test_test_command_generates_fail_event_on_failure(
        self,
        runner: CliRunner,
        cli_mocks: dict[str, Any],
        mock_completed_process_failure: subprocess.CompletedProcess[bytes],
    ) -> None:
        """Test that FAIL event is created when dbt test fails."""
        cli_mocks["subprocess"].return_value = mock_completed_process_failure

        runner.invoke(
            cli,
            [
                "test",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
            ],
        )

        fail_call = cli_mocks["wrapping"].call_args_list[1]
        assert fail_call[0][0] == "FAIL"

    def test_test_command_constructs_test_events(
        self, runner: CliRunner, cli_mocks: dict[str, Any]
    ) -> None:
        """Test that test events are constructed from artifacts."""
        runner.invoke(
            cli,
            [
                "test",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
            ],
        )

        cli_mocks["construct"].assert_called_once()
        call_kwargs = cli_mocks["construct"].call_args[1]
        assert call_kwargs["run_results"] == cli_mocks["run_results"]
        assert call_kwargs["manifest"] == cli_mocks["manifest"]


# =============================================================================
# D. Emission Tests
# =============================================================================


@pytest.mark.unit
class TestEmission:
    """Tests for event emission to Correlator."""

    def test_test_command_emits_events_to_correlator(
        self, runner: CliRunner, cli_mocks: dict[str, Any]
    ) -> None:
        """Test that emit_events is called with Correlator endpoint.

        Test command emits twice:
        1. START event immediately
        2. lineage + test + terminal events in batch
        """
        endpoint = "http://localhost:8080/api/v1/lineage/events"
        runner.invoke(cli, ["test", "--correlator-endpoint", endpoint])

        # Should be called twice (START + batch)
        assert cli_mocks["emit"].call_count == 2
        # Both calls should use the same endpoint
        for call in cli_mocks["emit"].call_args_list:
            assert call[0][1] == endpoint

    def test_test_command_includes_api_key_header(
        self, runner: CliRunner, cli_mocks: dict[str, Any]
    ) -> None:
        """Test that API key is passed to emit_events when provided."""
        api_key = "test-api-key-123"
        runner.invoke(
            cli,
            [
                "test",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
                "--correlator-api-key",
                api_key,
            ],
        )

        # Should be called twice (START + batch)
        assert cli_mocks["emit"].call_count == 2
        # Both calls should include the API key
        for call in cli_mocks["emit"].call_args_list:
            assert call[0][2] == api_key

    def test_test_command_batch_emits_all_events(
        self, runner: CliRunner, cli_mocks: dict[str, Any]
    ) -> None:
        """Test that lineage + test + terminal events are emitted in batch.

        Test command emits:
        1. START event immediately (1 event)
        2. test events (2) + terminal (1) = 3 events in batch
           (no lineage events - tests validate inputs, don't produce outputs)
        """
        cli_mocks["construct"].return_value = [
            cli_mocks["run_event"],
            cli_mocks["run_event"],
        ]
        # Note: construct_lineage is NOT called for test command

        runner.invoke(
            cli,
            [
                "test",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
            ],
        )

        # Should be called twice (START + batch)
        assert cli_mocks["emit"].call_count == 2
        # First call: START event only
        start_events = cli_mocks["emit"].call_args_list[0][0][0]
        assert len(start_events) == 1
        # Second call: test events (2) + terminal (1) = 3 events (no lineage)
        batch_events = cli_mocks["emit"].call_args_list[1][0][0]
        assert len(batch_events) == 3


# =============================================================================
# E. Exit Code Tests
# =============================================================================


@pytest.mark.unit
class TestExitCodes:
    """Tests for exit code propagation from dbt."""

    def test_test_command_exits_with_dbt_exit_code_success(
        self, runner: CliRunner, cli_mocks: dict[str, Any]
    ) -> None:
        """Test that CLI exits with 0 when dbt test succeeds."""
        result = runner.invoke(
            cli,
            [
                "test",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
            ],
        )

        assert cli_mocks
        assert result.exit_code == 0

    def test_test_command_exits_with_dbt_exit_code_failure(
        self,
        runner: CliRunner,
        cli_mocks: dict[str, Any],
        mock_completed_process_failure: subprocess.CompletedProcess[bytes],
    ) -> None:
        """Test that CLI exits with 1 when dbt test has failures."""
        cli_mocks["subprocess"].return_value = mock_completed_process_failure

        result = runner.invoke(
            cli,
            [
                "test",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
            ],
        )

        assert result.exit_code == 1

    def test_test_command_exits_with_dbt_exit_code_error(
        self,
        runner: CliRunner,
        cli_mocks: dict[str, Any],
        mock_completed_process_error: subprocess.CompletedProcess[bytes],
    ) -> None:
        """Test that CLI exits with 2 when dbt has compilation error."""
        cli_mocks["subprocess"].return_value = mock_completed_process_error

        result = runner.invoke(
            cli,
            [
                "test",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
            ],
        )

        assert result.exit_code == 2


# =============================================================================
# F. Skip dbt Run Tests
# =============================================================================


@pytest.mark.unit
class TestSkipDbtRun:
    """Tests for --skip-dbt-run flag."""

    def test_test_command_skip_dbt_run_skips_subprocess(
        self, runner: CliRunner, cli_mocks: dict[str, Any]
    ) -> None:
        """Test that --skip-dbt-run skips subprocess call."""
        runner.invoke(
            cli,
            [
                "test",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
                "--skip-dbt-run",
            ],
        )

        cli_mocks["subprocess"].assert_not_called()

    def test_test_command_skip_dbt_run_uses_existing_artifacts(
        self, runner: CliRunner, cli_mocks: dict[str, Any]
    ) -> None:
        """Test that --skip-dbt-run still parses existing artifacts."""
        runner.invoke(
            cli,
            [
                "test",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
                "--skip-dbt-run",
            ],
        )

        cli_mocks["parse_results"].assert_called_once()
        # parse_manifest is called twice: once for job_name, once for artifacts
        assert cli_mocks["parse_manifest"].call_count >= 1

    def test_test_command_skip_dbt_run_still_emits_events(
        self, runner: CliRunner, cli_mocks: dict[str, Any]
    ) -> None:
        """Test that --skip-dbt-run still emits events."""
        runner.invoke(
            cli,
            [
                "test",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
                "--skip-dbt-run",
            ],
        )

        # Test command emits twice (START + batch)
        assert cli_mocks["emit"].call_count == 2


# =============================================================================
# G. Error Handling Tests
# =============================================================================


@pytest.mark.unit
class TestErrorHandling:
    """Tests for error handling in CLI."""

    def test_test_command_handles_missing_artifacts(
        self, runner: CliRunner, cli_mocks: dict[str, Any]
    ) -> None:
        """Test that missing artifacts produce clear error."""
        cli_mocks["parse_results"].side_effect = FileNotFoundError(
            "run_results.json not found"
        )

        result = runner.invoke(
            cli,
            [
                "test",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
            ],
        )

        assert result.exit_code != 0
        assert "run_results.json" in result.output or "not found" in result.output

    def test_test_command_handles_dbt_not_found(
        self, runner: CliRunner, cli_mocks: dict[str, Any]
    ) -> None:
        """Test that dbt not found produces clear error."""
        cli_mocks["subprocess"].side_effect = FileNotFoundError("dbt not found")

        result = runner.invoke(
            cli,
            [
                "test",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
            ],
        )

        assert result.exit_code != 0
        assert "dbt" in result.output.lower()

    def test_test_command_handles_correlator_unreachable(
        self, runner: CliRunner, cli_mocks: dict[str, Any]
    ) -> None:
        """Test that Correlator unreachable logs warning but doesn't fail dbt."""
        cli_mocks["emit"].side_effect = ConnectionError("Connection refused")

        result = runner.invoke(
            cli,
            [
                "test",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
            ],
        )

        assert result.exit_code == 0
        assert "warning" in result.output.lower() or "failed" in result.output.lower()

    def test_test_command_handles_emission_timeout(
        self, runner: CliRunner, cli_mocks: dict[str, Any]
    ) -> None:
        """Test that emission timeout logs warning but doesn't fail dbt."""
        cli_mocks["emit"].side_effect = TimeoutError("Request timed out")

        result = runner.invoke(
            cli,
            [
                "test",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
            ],
        )

        assert result.exit_code == 0
        assert "warning" in result.output.lower() or "failed" in result.output.lower()


# =============================================================================
# H. Config File Integration Tests
# =============================================================================


@pytest.mark.unit
class TestConfigFileIntegration:
    """Tests for config file integration with CLI."""

    def test_test_command_with_config_file(
        self, runner: CliRunner, cli_mocks: dict[str, Any], tmp_path: Path
    ) -> None:
        """Test that config file values are used when no CLI args provided."""
        config_file = tmp_path / ".dbt-correlator.yml"
        config_file.write_text(
            """\
correlator:
  endpoint: http://config-file-endpoint:8080/api/v1/lineage/events
  namespace: from-config
"""
        )

        runner.invoke(
            cli,
            [
                "test",
                "--config",
                str(config_file),
            ],
        )

        # Verify the endpoint from config file was used (test emits twice)
        assert cli_mocks["emit"].call_count == 2
        # Check both calls use the config endpoint
        for call in cli_mocks["emit"].call_args_list:
            assert (
                call[0][1] == "http://config-file-endpoint:8080/api/v1/lineage/events"
            )

    def test_cli_args_override_config_file(
        self, runner: CliRunner, cli_mocks: dict[str, Any], tmp_path: Path
    ) -> None:
        """Test that CLI args take precedence over config file values."""
        config_file = tmp_path / ".dbt-correlator.yml"
        config_file.write_text(
            """\
correlator:
  endpoint: http://config-file-endpoint:8080
  namespace: from-config
"""
        )

        runner.invoke(
            cli,
            [
                "test",
                "--config",
                str(config_file),
                "--correlator-endpoint",
                "http://cli-override:8080/api/v1/lineage/events",
            ],
        )

        # CLI arg should override config file (test emits twice)
        assert cli_mocks["emit"].call_count == 2
        # Check both calls use the CLI override endpoint
        for call in cli_mocks["emit"].call_args_list:
            assert call[0][1] == "http://cli-override:8080/api/v1/lineage/events"

    def test_config_option_shown_in_help(self, runner: CliRunner) -> None:
        """Test that --config option appears in help."""
        result = runner.invoke(cli, ["test", "--help"])

        assert result.exit_code == 0
        assert "--config" in result.output

    def test_invalid_config_file_shows_error(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """Test that invalid YAML config file produces clear error."""
        config_file = tmp_path / "invalid.yml"
        config_file.write_text("invalid: yaml: [unclosed")

        result = runner.invoke(
            cli,
            [
                "test",
                "--config",
                str(config_file),
            ],
        )

        assert result.exit_code != 0
        assert "invalid" in result.output.lower() or "yaml" in result.output.lower()

    def test_missing_config_file_with_explicit_path_shows_error(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """Test that explicitly specified missing config file shows error."""
        non_existent = tmp_path / "does-not-exist.yml"

        result = runner.invoke(
            cli,
            [
                "test",
                "--config",
                str(non_existent),
            ],
        )

        # Should either fail with missing config or missing endpoint
        assert result.exit_code != 0

    def test_env_var_interpolation_in_config_file(
        self,
        runner: CliRunner,
        cli_mocks: dict[str, Any],
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that env vars in config file are expanded."""
        monkeypatch.setenv("MY_ENDPOINT", "http://from-env:8080/api/v1/lineage/events")

        config_file = tmp_path / ".dbt-correlator.yml"
        config_file.write_text(
            """\
correlator:
  endpoint: ${MY_ENDPOINT}
"""
        )

        runner.invoke(
            cli,
            [
                "test",
                "--config",
                str(config_file),
            ],
        )

        # Test command emits twice (START + batch)
        assert cli_mocks["emit"].call_count == 2
        # Check both calls use the expanded endpoint
        for call in cli_mocks["emit"].call_args_list:
            assert call[0][1] == "http://from-env:8080/api/v1/lineage/events"

    def test_auto_discovers_config_in_cwd(
        self,
        runner: CliRunner,
        cli_mocks: dict[str, Any],
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that .dbt-correlator.yml is auto-discovered in cwd without --config."""
        # Create config file in tmp_path (will be cwd)
        config_file = tmp_path / ".dbt-correlator.yml"
        config_file.write_text(
            """\
correlator:
  endpoint: http://auto-discovered:8080/api/v1/lineage/events
  namespace: auto-discovered-namespace
"""
        )

        # Change working directory to tmp_path
        monkeypatch.chdir(tmp_path)

        # Invoke WITHOUT --config flag - should auto-discover
        runner.invoke(
            cli,
            ["test"],  # No --config flag!
        )

        # Verify auto-discovered endpoint was used (test emits twice)
        assert cli_mocks["emit"].call_count == 2
        for call in cli_mocks["emit"].call_args_list:
            assert call[0][1] == "http://auto-discovered:8080/api/v1/lineage/events"

    def test_env_var_overrides_config_file_in_cli(
        self,
        runner: CliRunner,
        cli_mocks: dict[str, Any],
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that CORRELATOR_ENDPOINT env var overrides config file value."""
        # Set env var with higher priority
        monkeypatch.setenv(
            "CORRELATOR_ENDPOINT", "http://from-env-var:8080/api/v1/lineage/events"
        )

        # Create config file with different endpoint
        config_file = tmp_path / ".dbt-correlator.yml"
        config_file.write_text(
            """\
correlator:
  endpoint: http://from-config-file:8080/api/v1/lineage/events
"""
        )

        # Invoke with config file - env var should still win
        runner.invoke(
            cli,
            [
                "test",
                "--config",
                str(config_file),
            ],
        )

        # Verify env var wins over config file (test emits twice)
        assert cli_mocks["emit"].call_count == 2
        for call in cli_mocks["emit"].call_args_list:
            assert call[0][1] == "http://from-env-var:8080/api/v1/lineage/events"


# =============================================================================
# I. Run Command Tests
# =============================================================================


@pytest.mark.unit
class TestRunCommand:
    """Tests for 'dbt-correlator run' command."""

    def test_run_command_help(self, runner: CliRunner) -> None:
        """Test that 'dbt-correlator run --help' shows run command help."""
        result = runner.invoke(cli, ["run", "--help"])

        assert result.exit_code == 0
        assert "--correlator-endpoint" in result.output
        assert "--project-dir" in result.output
        assert "--dataset-namespace" in result.output
        assert "lineage" in result.output.lower()

    def test_run_command_requires_correlator_endpoint(self, runner: CliRunner) -> None:
        """Test that run command fails without --correlator-endpoint."""
        result = runner.invoke(cli, ["run"])

        assert result.exit_code != 0
        assert "correlator-endpoint" in result.output.lower()

    def test_run_command_runs_dbt_run(
        self, runner: CliRunner, cli_mocks: dict[str, Any]
    ) -> None:
        """Test that run command calls subprocess.run with dbt run."""
        runner.invoke(
            cli,
            [
                "run",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
            ],
        )

        # Check subprocess was called with 'dbt run'
        subprocess_calls = cli_mocks["subprocess"].call_args_list
        # Find the call with 'run' command (skip any 'test' calls)
        run_call = None
        for call in subprocess_calls:
            if "run" in call[0][0]:
                run_call = call
                break

        assert run_call is not None, "Expected dbt run to be called"
        assert "dbt" in run_call[0][0]
        assert "run" in run_call[0][0]

    def test_run_command_extracts_lineage(
        self, runner: CliRunner, cli_mocks: dict[str, Any]
    ) -> None:
        """Test that run command extracts model lineage."""
        runner.invoke(
            cli,
            [
                "run",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
            ],
        )

        cli_mocks["extract_lineage"].assert_called_once()
        cli_mocks["construct_lineage"].assert_called_once()

    def test_run_command_emits_lineage_events(
        self, runner: CliRunner, cli_mocks: dict[str, Any]
    ) -> None:
        """Test that run command emits lineage events."""
        runner.invoke(
            cli,
            [
                "run",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
            ],
        )

        # emit_events should be called at least twice (START + lineage+terminal)
        assert cli_mocks["emit"].call_count >= 2

    def test_run_command_passes_dataset_namespace(
        self, runner: CliRunner, cli_mocks: dict[str, Any]
    ) -> None:
        """Test that --dataset-namespace is passed to extract_all_model_lineage."""
        runner.invoke(
            cli,
            [
                "run",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
                "--dataset-namespace",
                "postgresql://localhost:5432/mydb",
            ],
        )

        # Verify namespace_override was passed to extract_all_model_lineage
        call_kwargs = cli_mocks["extract_lineage"].call_args[1]
        assert (
            call_kwargs.get("namespace_override") == "postgresql://localhost:5432/mydb"
        )

    def test_run_command_skip_dbt_run_flag(
        self, runner: CliRunner, cli_mocks: dict[str, Any]
    ) -> None:
        """Test that --skip-dbt-run flag skips dbt execution."""
        runner.invoke(
            cli,
            [
                "run",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
                "--skip-dbt-run",
            ],
        )

        # subprocess should only be called for emit, not dbt run
        # When skipping, subprocess.run for 'dbt run' should not be called
        for call in cli_mocks["subprocess"].call_args_list:
            cmd = call[0][0]
            # Should not have a 'dbt run' call
            assert not (
                "dbt" in cmd and "run" in cmd and "--project-dir" in cmd
            ), "dbt run should not be called with --skip-dbt-run"

    def test_run_command_propagates_exit_code(
        self, runner: CliRunner, cli_mocks: dict[str, Any]
    ) -> None:
        """Test that run command propagates dbt exit code."""
        # Set up failure return code
        cli_mocks["subprocess"].return_value = subprocess.CompletedProcess(
            args=["dbt", "run"], returncode=1, stdout=b"", stderr=b""
        )

        result = runner.invoke(
            cli,
            [
                "run",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
            ],
        )

        assert result.exit_code == 1


# =============================================================================
# J. Build Command Tests
# =============================================================================


@pytest.mark.unit
class TestBuildCommand:
    """Tests for 'dbt-correlator build' command."""

    def test_build_command_help(self, runner: CliRunner) -> None:
        """Test that 'dbt-correlator build --help' shows build command help."""
        result = runner.invoke(cli, ["build", "--help"])

        assert result.exit_code == 0
        assert "--correlator-endpoint" in result.output
        assert "--project-dir" in result.output
        assert "--dataset-namespace" in result.output
        assert "lineage" in result.output.lower()
        assert "test" in result.output.lower()

    def test_build_command_requires_correlator_endpoint(
        self, runner: CliRunner
    ) -> None:
        """Test that build command fails without --correlator-endpoint."""
        result = runner.invoke(cli, ["build"])

        assert result.exit_code != 0
        assert "correlator-endpoint" in result.output.lower()

    def test_build_command_runs_dbt_build(
        self, runner: CliRunner, cli_mocks: dict[str, Any]
    ) -> None:
        """Test that build command calls subprocess.run with dbt build."""
        runner.invoke(
            cli,
            [
                "build",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
            ],
        )

        # Check subprocess was called with 'dbt build'
        subprocess_calls = cli_mocks["subprocess"].call_args_list
        build_call = None
        for call in subprocess_calls:
            if "build" in call[0][0]:
                build_call = call
                break

        assert build_call is not None, "Expected dbt build to be called"
        assert "dbt" in build_call[0][0]
        assert "build" in build_call[0][0]

    def test_build_command_emits_both_lineage_and_test_events(
        self, runner: CliRunner, cli_mocks: dict[str, Any]
    ) -> None:
        """Test that build command emits both lineage and test events."""
        runner.invoke(
            cli,
            [
                "build",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
            ],
        )

        # Both construct_lineage_events and construct_test_events should be called
        cli_mocks["construct_lineage"].assert_called_once()
        cli_mocks["construct"].assert_called_once()

    def test_build_command_test_events_use_consolidated_pattern(
        self, runner: CliRunner, cli_mocks: dict[str, Any]
    ) -> None:
        """Test that build command uses consolidated pattern for test events.

        Consolidated pattern: test events no longer receive model_run_ids.
        Instead, a single event with all test inputs is created.
        """
        runner.invoke(
            cli,
            [
                "build",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
            ],
        )

        # Verify construct_test_events was called without model_run_ids
        construct_call_kwargs = cli_mocks["construct"].call_args[1]

        # model_run_ids should NOT be passed (consolidated pattern)
        assert (
            "model_run_ids" not in construct_call_kwargs
        ), "construct_test_events should NOT receive model_run_ids (consolidated pattern)"

        # parent params should NOT be passed (fixes self-referential parent bug)
        assert (
            "parent_run_id" not in construct_call_kwargs
        ), "construct_test_events should NOT receive parent_run_id (consolidated pattern)"

    def test_build_command_propagates_exit_code(
        self, runner: CliRunner, cli_mocks: dict[str, Any]
    ) -> None:
        """Test that build command propagates dbt exit code."""
        # Set up failure return code (test failures)
        cli_mocks["subprocess"].return_value = subprocess.CompletedProcess(
            args=["dbt", "build"], returncode=1, stdout=b"", stderr=b""
        )

        result = runner.invoke(
            cli,
            [
                "build",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
            ],
        )

        assert result.exit_code == 1

    def test_build_command_skip_dbt_run_flag(
        self, runner: CliRunner, cli_mocks: dict[str, Any]
    ) -> None:
        """Test that --skip-dbt-run flag skips dbt execution in build."""
        runner.invoke(
            cli,
            [
                "build",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
                "--skip-dbt-run",
            ],
        )

        # Should not have a 'dbt build' call
        for call in cli_mocks["subprocess"].call_args_list:
            cmd = call[0][0]
            assert not (
                "dbt" in cmd and "build" in cmd and "--project-dir" in cmd
            ), "dbt build should not be called with --skip-dbt-run"


# =============================================================================
# K. CLI Help Shows All Commands
# =============================================================================


@pytest.mark.unit
class TestCLIShowsAllCommands:
    """Tests to verify all commands are visible in CLI help."""

    def test_cli_help_shows_all_commands(self, runner: CliRunner) -> None:
        """Test that main CLI help shows test, run, and build commands."""
        result = runner.invoke(cli, ["--help"])

        assert result.exit_code == 0
        assert "test" in result.output
        assert "run" in result.output
        assert "build" in result.output


# =============================================================================
# L. Dynamic Job Name Tests
# =============================================================================


@pytest.mark.unit
class TestDynamicJobName:
    """Tests for dynamic job name feature (dbt-ol compatible)."""

    def test_get_default_job_name_returns_project_command_format(
        self, mock_manifest: Any
    ) -> None:
        """Test that get_default_job_name returns {project_name}.{command} format."""
        # Add project_name to mock manifest metadata
        mock_manifest.metadata["project_name"] = "jaffle_shop"

        assert get_default_job_name(mock_manifest, "test") == "jaffle_shop.test"
        assert get_default_job_name(mock_manifest, "run") == "jaffle_shop.run"
        assert get_default_job_name(mock_manifest, "build") == "jaffle_shop.build"

    def test_get_default_job_name_falls_back_to_dbt(self, mock_manifest: Any) -> None:
        """Test that get_default_job_name falls back to 'dbt' if no project_name."""
        # Remove project_name from metadata
        mock_manifest.metadata.pop("project_name", None)

        assert get_default_job_name(mock_manifest, "test") == "dbt.test"
        assert get_default_job_name(mock_manifest, "run") == "dbt.run"

    def test_test_command_uses_dynamic_job_name(
        self, runner: CliRunner, cli_mocks: dict[str, Any]
    ) -> None:
        """Test that test command uses dynamic job name from manifest."""
        # Set up manifest with project_name
        cli_mocks["manifest"].metadata["project_name"] = "my_project"

        runner.invoke(
            cli,
            [
                "test",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
            ],
        )

        # Verify wrapping event was called with dynamic job name
        wrapping_calls = cli_mocks["wrapping"].call_args_list
        # First call is START event
        start_call = wrapping_calls[0]
        # job_name is the 3rd positional argument (after event_type and run_id)
        job_name_used = start_call[0][2]
        assert job_name_used == "my_project.test"

    def test_custom_job_name_overrides_dynamic(
        self, runner: CliRunner, cli_mocks: dict[str, Any]
    ) -> None:
        """Test that --job-name overrides dynamic job name."""
        cli_mocks["manifest"].metadata["project_name"] = "my_project"

        runner.invoke(
            cli,
            [
                "test",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
                "--job-name",
                "custom_job_name",
            ],
        )

        # Verify custom job name was used
        wrapping_calls = cli_mocks["wrapping"].call_args_list
        start_call = wrapping_calls[0]
        job_name_used = start_call[0][2]
        assert job_name_used == "custom_job_name"


# =============================================================================
# M. OpenLineage Environment Variable Compatibility Tests
# =============================================================================


@pytest.mark.unit
class TestOpenLineageEnvVarCompatibility:
    """Tests for dbt-ol compatible environment variable fallbacks.

    dbt-correlator supports dbt-ol environment variables as fallbacks
    to simplify migration from dbt-ol.

    Priority order:
        1. CLI arguments (highest)
        2. CORRELATOR_* env vars
        3. OPENLINEAGE_* env vars (lowest)
    """

    def test_openlineage_url_fallback_when_no_endpoint_provided(
        self,
        runner: CliRunner,
        cli_mocks: dict[str, Any],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that OPENLINEAGE_URL env var is used when no endpoint provided."""
        monkeypatch.setenv(
            "OPENLINEAGE_URL", "http://openlineage-backend:5000/api/v1/lineage"
        )

        runner.invoke(cli, ["test"])

        # Should use OPENLINEAGE_URL as endpoint (test emits twice)
        assert cli_mocks["emit"].call_count == 2
        for call in cli_mocks["emit"].call_args_list:
            assert call[0][1] == "http://openlineage-backend:5000/api/v1/lineage"

    def test_correlator_endpoint_takes_priority_over_openlineage_url(
        self,
        runner: CliRunner,
        cli_mocks: dict[str, Any],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that CORRELATOR_ENDPOINT takes priority over OPENLINEAGE_URL."""
        monkeypatch.setenv(
            "CORRELATOR_ENDPOINT", "http://correlator:8080/api/v1/lineage/events"
        )
        monkeypatch.setenv("OPENLINEAGE_URL", "http://openlineage:5000/api/v1/lineage")

        runner.invoke(cli, ["test"])

        # Should use CORRELATOR_ENDPOINT, not OPENLINEAGE_URL (test emits twice)
        assert cli_mocks["emit"].call_count == 2
        for call in cli_mocks["emit"].call_args_list:
            assert call[0][1] == "http://correlator:8080/api/v1/lineage/events"

    def test_cli_endpoint_takes_priority_over_all_env_vars(
        self,
        runner: CliRunner,
        cli_mocks: dict[str, Any],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that CLI --correlator-endpoint overrides all env vars."""
        monkeypatch.setenv(
            "CORRELATOR_ENDPOINT", "http://correlator-env:8080/api/v1/lineage/events"
        )
        monkeypatch.setenv(
            "OPENLINEAGE_URL", "http://openlineage-env:5000/api/v1/lineage"
        )

        runner.invoke(
            cli,
            [
                "test",
                "--correlator-endpoint",
                "http://cli-override:8080/api/v1/lineage/events",
            ],
        )

        # CLI arg should win (test emits twice)
        assert cli_mocks["emit"].call_count == 2
        for call in cli_mocks["emit"].call_args_list:
            assert call[0][1] == "http://cli-override:8080/api/v1/lineage/events"

    def test_openlineage_api_key_fallback_when_no_api_key_provided(
        self,
        runner: CliRunner,
        cli_mocks: dict[str, Any],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that OPENLINEAGE_API_KEY env var is used when no API key provided."""
        monkeypatch.setenv(
            "CORRELATOR_ENDPOINT", "http://localhost:8080/api/v1/lineage/events"
        )
        monkeypatch.setenv("OPENLINEAGE_API_KEY", "openlineage-api-key-123")

        runner.invoke(cli, ["test"])

        # Should use OPENLINEAGE_API_KEY (test emits twice)
        assert cli_mocks["emit"].call_count == 2
        for call in cli_mocks["emit"].call_args_list:
            assert call[0][2] == "openlineage-api-key-123"

    def test_correlator_api_key_takes_priority_over_openlineage_api_key(
        self,
        runner: CliRunner,
        cli_mocks: dict[str, Any],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that CORRELATOR_API_KEY takes priority over OPENLINEAGE_API_KEY."""
        monkeypatch.setenv(
            "CORRELATOR_ENDPOINT", "http://localhost:8080/api/v1/lineage/events"
        )
        monkeypatch.setenv("CORRELATOR_API_KEY", "correlator-api-key-456")
        monkeypatch.setenv("OPENLINEAGE_API_KEY", "openlineage-api-key-123")

        runner.invoke(cli, ["test"])

        # Should use CORRELATOR_API_KEY, not OPENLINEAGE_API_KEY (test emits twice)
        assert cli_mocks["emit"].call_count == 2
        for call in cli_mocks["emit"].call_args_list:
            assert call[0][2] == "correlator-api-key-456"

    def test_cli_api_key_takes_priority_over_all_env_vars(
        self,
        runner: CliRunner,
        cli_mocks: dict[str, Any],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that CLI --correlator-api-key overrides all env vars."""
        monkeypatch.setenv(
            "CORRELATOR_ENDPOINT", "http://localhost:8080/api/v1/lineage/events"
        )
        monkeypatch.setenv("CORRELATOR_API_KEY", "correlator-env-key")
        monkeypatch.setenv("OPENLINEAGE_API_KEY", "openlineage-env-key")

        runner.invoke(
            cli,
            [
                "test",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
                "--correlator-api-key",
                "cli-override-key",
            ],
        )

        # CLI arg should win (test emits twice)
        assert cli_mocks["emit"].call_count == 2
        for call in cli_mocks["emit"].call_args_list:
            assert call[0][2] == "cli-override-key"

    def test_missing_endpoint_shows_helpful_error_message(
        self, runner: CliRunner
    ) -> None:
        """Test that missing endpoint shows error mentioning all options."""
        result = runner.invoke(cli, ["test"])

        assert result.exit_code != 0
        # Error should mention all ways to provide endpoint
        assert (
            "CORRELATOR_ENDPOINT" in result.output
            or "correlator-endpoint" in result.output
        )
        assert "OPENLINEAGE_URL" in result.output


# =============================================================================
# N. START Emission Timing Tests
# =============================================================================


@pytest.mark.unit
class TestStartEmissionTiming:
    """Tests to verify START event is emitted before dbt execution."""

    def test_start_event_emitted_before_dbt_test_runs(
        self, runner: CliRunner, cli_mocks: dict[str, Any]
    ) -> None:
        """Test that START event is emitted BEFORE dbt test subprocess runs.

        This is critical for tracking job lifecycle in OpenLineage consumers.
        The START event should be emitted immediately when the command starts,
        not after dbt execution completes.
        """
        call_order: list[str] = []

        # Track call order
        original_subprocess = cli_mocks["subprocess"]

        def track_emit(*args: Any, **kwargs: Any) -> None:
            call_order.append("emit")

        def track_subprocess(*args: Any, **kwargs: Any) -> Any:
            call_order.append("subprocess")
            return original_subprocess.return_value

        cli_mocks["emit"].side_effect = track_emit
        cli_mocks["subprocess"].side_effect = track_subprocess

        runner.invoke(
            cli,
            [
                "test",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
            ],
        )

        # Verify emit was called before subprocess
        # Call order should be: emit (START), subprocess (dbt test), emit (batch)
        assert call_order[0] == "emit", "START event should be emitted before dbt runs"
        assert call_order[1] == "subprocess", "dbt should run after START emission"
        assert call_order[2] == "emit", "Batch emission should happen after dbt"

    def test_start_event_emitted_before_dbt_run_runs(
        self, runner: CliRunner, cli_mocks: dict[str, Any]
    ) -> None:
        """Test that START event is emitted BEFORE dbt run subprocess runs."""
        call_order: list[str] = []

        original_subprocess = cli_mocks["subprocess"]

        def track_emit(*args: Any, **kwargs: Any) -> None:
            call_order.append("emit")

        def track_subprocess(*args: Any, **kwargs: Any) -> Any:
            call_order.append("subprocess")
            return original_subprocess.return_value

        cli_mocks["emit"].side_effect = track_emit
        cli_mocks["subprocess"].side_effect = track_subprocess

        runner.invoke(
            cli,
            [
                "run",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
            ],
        )

        # Verify emit (START) was called before subprocess (dbt run)
        assert call_order[0] == "emit", "START event should be emitted before dbt runs"
        assert call_order[1] == "subprocess", "dbt should run after START emission"

    def test_start_event_emitted_before_dbt_build_runs(
        self, runner: CliRunner, cli_mocks: dict[str, Any]
    ) -> None:
        """Test that START event is emitted BEFORE dbt build subprocess runs."""
        call_order: list[str] = []

        original_subprocess = cli_mocks["subprocess"]

        def track_emit(*args: Any, **kwargs: Any) -> None:
            call_order.append("emit")

        def track_subprocess(*args: Any, **kwargs: Any) -> Any:
            call_order.append("subprocess")
            return original_subprocess.return_value

        cli_mocks["emit"].side_effect = track_emit
        cli_mocks["subprocess"].side_effect = track_subprocess

        runner.invoke(
            cli,
            [
                "build",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
            ],
        )

        # Verify emit (START) was called before subprocess (dbt build)
        assert call_order[0] == "emit", "START event should be emitted before dbt runs"
        assert call_order[1] == "subprocess", "dbt should run after START emission"


# =============================================================================
# O. Test Command Does NOT Emit Lineage (Only Test Events)
# =============================================================================


@pytest.mark.unit
class TestTestCommandNoLineageEmission:
    """Tests verifying test command does NOT emit lineage events.

    Tests validate existing data (inputs only) - they don't produce outputs.
    Emitting lineage events with outputs for test command creates spurious
    edges in Correlator's lineage_impact_analysis view.
    """

    def test_test_command_does_not_call_extract_lineage(
        self, runner: CliRunner, cli_mocks: dict[str, Any]
    ) -> None:
        """Test that test command does NOT extract model lineage."""
        runner.invoke(
            cli,
            [
                "test",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
            ],
        )

        # extract_all_model_lineage should NOT be called for test command
        cli_mocks["extract_lineage"].assert_not_called()

    def test_test_command_does_not_call_construct_lineage_events(
        self, runner: CliRunner, cli_mocks: dict[str, Any]
    ) -> None:
        """Test that test command does NOT construct lineage events."""
        runner.invoke(
            cli,
            [
                "test",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
            ],
        )

        # construct_lineage_events should NOT be called for test command
        cli_mocks["construct_lineage"].assert_not_called()

    def test_test_command_does_not_call_get_executed_models(
        self, runner: CliRunner, cli_mocks: dict[str, Any]
    ) -> None:
        """Test that test command does NOT call get_executed_models.

        get_executed_models is only used for lineage extraction in run/build.
        """
        runner.invoke(
            cli,
            [
                "test",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
            ],
        )

        # get_executed_models should NOT be called for test command
        cli_mocks["get_executed"].assert_not_called()

    def test_test_command_only_emits_test_events_plus_terminal(
        self, runner: CliRunner, cli_mocks: dict[str, Any]
    ) -> None:
        """Test that test command emits only test events + terminal, no lineage."""
        cli_mocks["construct"].return_value = [
            cli_mocks["run_event"],
            cli_mocks["run_event"],
        ]

        runner.invoke(
            cli,
            [
                "test",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
            ],
        )

        # Second emit call should have test events + terminal only
        batch_events = cli_mocks["emit"].call_args_list[1][0][0]
        # 2 test events + 1 terminal = 3 events (no lineage!)
        assert len(batch_events) == 3

    def test_test_command_dataset_namespace_passed_to_test_events(
        self, runner: CliRunner, cli_mocks: dict[str, Any]
    ) -> None:
        """Test that --dataset-namespace is passed to construct_test_events."""
        runner.invoke(
            cli,
            [
                "test",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
                "--dataset-namespace",
                "postgresql://mydb.example.com:5432/analytics",
            ],
        )

        # Verify namespace_override was passed to construct_test_events
        call_kwargs = cli_mocks["construct"].call_args[1]
        assert (
            call_kwargs.get("namespace_override")
            == "postgresql://mydb.example.com:5432/analytics"
        )


# =============================================================================
# Tests for get_parent_run_metadata()
# =============================================================================


@pytest.mark.unit
class TestGetParentRunMetadata:
    """Tests for reading orchestrator parent context from environment variables.

    Validates parsing of OPENLINEAGE_PARENT_ID and OPENLINEAGE_ROOT_PARENT_ID
    environment variables set by Airflow's Jinja macros.
    """

    def test_reads_parent_id_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that OPENLINEAGE_PARENT_ID is correctly parsed."""
        monkeypatch.setenv(
            "OPENLINEAGE_PARENT_ID",
            "airflow/demo_pipeline.dbt_test/019c7c79-b160-7c2f-8ad4-a026c5a82b5a",
        )
        # Ensure root is not set (test parent-only behavior with fallback)
        monkeypatch.delenv("OPENLINEAGE_ROOT_PARENT_ID", raising=False)

        result = get_parent_run_metadata()

        assert result is not None
        assert result.job_namespace == "airflow"
        assert result.job_name == "demo_pipeline.dbt_test"
        assert result.run_id == "019c7c79-b160-7c2f-8ad4-a026c5a82b5a"

    def test_returns_none_when_not_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test returns None when not orchestrated (standalone CLI)."""
        monkeypatch.delenv("OPENLINEAGE_PARENT_ID", raising=False)
        monkeypatch.delenv("OPENLINEAGE_ROOT_PARENT_ID", raising=False)

        result = get_parent_run_metadata()

        assert result is None

    def test_returns_none_on_malformed_input(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test returns None and warns on malformed OPENLINEAGE_PARENT_ID."""
        monkeypatch.setenv("OPENLINEAGE_PARENT_ID", "invalid-format")

        result = get_parent_run_metadata()

        assert result is None

    def test_handles_airflow_default_namespace(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that airflow default namespace is parsed correctly."""
        monkeypatch.setenv(
            "OPENLINEAGE_PARENT_ID",
            "default/demo_pipeline.dbt_run/019c8582-5c58-7c59-a16f-5bd41c03f6cd",
        )
        monkeypatch.delenv("OPENLINEAGE_ROOT_PARENT_ID", raising=False)

        result = get_parent_run_metadata()

        assert result is not None
        assert result.job_namespace == "default"
        assert result.job_name == "demo_pipeline.dbt_run"
        assert result.run_id == "019c8582-5c58-7c59-a16f-5bd41c03f6cd"

    def test_handles_url_style_namespace(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that URL-style namespaces (e.g., airflow://demo) are parsed correctly."""
        monkeypatch.setenv(
            "OPENLINEAGE_PARENT_ID",
            "airflow://demo/demo_pipeline.dbt_run/019c8582-5c58-7c59-a16f-5bd41c03f6cd",
        )
        monkeypatch.delenv("OPENLINEAGE_ROOT_PARENT_ID", raising=False)

        result = get_parent_run_metadata()

        assert result is not None
        assert result.job_namespace == "airflow://demo"
        assert result.job_name == "demo_pipeline.dbt_run"
        assert result.run_id == "019c8582-5c58-7c59-a16f-5bd41c03f6cd"

    def test_handles_url_style_namespace_with_root(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test URL-style namespace parsing for both parent and root."""
        monkeypatch.setenv(
            "OPENLINEAGE_PARENT_ID",
            "airflow://demo/demo_pipeline.dbt_run/019c8582-5c58-7c59-a16f-5bd41c03f6cd",
        )
        monkeypatch.setenv(
            "OPENLINEAGE_ROOT_PARENT_ID",
            "airflow://demo/demo_pipeline/019c8582-5c58-7c59-a16f-5bd41c03f6cd",
        )

        result = get_parent_run_metadata()

        assert result is not None
        assert result.job_namespace == "airflow://demo"
        assert result.job_name == "demo_pipeline.dbt_run"
        assert result.root_job_namespace == "airflow://demo"
        assert result.root_job_name == "demo_pipeline"

    def test_reads_root_parent_id(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that OPENLINEAGE_ROOT_PARENT_ID is correctly parsed."""
        monkeypatch.setenv(
            "OPENLINEAGE_PARENT_ID",
            "airflow/demo_pipeline.dbt_test/019c7c79-b160-7c2f-8ad4-a026c5a82b5a",
        )
        monkeypatch.setenv(
            "OPENLINEAGE_ROOT_PARENT_ID",
            "airflow/demo_pipeline/019c7c79-aaaa-bbbb-cccc-111122223333",
        )

        result = get_parent_run_metadata()

        assert result is not None
        assert result.root_run_id == "019c7c79-aaaa-bbbb-cccc-111122223333"
        assert result.root_job_namespace == "airflow"
        assert result.root_job_name == "demo_pipeline"

    def test_root_falls_back_to_parent_when_not_set(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that root defaults to parent values when ROOT env var is unset.

        This matches dbt-ol behavior: when OPENLINEAGE_ROOT_PARENT_ID is not
        set, the parent values are reused as root.
        """
        monkeypatch.setenv(
            "OPENLINEAGE_PARENT_ID",
            "airflow/demo_pipeline.dbt_test/019c7c79-b160-7c2f-8ad4-a026c5a82b5a",
        )
        monkeypatch.delenv("OPENLINEAGE_ROOT_PARENT_ID", raising=False)

        result = get_parent_run_metadata()

        assert result is not None
        # Root should fall back to parent values
        assert result.root_run_id == "019c7c79-b160-7c2f-8ad4-a026c5a82b5a"
        assert result.root_job_namespace == "airflow"
        assert result.root_job_name == "demo_pipeline.dbt_test"

    def test_root_none_on_malformed_root(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that malformed ROOT env var results in None root fields."""
        monkeypatch.setenv(
            "OPENLINEAGE_PARENT_ID",
            "airflow/demo_pipeline.dbt_test/019c7c79-b160-7c2f-8ad4-a026c5a82b5a",
        )
        monkeypatch.setenv("OPENLINEAGE_ROOT_PARENT_ID", "malformed-value")

        result = get_parent_run_metadata()

        assert result is not None
        # Parent should still be parsed
        assert result.job_namespace == "airflow"
        # Root should be None due to malformed input
        assert result.root_run_id is None
        assert result.root_job_namespace is None
        assert result.root_job_name is None

    def test_returns_none_on_empty_string(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test returns None when OPENLINEAGE_PARENT_ID is empty string."""
        monkeypatch.setenv("OPENLINEAGE_PARENT_ID", "")

        result = get_parent_run_metadata()

        assert result is None


# =============================================================================
# Tests for execute_workflow() orchestrator parent integration
# =============================================================================


@pytest.mark.unit
class TestWorkflowOrchestratorParent:
    """Tests for orchestrator parent context in execute_workflow().

    Validates that OPENLINEAGE_PARENT_ID is read and passed to wrapping events.
    """

    def test_workflow_passes_orchestrator_parent_to_wrapping_events(
        self,
        runner: CliRunner,
        cli_mocks: dict[str, Any],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that wrapping events include parent when env var is set."""
        monkeypatch.setenv(
            "OPENLINEAGE_PARENT_ID",
            "airflow/demo_pipeline.dbt_test/019c7c79-b160-7c2f-8ad4-a026c5a82b5a",
        )
        monkeypatch.delenv("OPENLINEAGE_ROOT_PARENT_ID", raising=False)

        runner.invoke(
            cli,
            [
                "test",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
            ],
        )

        # Both START and terminal wrapping events should have parent
        assert cli_mocks["wrapping"].call_count == 2

        for call in cli_mocks["wrapping"].call_args_list:
            kwargs = call[1] if call[1] else {}
            parent = kwargs.get("parent")
            assert parent is not None
            assert isinstance(parent, ParentRunMetadata)
            assert parent.run_id == "019c7c79-b160-7c2f-8ad4-a026c5a82b5a"
            assert parent.job_namespace == "airflow"
            assert parent.job_name == "demo_pipeline.dbt_test"
            # Root falls back to parent when ROOT env var not set
            assert parent.root_run_id == "019c7c79-b160-7c2f-8ad4-a026c5a82b5a"
            assert parent.root_job_namespace == "airflow"
            assert parent.root_job_name == "demo_pipeline.dbt_test"

    def test_workflow_passes_separate_root_parent(
        self,
        runner: CliRunner,
        cli_mocks: dict[str, Any],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that separate root parent is passed when both env vars are set."""
        monkeypatch.setenv(
            "OPENLINEAGE_PARENT_ID",
            "airflow/demo_pipeline.dbt_test/019c7c79-b160-7c2f-8ad4-a026c5a82b5a",
        )
        monkeypatch.setenv(
            "OPENLINEAGE_ROOT_PARENT_ID",
            "airflow/demo_pipeline/019c7c79-aaaa-bbbb-cccc-111122223333",
        )

        runner.invoke(
            cli,
            [
                "test",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
            ],
        )

        for call in cli_mocks["wrapping"].call_args_list:
            kwargs = call[1] if call[1] else {}
            parent = kwargs.get("parent")
            assert parent is not None
            # Parent should be the task
            assert parent.run_id == "019c7c79-b160-7c2f-8ad4-a026c5a82b5a"
            assert parent.job_name == "demo_pipeline.dbt_test"
            # Root should be the DAG
            assert parent.root_run_id == "019c7c79-aaaa-bbbb-cccc-111122223333"
            assert parent.root_job_name == "demo_pipeline"

    def test_workflow_no_parent_when_env_not_set(
        self,
        runner: CliRunner,
        cli_mocks: dict[str, Any],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that wrapping events have no parent when standalone."""
        monkeypatch.delenv("OPENLINEAGE_PARENT_ID", raising=False)
        monkeypatch.delenv("OPENLINEAGE_ROOT_PARENT_ID", raising=False)

        runner.invoke(
            cli,
            [
                "test",
                "--correlator-endpoint",
                "http://localhost:8080/api/v1/lineage/events",
            ],
        )

        for call in cli_mocks["wrapping"].call_args_list:
            kwargs = call[1] if call[1] else {}
            assert kwargs.get("parent") is None
