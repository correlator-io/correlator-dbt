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
from dbt_correlator.cli import cli
from dbt_correlator.parser import Manifest, RunResults, RunResultsMetadata, TestResult

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def runner() -> CliRunner:
    """Create Click test runner."""
    return CliRunner()


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
                execution_time=1.2,
                failures=0,
            ),
            TestResult(
                unique_id="test.my_project.not_null_users_email",
                status="fail",
                execution_time=0.8,
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

    This fixture eliminates the need to repeat 6 @patch decorators on every test.
    It sets up sensible defaults that can be overridden in individual tests.

    Returns:
        Dictionary with all mock objects for assertion access.
    """
    with (
        patch("dbt_correlator.cli.subprocess.run") as mock_subprocess,
        patch("dbt_correlator.cli.emit_events") as mock_emit,
        patch("dbt_correlator.cli.construct_events") as mock_construct,
        patch("dbt_correlator.cli.create_wrapping_event") as mock_wrapping,
        patch("dbt_correlator.cli.parse_manifest") as mock_parse_manifest,
        patch("dbt_correlator.cli.parse_run_results") as mock_parse_results,
    ):
        # Set default return values
        mock_subprocess.return_value = mock_completed_process_success
        mock_parse_results.return_value = mock_run_results
        mock_parse_manifest.return_value = mock_manifest
        mock_wrapping.return_value = mock_run_event
        mock_construct.return_value = [mock_run_event]

        yield {
            "subprocess": mock_subprocess,
            "emit": mock_emit,
            "construct": mock_construct,
            "wrapping": mock_wrapping,
            "parse_manifest": mock_parse_manifest,
            "parse_results": mock_parse_results,
            "run_results": mock_run_results,
            "manifest": mock_manifest,
            "run_event": mock_run_event,
        }


# =============================================================================
# A. Command Structure Tests
# =============================================================================


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
        call_args = cli_mocks["construct"].call_args
        assert call_args[0][0] == cli_mocks["run_results"]
        assert call_args[0][1] == cli_mocks["manifest"]


# =============================================================================
# D. Emission Tests
# =============================================================================


class TestEmission:
    """Tests for event emission to Correlator."""

    def test_test_command_emits_events_to_correlator(
        self, runner: CliRunner, cli_mocks: dict[str, Any]
    ) -> None:
        """Test that emit_events is called with Correlator endpoint."""
        endpoint = "http://localhost:8080/api/v1/lineage/events"
        runner.invoke(cli, ["test", "--correlator-endpoint", endpoint])

        cli_mocks["emit"].assert_called_once()
        call_args = cli_mocks["emit"].call_args
        assert call_args[0][1] == endpoint

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

        cli_mocks["emit"].assert_called_once()
        call_args = cli_mocks["emit"].call_args
        assert call_args[0][2] == api_key

    def test_test_command_batch_emits_all_events(
        self, runner: CliRunner, cli_mocks: dict[str, Any]
    ) -> None:
        """Test that all events are emitted in a single batch."""
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

        cli_mocks["emit"].assert_called_once()
        events = cli_mocks["emit"].call_args[0][0]
        # Should have: START (1) + test events (2) + COMPLETE (1) = 4 events
        assert len(events) == 4


# =============================================================================
# E. Exit Code Tests
# =============================================================================


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
        cli_mocks["parse_manifest"].assert_called_once()

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

        cli_mocks["emit"].assert_called_once()


# =============================================================================
# G. Error Handling Tests
# =============================================================================


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

        # Verify the endpoint from config file was used
        cli_mocks["emit"].assert_called_once()
        call_args = cli_mocks["emit"].call_args
        assert (
            call_args[0][1] == "http://config-file-endpoint:8080/api/v1/lineage/events"
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

        # CLI arg should override config file
        cli_mocks["emit"].assert_called_once()
        call_args = cli_mocks["emit"].call_args
        assert call_args[0][1] == "http://cli-override:8080/api/v1/lineage/events"

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

        cli_mocks["emit"].assert_called_once()
        call_args = cli_mocks["emit"].call_args
        assert call_args[0][1] == "http://from-env:8080/api/v1/lineage/events"

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

        # Verify auto-discovered endpoint was used
        cli_mocks["emit"].assert_called_once()
        call_args = cli_mocks["emit"].call_args
        assert call_args[0][1] == "http://auto-discovered:8080/api/v1/lineage/events"

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

        # Verify env var wins over config file
        cli_mocks["emit"].assert_called_once()
        call_args = cli_mocks["emit"].call_args
        assert call_args[0][1] == "http://from-env-var:8080/api/v1/lineage/events"
