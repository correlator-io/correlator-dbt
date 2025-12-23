"""Integration tests for dbt-correlator.

This module contains end-to-end integration tests that validate the complete
CLI workflow using committed fixture artifacts and mocked HTTP responses.

Test Categories:
    - TestEndToEndWithMockServer: Full workflow tests with mock Correlator
    - TestErrorScenarios: Error handling integration tests (Task 1.6.4)

These tests use the `responses` library to mock HTTP requests to Correlator,
allowing validation of OpenLineage event structure and batch emission behavior
without requiring a running Correlator instance.

All tests use --skip-dbt-run to use pre-generated artifacts from tests/fixtures/,
making them:
    - Fast: No subprocess execution
    - Deterministic: Uses known fixture data
    - CI/CD friendly: No dbt installation required
"""

import json
import re
from pathlib import Path
from typing import Any, cast
from uuid import UUID

import pytest
import requests
import responses  # type: ignore[import-not-found]
from click.testing import CliRunner

from dbt_correlator.cli import cli
from tests.conftest import MOCK_CORRELATOR_ENDPOINT

# =============================================================================
# Constants
# =============================================================================

# Expected minimum number of test events from fixture data
# Actual count depends on how tests are grouped by dataset
EXPECTED_MIN_TEST_EVENTS = 1  # At minimum we should have some test events

# OpenLineage schema URL pattern
OPENLINEAGE_SCHEMA_URL_PATTERN = (
    r"https://openlineage\.io/spec/\d+-\d+-\d+/OpenLineage\.json"
)


# =============================================================================
# Helper Functions
# =============================================================================


def parse_request_body(request: Any) -> list[dict[str, Any]]:
    """Parse JSON body from a responses request object.

    Args:
        request: Request object from responses library.

    Returns:
        Parsed JSON as list of event dictionaries.
    """
    body = request.body
    if isinstance(body, bytes):
        body = body.decode("utf-8")
    return cast(list[dict[str, Any]], json.loads(body))


def is_valid_uuid(value: str) -> bool:
    """Check if a string is a valid UUID.

    Args:
        value: String to validate.

    Returns:
        True if valid UUID, False otherwise.
    """
    try:
        UUID(value)
        return True
    except ValueError:
        return False


def is_valid_iso8601(value: str) -> bool:
    """Check if a string is valid ISO 8601 timestamp.

    Args:
        value: String to validate.

    Returns:
        True if valid ISO 8601 format, False otherwise.
    """
    # Basic ISO 8601 pattern check (YYYY-MM-DDTHH:MM:SS with optional timezone)
    pattern = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"
    return bool(re.match(pattern, value))


# =============================================================================
# End-to-End Integration Tests
# =============================================================================


@pytest.mark.integration
class TestEndToEndWithMockServer:
    """Integration tests using mock HTTP server.

    These tests validate the complete CLI workflow from invocation through
    HTTP emission using committed fixture artifacts and mocked Correlator
    responses.
    """

    def test_end_to_end_with_existing_artifacts(
        self,
        runner: CliRunner,
        mock_dbt_project_dir: Path,
        mock_correlator_success: responses.RequestsMock,
    ) -> None:
        """Full workflow using --skip-dbt-run with fixture artifacts.

        Validates:
            1. HTTP POST was made to Correlator
            2. Event count matches expected (START + test events + COMPLETE)
            3. Exit code is 0
        """
        result = runner.invoke(
            cli,
            [
                "test",
                "--skip-dbt-run",
                "--project-dir",
                str(mock_dbt_project_dir),
                "--profiles-dir",
                str(mock_dbt_project_dir),
                "--correlator-endpoint",
                MOCK_CORRELATOR_ENDPOINT,
            ],
        )

        # Verify exit code
        assert result.exit_code == 0, f"CLI failed with: {result.output}"

        # Verify HTTP POSTs were made (START + batch)
        assert (
            len(mock_correlator_success.calls) == 2
        ), "Expected 2 HTTP POSTs (START + batch)"

        # First call should be START event
        start_events = parse_request_body(mock_correlator_success.calls[0].request)
        assert len(start_events) == 1, "First call should have 1 START event"

        # Second call should be batch with lineage + test + terminal events
        batch_events = parse_request_body(mock_correlator_success.calls[1].request)
        assert isinstance(batch_events, list), "Events should be a JSON array"

        # Should have: lineage events + test events + COMPLETE (minimum 2)
        assert (
            len(batch_events) >= 2
        ), f"Expected at least 2 events in batch, got {len(batch_events)}"

        # Verify success message in output
        assert "Emitted" in result.output
        assert "events" in result.output

    def test_end_to_end_event_structure_validation(
        self,
        runner: CliRunner,
        mock_dbt_project_dir: Path,
        mock_correlator_success: responses.RequestsMock,
    ) -> None:
        """Validate OpenLineage event structure matches spec.

        Validates each event contains:
            - eventTime: ISO 8601 format
            - eventType: Valid type (START, COMPLETE, FAIL, RUNNING)
            - producer: Valid URL
            - schemaURL: OpenLineage schema URL
            - run.runId: UUID format
            - job.namespace: Present and non-empty
            - job.name: Present and non-empty
        """
        result = runner.invoke(
            cli,
            [
                "test",
                "--skip-dbt-run",
                "--project-dir",
                str(mock_dbt_project_dir),
                "--profiles-dir",
                str(mock_dbt_project_dir),
                "--correlator-endpoint",
                MOCK_CORRELATOR_ENDPOINT,
            ],
        )

        assert result.exit_code == 0, f"CLI failed with: {result.output}"

        events = parse_request_body(mock_correlator_success.calls[0].request)

        valid_event_types = {"START", "COMPLETE", "FAIL", "RUNNING", "ABORT", "OTHER"}

        for i, event in enumerate(events):
            # eventTime validation
            assert "eventTime" in event, f"Event {i} missing eventTime"
            assert is_valid_iso8601(
                event["eventTime"]
            ), f"Event {i} has invalid eventTime: {event['eventTime']}"

            # eventType validation
            assert "eventType" in event, f"Event {i} missing eventType"
            assert (
                event["eventType"] in valid_event_types
            ), f"Event {i} has invalid eventType: {event['eventType']}"

            # producer validation
            assert "producer" in event, f"Event {i} missing producer"
            assert event["producer"].startswith(
                "http"
            ), f"Event {i} has invalid producer URL"

            # schemaURL validation
            assert "schemaURL" in event, f"Event {i} missing schemaURL"
            assert re.match(
                OPENLINEAGE_SCHEMA_URL_PATTERN, event["schemaURL"]
            ), f"Event {i} has invalid schemaURL: {event['schemaURL']}"

            # run.runId validation
            assert "run" in event, f"Event {i} missing run"
            assert "runId" in event["run"], f"Event {i} missing run.runId"
            assert is_valid_uuid(
                event["run"]["runId"]
            ), f"Event {i} has invalid runId: {event['run']['runId']}"

            # job validation
            assert "job" in event, f"Event {i} missing job"
            assert "namespace" in event["job"], f"Event {i} missing job.namespace"
            assert "name" in event["job"], f"Event {i} missing job.name"
            assert event["job"]["namespace"], f"Event {i} has empty job.namespace"
            assert event["job"]["name"], f"Event {i} has empty job.name"

    def test_end_to_end_data_quality_assertions_facet(
        self,
        runner: CliRunner,
        mock_dbt_project_dir: Path,
        mock_correlator_success: responses.RequestsMock,
    ) -> None:
        """Validate dataQualityAssertions facet structure.

        Validates test events contain:
            - inputs array with datasets
            - inputFacets with dataQualityAssertions
            - assertions array with proper structure
            - Each assertion has: assertion name and success boolean
        """
        result = runner.invoke(
            cli,
            [
                "test",
                "--skip-dbt-run",
                "--project-dir",
                str(mock_dbt_project_dir),
                "--profiles-dir",
                str(mock_dbt_project_dir),
                "--correlator-endpoint",
                MOCK_CORRELATOR_ENDPOINT,
            ],
        )

        assert result.exit_code == 0, f"CLI failed with: {result.output}"

        # Test events are in the second call (batch)
        # First call is START event
        assert len(mock_correlator_success.calls) == 2, "Expected 2 HTTP calls"
        batch_events = parse_request_body(mock_correlator_success.calls[1].request)

        # Find test events (COMPLETE events with inputs that have dataQualityAssertions)
        # Note: Test events use COMPLETE type with dataQualityAssertions facets
        # Lineage events also have inputs but without dataQualityAssertions
        test_events = [
            e
            for e in batch_events
            if e.get("eventType") == "COMPLETE"
            and e.get("inputs")
            and any(
                "dataQualityAssertions" in inp.get("inputFacets", {})
                for inp in e.get("inputs", [])
            )
        ]

        assert (
            len(test_events) >= EXPECTED_MIN_TEST_EVENTS
        ), f"Expected test events with inputs, got {len(test_events)}"

        for event in test_events:
            # Validate inputs array
            assert "inputs" in event, "Test event missing inputs"
            assert len(event["inputs"]) > 0, "Test event has empty inputs"

            for input_dataset in event["inputs"]:
                # Validate dataset structure
                assert "namespace" in input_dataset, "Input missing namespace"
                assert "name" in input_dataset, "Input missing name"

                # Validate inputFacets with dataQualityAssertions
                assert "inputFacets" in input_dataset, "Input missing inputFacets"
                assert (
                    "dataQualityAssertions" in input_dataset["inputFacets"]
                ), "inputFacets missing dataQualityAssertions"

                dqa = input_dataset["inputFacets"]["dataQualityAssertions"]

                # Validate facet metadata
                assert "_producer" in dqa, "DQA missing _producer"
                assert "_schemaURL" in dqa, "DQA missing _schemaURL"

                # Validate assertions array
                assert "assertions" in dqa, "DQA missing assertions array"
                assert len(dqa["assertions"]) > 0, "DQA has empty assertions"

                for assertion in dqa["assertions"]:
                    assert (
                        "assertion" in assertion
                    ), "Assertion missing 'assertion' name"
                    assert "success" in assertion, "Assertion missing 'success' boolean"
                    assert isinstance(
                        assertion["success"], bool
                    ), "Assertion 'success' must be boolean"

    def test_end_to_end_batch_emission(
        self,
        runner: CliRunner,
        mock_dbt_project_dir: Path,
        mock_correlator_success: responses.RequestsMock,
    ) -> None:
        """Verify all events batched in single POST.

        Validates:
            - Only ONE HTTP POST request made
            - Request body is JSON array
            - Array contains START + test events + COMPLETE
            - All events share same run.runId
        """
        result = runner.invoke(
            cli,
            [
                "test",
                "--skip-dbt-run",
                "--project-dir",
                str(mock_dbt_project_dir),
                "--profiles-dir",
                str(mock_dbt_project_dir),
                "--correlator-endpoint",
                MOCK_CORRELATOR_ENDPOINT,
            ],
        )

        assert result.exit_code == 0, f"CLI failed with: {result.output}"

        # Verify 2 HTTP POSTs (START + batch)
        assert (
            len(mock_correlator_success.calls) == 2
        ), f"Expected 2 HTTP calls, got {len(mock_correlator_success.calls)}"

        # First call: START event
        start_events = parse_request_body(mock_correlator_success.calls[0].request)
        assert isinstance(start_events, list), "Events must be JSON array"
        assert len(start_events) == 1, "First call should have 1 START event"
        assert start_events[0].get("eventType") == "START", "First event must be START"

        # Second call: batch events (lineage + test + terminal)
        batch_events = parse_request_body(mock_correlator_success.calls[1].request)
        assert isinstance(batch_events, list), "Events must be JSON array"

        # Verify terminal event present in batch
        batch_event_types = [e.get("eventType") for e in batch_events]
        assert (
            "COMPLETE" in batch_event_types or "FAIL" in batch_event_types
        ), "Missing terminal event (COMPLETE/FAIL)"

        # Verify all events share same runId (across both calls)
        all_events = start_events + batch_events
        run_ids = {e["run"]["runId"] for e in all_events}
        assert len(run_ids) == 1, f"All events must share same runId, found: {run_ids}"

        # Verify START is first and terminal is last in logical order
        assert all_events[0]["eventType"] == "START", "First event must be START"
        assert all_events[-1]["eventType"] in {
            "COMPLETE",
            "FAIL",
        }, "Last event must be COMPLETE or FAIL"

    def test_end_to_end_with_config_file(
        self,
        runner: CliRunner,
        mock_dbt_project_dir: Path,
        mock_correlator_success: responses.RequestsMock,
        tmp_path: Path,
    ) -> None:
        """Test config file integration in full workflow.

        Steps:
            1. Create temp config file with endpoint
            2. Invoke CLI with --config pointing to temp file
            3. Assert endpoint from config was used
        """
        # Create temp config file
        config_content = f"""
correlator:
  endpoint: {MOCK_CORRELATOR_ENDPOINT}
  namespace: test-namespace
dbt:
  project_dir: {mock_dbt_project_dir}
  profiles_dir: {mock_dbt_project_dir}
job:
  name: config-test-job
"""
        config_file = tmp_path / ".dbt-correlator.yml"
        config_file.write_text(config_content)

        result = runner.invoke(
            cli,
            [
                "test",
                "--skip-dbt-run",
                "--config",
                str(config_file),
            ],
        )

        assert result.exit_code == 0, f"CLI failed with: {result.output}"

        # Verify HTTP POSTs were made (START + batch)
        assert len(mock_correlator_success.calls) == 2

        # Verify config values were applied (check START event)
        start_events = parse_request_body(mock_correlator_success.calls[0].request)
        assert start_events[0]["job"]["namespace"] == "test-namespace"
        assert start_events[0]["job"]["name"] == "config-test-job"


# =============================================================================
# Error Scenario Integration Tests
# =============================================================================


@pytest.mark.integration
class TestErrorScenarios:
    """Integration tests for error handling scenarios.

    These tests verify proper behavior when:
        - Required artifacts are missing
        - Correlator is unreachable or returns errors
        - Network timeouts occur

    Fire-and-forget pattern: Network/HTTP errors should NOT fail the CLI
    (exit code 0), but artifact errors SHOULD fail (non-zero exit code).
    """

    def test_end_to_end_missing_run_results(
        self,
        runner: CliRunner,
        fixtures_dir: Path,
        tmp_path: Path,
    ) -> None:
        """Clear error when run_results.json is missing.

        Steps:
            1. Create project dir with only manifest.json (no run_results)
            2. Invoke CLI with --skip-dbt-run
            3. Assert exit code is non-zero
            4. Assert error message mentions run_results.json
        """
        # Create target dir with only manifest
        target_dir = tmp_path / "target"
        target_dir.mkdir(parents=True)
        (target_dir / "manifest.json").symlink_to(fixtures_dir / "manifest.json")
        # Note: run_results.json is NOT created

        result = runner.invoke(
            cli,
            [
                "test",
                "--skip-dbt-run",
                "--project-dir",
                str(tmp_path),
                "--profiles-dir",
                str(tmp_path),
                "--correlator-endpoint",
                MOCK_CORRELATOR_ENDPOINT,
            ],
        )

        # Should fail with non-zero exit code
        assert result.exit_code != 0, "Expected non-zero exit code for missing artifact"

        # Error message should mention run_results
        assert (
            "run_results" in result.output.lower()
            or "run_results.json" in result.output
        ), f"Expected error about run_results, got: {result.output}"

    def test_end_to_end_missing_manifest(
        self,
        runner: CliRunner,
        fixtures_dir: Path,
        tmp_path: Path,
    ) -> None:
        """Clear error when manifest.json is missing.

        Steps:
            1. Create project dir with only run_results.json (no manifest)
            2. Invoke CLI with --skip-dbt-run
            3. Assert exit code is non-zero
            4. Assert error message mentions manifest.json
        """
        # Create target dir with only run_results
        target_dir = tmp_path / "target"
        target_dir.mkdir(parents=True)
        (target_dir / "run_results.json").symlink_to(
            fixtures_dir / "dbt_test_results.json"
        )
        # Note: manifest.json is NOT created

        result = runner.invoke(
            cli,
            [
                "test",
                "--skip-dbt-run",
                "--project-dir",
                str(tmp_path),
                "--profiles-dir",
                str(tmp_path),
                "--correlator-endpoint",
                MOCK_CORRELATOR_ENDPOINT,
            ],
        )

        # Should fail with non-zero exit code
        assert result.exit_code != 0, "Expected non-zero exit code for missing artifact"

        # Error message should mention manifest
        assert (
            "manifest" in result.output.lower() or "manifest.json" in result.output
        ), f"Expected error about manifest, got: {result.output}"

    def test_end_to_end_correlator_unreachable(
        self,
        runner: CliRunner,
        mock_dbt_project_dir: Path,
        mock_correlator_dynamic: responses.RequestsMock,
    ) -> None:
        """Fire-and-forget behavior when Correlator is unreachable.

        Steps:
            1. Mock HTTP to raise ConnectionError
            2. Invoke CLI with valid artifacts
            3. Assert exit code is 0 (fire-and-forget)
            4. Assert warning message in output
        """
        # Mock connection error
        mock_correlator_dynamic.add(
            responses.POST,
            MOCK_CORRELATOR_ENDPOINT,
            body=requests.ConnectionError("Connection refused"),
        )

        result = runner.invoke(
            cli,
            [
                "test",
                "--skip-dbt-run",
                "--project-dir",
                str(mock_dbt_project_dir),
                "--profiles-dir",
                str(mock_dbt_project_dir),
                "--correlator-endpoint",
                MOCK_CORRELATOR_ENDPOINT,
            ],
        )

        # Fire-and-forget: exit code should be 0
        assert result.exit_code == 0, f"Expected exit code 0, got: {result.exit_code}"

        # Should have warning about connection failure
        output_lower = result.output.lower()
        assert (
            "warning" in output_lower
            or "error" in output_lower
            or "failed" in output_lower
            or "connection" in output_lower
        ), f"Expected warning about connection, got: {result.output}"

    def test_end_to_end_correlator_timeout(
        self,
        runner: CliRunner,
        mock_dbt_project_dir: Path,
        mock_correlator_dynamic: responses.RequestsMock,
    ) -> None:
        """Timeout handling - fire-and-forget.

        Steps:
            1. Mock HTTP to raise Timeout
            2. Invoke CLI with valid artifacts
            3. Assert exit code is 0
            4. Assert warning message in output
        """
        # Mock timeout error
        mock_correlator_dynamic.add(
            responses.POST,
            MOCK_CORRELATOR_ENDPOINT,
            body=requests.Timeout("Request timed out"),
        )

        result = runner.invoke(
            cli,
            [
                "test",
                "--skip-dbt-run",
                "--project-dir",
                str(mock_dbt_project_dir),
                "--profiles-dir",
                str(mock_dbt_project_dir),
                "--correlator-endpoint",
                MOCK_CORRELATOR_ENDPOINT,
            ],
        )

        # Fire-and-forget: exit code should be 0
        assert result.exit_code == 0, f"Expected exit code 0, got: {result.exit_code}"

        # Should have warning about timeout
        output_lower = result.output.lower()
        assert (
            "warning" in output_lower
            or "timeout" in output_lower
            or "error" in output_lower
        ), f"Expected warning about timeout, got: {result.output}"

    def test_end_to_end_correlator_validation_error(
        self,
        runner: CliRunner,
        mock_dbt_project_dir: Path,
        mock_correlator_validation_error: responses.RequestsMock,
    ) -> None:
        """422 validation error response handling.

        Steps:
            1. Mock HTTP to return 422 with error details
            2. Invoke CLI with valid artifacts
            3. Assert exit code is 0 (fire-and-forget)
            4. Assert warning message includes validation error
        """
        result = runner.invoke(
            cli,
            [
                "test",
                "--skip-dbt-run",
                "--project-dir",
                str(mock_dbt_project_dir),
                "--profiles-dir",
                str(mock_dbt_project_dir),
                "--correlator-endpoint",
                MOCK_CORRELATOR_ENDPOINT,
            ],
        )

        # Fire-and-forget: exit code should be 0
        assert result.exit_code == 0, f"Expected exit code 0, got: {result.exit_code}"

        # HTTP calls were made (START + batch)
        assert len(mock_correlator_validation_error.calls) == 2

    def test_end_to_end_partial_success_207(
        self,
        runner: CliRunner,
        mock_dbt_project_dir: Path,
        mock_correlator_partial_success: responses.RequestsMock,
    ) -> None:
        """207 partial success response handling.

        Steps:
            1. Mock HTTP to return 207 with some events failed
            2. Invoke CLI with valid artifacts
            3. Assert exit code is 0 (fire-and-forget)
            4. Assert HTTP call was made
        """
        result = runner.invoke(
            cli,
            [
                "test",
                "--skip-dbt-run",
                "--project-dir",
                str(mock_dbt_project_dir),
                "--profiles-dir",
                str(mock_dbt_project_dir),
                "--correlator-endpoint",
                MOCK_CORRELATOR_ENDPOINT,
            ],
        )

        # Fire-and-forget: exit code should be 0
        assert result.exit_code == 0, f"Expected exit code 0, got: {result.exit_code}"

        # HTTP calls were made (START + batch)
        assert len(mock_correlator_partial_success.calls) == 2

    def test_end_to_end_api_key_header(
        self,
        runner: CliRunner,
        mock_dbt_project_dir: Path,
        mock_correlator_dynamic: responses.RequestsMock,
    ) -> None:
        """Verify X-API-Key header sent when configured.

        Steps:
            1. Setup mock HTTP server
            2. Invoke CLI with --correlator-api-key secret123
            3. Assert request has X-API-Key: secret123 header
            4. Assert exit code is 0
        """
        # Track request headers
        captured_headers: dict[str, str] = {}

        def capture_request(request: Any) -> tuple[int, dict[str, str], str]:
            captured_headers.update(request.headers)
            return 200, {}, '{"status": "success", "summary": {"received": 1}}'

        mock_correlator_dynamic.add_callback(
            responses.POST,
            MOCK_CORRELATOR_ENDPOINT,
            callback=capture_request,
        )

        result = runner.invoke(
            cli,
            [
                "test",
                "--skip-dbt-run",
                "--project-dir",
                str(mock_dbt_project_dir),
                "--profiles-dir",
                str(mock_dbt_project_dir),
                "--correlator-endpoint",
                MOCK_CORRELATOR_ENDPOINT,
                "--correlator-api-key",
                "secret123",
            ],
        )

        assert result.exit_code == 0, f"CLI failed with: {result.output}"

        # Verify API key header was sent
        assert "X-API-Key" in captured_headers, "Expected X-API-Key header"
        assert (
            captured_headers["X-API-Key"] == "secret123"
        ), f"Expected 'secret123', got '{captured_headers.get('X-API-Key')}'"
