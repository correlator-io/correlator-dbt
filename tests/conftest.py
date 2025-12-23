"""Shared test fixtures for dbt-correlator.

This module provides reusable pytest fixtures for integration testing,
including path fixtures for mock dbt projects and mock HTTP response
fixtures for Correlator API testing.

Fixtures:
    Path Fixtures:
        - fixtures_dir: Path to test fixtures directory
        - mock_dbt_project_dir: Mock dbt project directory with artifacts

    Mock HTTP Fixtures:
        - mock_correlator_success: Mock Correlator returning 200 OK
        - mock_correlator_partial_success: Mock returning 207 Multi-Status
        - mock_correlator_validation_error: Mock returning 422 Unprocessable Entity
        - mock_correlator_server_error: Mock returning 500 Internal Server Error

    CLI Fixtures:
        - runner: Click CliRunner for CLI testing
"""

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import responses  # type: ignore[import-not-found]
from click.testing import CliRunner

# =============================================================================
# Constants
# =============================================================================

# Default mock Correlator endpoint used in integration tests
MOCK_CORRELATOR_ENDPOINT = "http://localhost:8080/api/v1/lineage/events"


# =============================================================================
# Path Fixtures
# =============================================================================


@pytest.fixture
def fixtures_dir() -> Path:
    """Return path to test fixtures directory.

    Returns:
        Path to the fixtures directory containing committed test artifacts.
    """
    return Path(__file__).parent / "fixtures"


@pytest.fixture
def mock_dbt_project_dir(fixtures_dir: Path, tmp_path: Path) -> Path:
    """Create a mock dbt project directory with test results fixtures.

    The CLI expects artifacts at {project_dir}/target/. This fixture creates
    a temporary directory structure with symlinks to the committed fixtures:
        - {tmp_path}/target/manifest.json -> fixtures/manifest.json
        - {tmp_path}/target/run_results.json -> fixtures/dbt_test_results.json

    This fixture is used for testing the `test` command which expects
    test execution results.

    Args:
        fixtures_dir: Path to fixtures directory with committed artifacts.
        tmp_path: Pytest-provided temporary directory.

    Returns:
        Path to temporary project directory with proper structure.
    """
    manifest_path = fixtures_dir / "manifest.json"
    run_results_path = fixtures_dir / "dbt_test_results.json"

    if not manifest_path.exists():
        pytest.skip(f"Fixture not found: {manifest_path}")
    if not run_results_path.exists():
        pytest.skip(f"Fixture not found: {run_results_path}")

    # Create target directory structure
    target_dir = tmp_path / "target"
    target_dir.mkdir(parents=True, exist_ok=True)

    # Symlink artifacts (faster than copying)
    (target_dir / "manifest.json").symlink_to(manifest_path)
    # Symlink to dbt_test_results.json but CLI expects run_results.json in target/
    (target_dir / "run_results.json").symlink_to(run_results_path)

    return tmp_path


@pytest.fixture
def mock_dbt_project_dir_with_model_results(fixtures_dir: Path, tmp_path: Path) -> Path:
    """Create a mock dbt project directory with model run results fixtures.

    Similar to mock_dbt_project_dir but uses dbt_run_results.json which contains
    model execution results (from `dbt run`) instead of test results.

    This fixture is used for testing the `run` and `build` commands which
    expect model execution results with timing information.

    Args:
        fixtures_dir: Path to fixtures directory with committed artifacts.
        tmp_path: Pytest-provided temporary directory.

    Returns:
        Path to temporary project directory with proper structure.
    """
    manifest_path = fixtures_dir / "manifest.json"
    run_results_path = fixtures_dir / "dbt_run_results.json"

    if not manifest_path.exists():
        pytest.skip(f"Fixture not found: {manifest_path}")
    if not run_results_path.exists():
        pytest.skip(f"Fixture not found: {run_results_path}")

    # Create target directory structure
    target_dir = tmp_path / "target"
    target_dir.mkdir(parents=True, exist_ok=True)

    # Symlink artifacts (faster than copying)
    (target_dir / "manifest.json").symlink_to(manifest_path)
    (target_dir / "run_results.json").symlink_to(run_results_path)

    return tmp_path


# =============================================================================
# CLI Fixtures
# =============================================================================


@pytest.fixture
def runner() -> CliRunner:
    """Create Click test runner for CLI testing.

    Returns:
        CliRunner instance for CLI testing.
    """
    return CliRunner()


# =============================================================================
# Mock HTTP Response Fixtures
# =============================================================================


def _success_response_body(received: int = 1) -> dict[str, Any]:
    """Generate a success response body matching Correlator API format.

    Args:
        received: Number of events received.

    Returns:
        Dict matching Correlator's success response format.
    """
    return {
        "status": "success",
        "summary": {
            "received": received,
            "successful": received,
            "failed": 0,
            "retriable": 0,
            "non_retriable": 0,
        },
        "failed_events": [],
        "correlation_id": "test-correlation-id-123",
        "timestamp": "2024-01-01T12:00:00Z",
    }


def _partial_success_response_body(
    received: int = 10, failed: int = 2
) -> dict[str, Any]:
    """Generate a 207 partial success response body.

    Args:
        received: Total events received.
        failed: Number of events that failed.

    Returns:
        Dict matching Correlator's partial success response format.
    """
    return {
        "status": "partial_success",
        "summary": {
            "received": received,
            "successful": received - failed,
            "failed": failed,
            "retriable": 0,
            "non_retriable": failed,
        },
        "failed_events": [
            {
                "index": i,
                "reason": f"Validation error for event {i}",
                "retriable": False,
            }
            for i in range(failed)
        ],
        "correlation_id": "test-correlation-id-456",
        "timestamp": "2024-01-01T12:00:00Z",
    }


def _validation_error_response_body() -> dict[str, Any]:
    """Generate a 422 validation error response body.

    Returns:
        Dict matching Correlator's validation error response format.
    """
    return {
        "status": "error",
        "summary": {
            "received": 1,
            "successful": 0,
            "failed": 1,
            "retriable": 0,
            "non_retriable": 1,
        },
        "failed_events": [
            {
                "index": 0,
                "reason": "eventTime is required and cannot be zero value",
                "retriable": False,
            }
        ],
        "correlation_id": "test-correlation-id-789",
        "timestamp": "2024-01-01T12:00:00Z",
    }


@pytest.fixture
def mock_correlator_success() -> Iterator[responses.RequestsMock]:
    """Mock Correlator server returning 200 OK success response.

    Uses the responses library to mock HTTP POST requests to the
    Correlator lineage events endpoint.

    Yields:
        responses.RequestsMock context with configured mock.

    Example:
        def test_with_mock(mock_correlator_success):
            # HTTP POST to MOCK_CORRELATOR_ENDPOINT will return 200 OK
            response = requests.post(MOCK_CORRELATOR_ENDPOINT, json=[...])
            assert response.status_code == 200
    """
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            MOCK_CORRELATOR_ENDPOINT,
            json=_success_response_body(),
            status=200,
        )
        yield rsps


@pytest.fixture
def mock_correlator_partial_success() -> Iterator[responses.RequestsMock]:
    """Mock Correlator server returning 207 Multi-Status partial success.

    This fixture simulates the scenario where some events succeed and
    others fail validation.

    Yields:
        responses.RequestsMock context with configured mock.
    """
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            MOCK_CORRELATOR_ENDPOINT,
            json=_partial_success_response_body(received=10, failed=2),
            status=207,
        )
        yield rsps


@pytest.fixture
def mock_correlator_validation_error() -> Iterator[responses.RequestsMock]:
    """Mock Correlator server returning 422 Unprocessable Entity.

    This fixture simulates validation errors where all events fail.

    Yields:
        responses.RequestsMock context with configured mock.
    """
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            MOCK_CORRELATOR_ENDPOINT,
            json=_validation_error_response_body(),
            status=422,
        )
        yield rsps


@pytest.fixture
def mock_correlator_server_error() -> Iterator[responses.RequestsMock]:
    """Mock Correlator server returning 500 Internal Server Error.

    This fixture simulates server-side errors.

    Yields:
        responses.RequestsMock context with configured mock.
    """
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            MOCK_CORRELATOR_ENDPOINT,
            json={"error": "Internal server error"},
            status=500,
        )
        yield rsps


@pytest.fixture
def mock_correlator_dynamic() -> Iterator[responses.RequestsMock]:
    """Mock Correlator server with dynamic response based on request.

    This fixture provides a RequestsMock that can be configured by
    the test to add custom responses. Useful for tests that need
    to inspect the request body or customize responses.

    Yields:
        responses.RequestsMock context for custom configuration.

    Example:
        def test_custom_response(mock_correlator_dynamic):
            def callback(request):
                events = json.loads(request.body)
                return (200, {}, json.dumps(_success_response_body(len(events))))

            mock_correlator_dynamic.add_callback(
                responses.POST,
                MOCK_CORRELATOR_ENDPOINT,
                callback=callback
            )
    """
    with responses.RequestsMock() as rsps:
        yield rsps
