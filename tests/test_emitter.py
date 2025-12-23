"""Tests for OpenLineage event emitter module.

This module contains unit and integration tests for constructing OpenLineage
events with dataQualityAssertions facets and emitting them to Correlator.

Test Coverage:
    - construct_test_events(): Build OpenLineage events with test results
    - emit_events(): Send events to OpenLineage backend
    - group_tests_by_dataset(): Group tests by target dataset
    - dataQualityAssertions facet structure validation
"""

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import attr
import pytest
import requests
from openlineage.client.event_v2 import RunState

from dbt_correlator.emitter import (
    construct_lineage_event,
    construct_lineage_events,
    construct_test_events,
    create_wrapping_event,
    emit_events,
    group_tests_by_dataset,
)
from dbt_correlator.parser import (
    DatasetInfo,
    Manifest,
    ModelExecutionResult,
    ModelLineage,
    RunResults,
    RunResultsMetadata,
    TestResult,
    parse_manifest,
    parse_run_results,
)

# Path to test fixtures
FIXTURES_DIR = Path(__file__).parent / "fixtures"
RUN_RESULTS_PATH = FIXTURES_DIR / "dbt_test_results.json"
MANIFEST_PATH = FIXTURES_DIR / "manifest.json"


@pytest.fixture
def sample_run_results():
    """Load sample run_results.json from fixtures."""
    return parse_run_results(str(RUN_RESULTS_PATH))


@pytest.fixture
def sample_manifest():
    """Load sample manifest.json from fixtures."""
    return parse_manifest(str(MANIFEST_PATH))


@pytest.fixture
def minimal_test_data():
    """Create minimal test data for emit_events() tests.

    Returns a tuple of (run_results, manifest) with one passing test.
    Used by emit_events() tests that need minimal valid data to construct events.
    """
    run_results = RunResults(
        metadata=RunResultsMetadata(
            generated_at=datetime.now(),
            invocation_id="test-invocation-id",
            dbt_version="1.10.15",
            elapsed_time=0.0,
        ),
        results=[
            TestResult(
                unique_id="test.pkg.not_null_test.abc123",
                status="pass",
                failures=0,
                message=None,
                execution_time_seconds=0.1,
            )
        ],
    )

    manifest = Manifest(
        metadata={
            "generated_at": datetime.now().isoformat(),
            "dbt_schema_version": "v12",
        },
        nodes={
            "test.pkg.not_null_test.abc123": {
                "unique_id": "test.pkg.not_null_test.abc123",
                "package_name": "pkg",
                "test_metadata": {"name": "not_null", "kwargs": {"column_name": "id"}},
                "refs": [{"name": "test_model"}],
            },
            "model.pkg.test_model": {
                "unique_id": "model.pkg.test_model",
                "database": "test_db",
                "schema": "test_schema",
                "name": "test_model",
            },
        },
        sources={},
    )

    return run_results, manifest


# ============================================================================
# Tests for group_tests_by_dataset()
# ============================================================================


@pytest.mark.unit
def test_group_tests_by_dataset_groups_by_dataset_urn(
    sample_run_results, sample_manifest
) -> None:
    """Test that tests are grouped by dataset URN.

    Validates that:
        - Tests on same dataset are grouped together
        - Tests on different datasets are separated
        - Dataset URN format is: namespace:name
        - Each group contains list of test results

    Note:
        Uses real fixtures from jaffle shop with 27 tests.
    """
    grouped = group_tests_by_dataset(sample_run_results, sample_manifest)

    # Should have multiple datasets (jaffle shop has tests on multiple models)
    assert isinstance(grouped, dict)
    assert len(grouped) > 0

    # Each key should be dataset URN string
    for dataset_urn, tests in grouped.items():
        assert isinstance(dataset_urn, str)
        assert ":" in dataset_urn, "Dataset URN should be 'namespace:name' format"

        # Each value should be list of test results
        assert isinstance(tests, list)
        assert len(tests) > 0

        # Each test should have required fields
        for test in tests:
            assert "unique_id" in test
            assert "status" in test
            assert test["status"] in {"pass", "fail", "error", "skipped", "warn"}


@pytest.mark.unit
def test_group_tests_by_dataset_multiple_tests_on_same_dataset(
    sample_run_results, sample_manifest
) -> None:
    """Test that multiple tests on same dataset are grouped together.

    Validates that:
        - Tests referencing same model are in same group
        - Group contains all relevant tests
        - No duplicate datasets created

    Note:
        Jaffle shop has models like 'customers' with multiple tests
        (e.g., unique, not_null tests on different columns).
    """
    grouped = group_tests_by_dataset(sample_run_results, sample_manifest)

    # Find a dataset with multiple tests (common in jaffle shop)
    datasets_with_multiple_tests = {
        urn: tests for urn, tests in grouped.items() if len(tests) > 1
    }

    assert (
        len(datasets_with_multiple_tests) > 0
    ), "Should have datasets with multiple tests"

    # Verify one dataset has multiple tests grouped correctly
    _dataset_urn, tests = next(iter(datasets_with_multiple_tests.items()))
    assert len(tests) >= 2

    # All tests should reference the same dataset URN
    for test in tests:
        assert isinstance(test["unique_id"], str)
        assert test["unique_id"].startswith("test.")


@pytest.mark.unit
def test_group_tests_by_dataset_empty_results() -> None:
    """Test grouping with no test results.

    Validates that:
        - Empty results return empty dictionary
        - No errors are raised
        - Handles gracefully
    """

    # Create empty run_results
    empty_run_results = RunResults(
        metadata=RunResultsMetadata(
            generated_at=datetime.now(),
            invocation_id="test-invocation-id",
            dbt_version="1.10.15",
            elapsed_time=0.0,
        ),
        results=[],
    )

    # Create minimal manifest
    empty_manifest = Manifest(
        metadata={
            "generated_at": datetime.now().isoformat(),
            "dbt_schema_version": "v12",
        },
        nodes={},
        sources={},
    )

    grouped = group_tests_by_dataset(empty_run_results, empty_manifest)

    assert isinstance(grouped, dict)
    assert len(grouped) == 0


@pytest.mark.unit
def test_group_tests_by_dataset_includes_test_metadata(
    sample_run_results, sample_manifest
) -> None:
    """Test that grouped results include necessary test metadata.

    Validates that each test result contains:
        - unique_id (for looking up in manifest)
        - status (pass/fail)
        - failures count
        - message (if present)
        - timing information

    This metadata is needed for constructing OpenLineage assertions.
    """
    grouped = group_tests_by_dataset(sample_run_results, sample_manifest)

    # Pick first dataset with tests
    _dataset_urn, tests = next(iter(grouped.items()))

    for test in tests:
        # Required fields for assertion construction
        assert "unique_id" in test, "unique_id needed to lookup test in manifest"
        assert "status" in test, "status needed for assertion.success"

        # Optional but common fields
        # Note: failures might be 0 or None for passing tests
        # message might be None for passing tests


# ============================================================================
# Tests for create_wrapping_event()
# ============================================================================


@pytest.mark.unit
def test_create_wrapping_event_start() -> None:
    """Test creation of START wrapping event.

    Validates that:
        - eventType is START
        - Event has run.runId
        - Event has job namespace and name
        - Event has no inputs/outputs (wrapping event)
        - Timestamp is properly formatted
    """

    run_id = str(uuid.uuid4())
    timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    event = create_wrapping_event(
        event_type="START",
        run_id=run_id,
        job_name="dbt_test",
        job_namespace="dbt",
        timestamp=timestamp,
    )

    # Verify event structure
    assert event.eventType == RunState.START
    assert event.run.runId == run_id
    assert event.job.namespace == "dbt"
    assert event.job.name == "dbt_test"
    assert len(event.inputs) == 0
    assert len(event.outputs) == 0
    assert "correlator-io/dbt-correlator" in event.producer


@pytest.mark.unit
def test_create_wrapping_event_complete() -> None:
    """Test creation of COMPLETE wrapping event.

    Validates that:
        - eventType is COMPLETE
        - Event structure matches START event
        - Can be used as terminal event
    """

    run_id = str(uuid.uuid4())
    timestamp = datetime(2024, 1, 1, 12, 5, 0, tzinfo=timezone.utc)

    event = create_wrapping_event(
        event_type="COMPLETE",
        run_id=run_id,
        job_name="dbt_test",
        job_namespace="dbt",
        timestamp=timestamp,
    )

    # Verify event structure
    assert event.eventType == RunState.COMPLETE
    assert event.run.runId == run_id
    assert event.job.namespace == "dbt"
    assert event.job.name == "dbt_test"
    assert len(event.inputs) == 0
    assert len(event.outputs) == 0


@pytest.mark.unit
def test_create_wrapping_event_fail() -> None:
    """Test creation of FAIL wrapping event.

    Validates that:
        - eventType is FAIL
        - Can be used when dbt test execution fails
        - Event structure consistent with other wrapping events
    """

    run_id = str(uuid.uuid4())
    timestamp = datetime(2024, 1, 1, 12, 10, 0, tzinfo=timezone.utc)

    event = create_wrapping_event(
        event_type="FAIL",
        run_id=run_id,
        job_name="dbt_test",
        job_namespace="dbt",
        timestamp=timestamp,
    )

    # Verify event structure
    assert event.eventType == RunState.FAIL
    assert event.run.runId == run_id
    assert len(event.inputs) == 0
    assert len(event.outputs) == 0


# ============================================================================
# Tests for construct_test_events()
# ============================================================================


@pytest.mark.unit
def test_construct_event_creates_valid_openlineage_event(
    sample_run_results, sample_manifest
) -> None:
    """Test construction of valid OpenLineage RunEvent.

    Validates that:
        - Returns RunEvent object (or dict with RunEvent structure)
        - eventType is "COMPLETE"
        - run.runId matches invocation_id from run_results
        - job namespace and name are set correctly
        - producer is "https://github.com/correlator-io/dbt-correlator/..."
        - schemaURL points to OpenLineage spec
        - inputs array contains datasets
        - outputs array is empty (tests validate inputs, not produce outputs)

    Note:
        Implementation will use openlineage-python client types.
    """
    events = construct_test_events(
        run_results=sample_run_results,
        manifest=sample_manifest,
        job_namespace="dbt",
        job_name="dbt_test_run",
        run_id=sample_run_results.metadata.invocation_id,
    )
    event = events[0]  # Extract first event for testing

    # Check event structure (always RunEvent object)
    assert event.eventType == RunState.COMPLETE
    assert event.run.runId == sample_run_results.metadata.invocation_id
    assert event.job.namespace == "dbt"
    assert event.job.name == "dbt_test_run"
    assert "correlator-io/dbt-correlator" in event.producer
    assert isinstance(event.inputs, list)
    assert len(event.inputs) > 0  # Should have datasets with tests
    assert isinstance(event.outputs, list)
    assert len(event.outputs) == 0  # Tests don't produce outputs


@pytest.mark.unit
def test_construct_event_includes_data_quality_assertions_facet(
    sample_run_results, sample_manifest
) -> None:
    """Test that datasets include dataQualityAssertions facet.

    Validates that:
        - Each input dataset has facets dictionary
        - facets contains "dataQualityAssertions" key
        - dataQualityAssertions has "assertions" array
        - Each assertion has required fields: assertion, success

    OpenLineage Spec Reference:
        https://openlineage.io/docs/spec/facets/dataset-facets/data-quality-assertions
    """
    events = construct_test_events(
        run_results=sample_run_results,
        manifest=sample_manifest,
        job_namespace="dbt",
        job_name="dbt_test_run",
        run_id="8b09d9b8-6506-438a-89ab-a51da1f0d891",
    )
    event = events[0]  # Extract first event for testing

    # Access inputs (always RunEvent object)
    assert len(event.inputs) > 0

    # Check first dataset has dataQualityAssertions facet
    first_dataset = event.inputs[0]

    # DataQualityAssertions is an InputDatasetFacet, so check inputFacets not facets
    assert (
        "dataQualityAssertions" in first_dataset.inputFacets
    ), "Missing dataQualityAssertions facet"

    dqa_facet = first_dataset.inputFacets["dataQualityAssertions"]

    # Check facet structure
    assert isinstance(dqa_facet.assertions, list)
    assert len(dqa_facet.assertions) > 0

    # Check first assertion has required fields
    first_assertion = dqa_facet.assertions[0]
    assert isinstance(first_assertion.assertion, str)
    assert isinstance(first_assertion.success, bool)


@pytest.mark.unit
def test_construct_event_assertion_structure_for_passed_test(
    sample_run_results, sample_manifest
) -> None:
    """Test assertion structure for passed tests.

    Validates that passed tests have:
        - success: True
        - assertion: test name (e.g., "not_null", "unique")
        - column: column name (if applicable)
        - failedCount: 0 or None
        - message: None or success message
    """
    events = construct_test_events(
        run_results=sample_run_results,
        manifest=sample_manifest,
        job_namespace="dbt",
        job_name="dbt_test_run",
        run_id="8b09d9b8-6506-438a-89ab-a51da1f0d891",
    )
    event = events[0]  # Extract first event for testing

    # Find a passing test assertion
    passing_assertion = None
    for dataset in event.inputs:
        dqa_facet = dataset.inputFacets.get("dataQualityAssertions")
        if dqa_facet:
            for assertion in dqa_facet.assertions:
                if assertion.success:
                    passing_assertion = assertion
                    break
        if passing_assertion:
            break

    assert passing_assertion is not None, "Should have at least one passing test"

    # Validate passing assertion structure
    assert passing_assertion.success is True
    assert isinstance(passing_assertion.assertion, str)
    assert len(passing_assertion.assertion) > 0


@pytest.mark.unit
def test_construct_event_uses_correct_event_time(
    sample_run_results, sample_manifest
) -> None:
    """Test that eventTime matches run_results generation timestamp.

    Validates that:
        - eventTime is set from run_results.metadata.generated_at
        - Timestamp is properly formatted (ISO 8601)
        - Critical for correlation - events must have accurate timestamps

    Note:
        eventTime represents when test execution completed.
        This enables temporal correlation with other lineage events.
    """
    events = construct_test_events(
        run_results=sample_run_results,
        manifest=sample_manifest,
        job_namespace="dbt",
        job_name="dbt_test_run",
        run_id="8b09d9b8-6506-438a-89ab-a51da1f0d891",
    )
    event = events[0]  # Extract first event for testing

    # Get expected timestamp from run_results
    expected_time = sample_run_results.metadata.generated_at

    # Extract eventTime from event (always string in ISO 8601 format)
    actual_time = event.eventTime
    assert isinstance(actual_time, str), "eventTime should be ISO 8601 string"

    # Should be UTC timezone aware or Z suffix
    assert (
        "Z" in actual_time or "+" in actual_time or actual_time.endswith(":00")
    ), "eventTime should be ISO 8601 with timezone"

    # Convert to datetime for comparison
    actual_dt = datetime.fromisoformat(actual_time.replace("Z", "+00:00"))
    assert (
        actual_dt == expected_time or actual_dt.replace(tzinfo=None) == expected_time
    ), f"eventTime {actual_time} should match run_results.generated_at {expected_time}"


@pytest.mark.unit
def test_construct_event_uses_invocation_id_as_run_id(
    sample_run_results, sample_manifest
) -> None:
    """Test that run.runId uses dbt invocation_id.

    Validates that:
        - run.runId matches run_results.metadata.invocation_id
        - Critical for correlation - Correlator creates canonical ID: dbt:{invocation_id}
        - Enables linking test results to job runs in Correlator

    Note:
        Correlator generates canonical job run ID in format: {namespace}:{runId}
        Example: dbt:550e8400-e29b-41d4-a716-446655440000

        This canonical ID is used across:
        - Lineage events (from dbt-ol or this plugin)
        - Test results (from this plugin)
        - Correlation queries

    Reference:
        Plugin Developer Guide, section "Canonical Job Run ID"
    """
    events = construct_test_events(
        run_results=sample_run_results,
        manifest=sample_manifest,
        job_namespace="dbt",
        job_name="dbt_test_run",
        run_id=sample_run_results.metadata.invocation_id,
    )
    event = events[0]  # Extract first event for testing

    # Get expected runId from run_results
    expected_run_id = sample_run_results.metadata.invocation_id

    # Extract run.runId from event (always RunEvent object)
    actual_run_id = event.run.runId

    # Validate runId matches invocation_id
    assert (
        actual_run_id == expected_run_id
    ), "run.runId should be invocation_id from run_results for correlation"

    # Validate runId is a valid UUID string or format
    assert isinstance(actual_run_id, str), "runId should be string"
    assert len(actual_run_id) > 0, "runId should not be empty"


@pytest.mark.unit
def test_construct_event_with_custom_namespace_and_job_name() -> None:
    """Test event construction with custom namespace and job name.

    Validates that:
        - Namespace parameter is respected
        - Job name parameter is respected
        - Different combinations work correctly
    """

    # Create minimal test data
    run_results = RunResults(
        metadata=RunResultsMetadata(
            generated_at=datetime.now(),
            invocation_id="test-invocation-123",
            dbt_version="1.10.15",
            elapsed_time=0.5,
        ),
        results=[
            TestResult(
                unique_id="test.jaffle_shop.not_null_customers_customer_id.5c9bf9911d",
                status="pass",
                failures=0,
                message=None,
                execution_time_seconds=0.5,
            )
        ],
    )

    manifest = Manifest(
        metadata={
            "generated_at": datetime.now().isoformat(),
            "dbt_schema_version": "v12",
        },
        nodes={
            "test.jaffle_shop.not_null_customers_customer_id.5c9bf9911d": {
                "unique_id": "test.jaffle_shop.not_null_customers_customer_id.5c9bf9911d",
                "package_name": "jaffle_shop",
                "test_metadata": {
                    "name": "not_null",
                    "kwargs": {"column_name": "customer_id"},
                },
                "refs": [{"name": "customers"}],
            },
            "model.jaffle_shop.customers": {
                "unique_id": "model.jaffle_shop.customers",
                "database": "jaffle_shop",
                "schema": "main",
                "name": "customers",
            },
        },
        sources={},
    )

    # Test custom job_namespace
    events = construct_test_events(
        run_results=run_results,
        manifest=manifest,
        job_namespace="production",
        job_name="nightly_dbt_tests",
        run_id="8b09d9b8-6506-438a-89ab-a51da1f0d891",
    )
    event = events[0]  # Extract first event for testing

    # Verify custom values used (always RunEvent object)
    assert event.job.namespace == "production"
    assert event.job.name == "nightly_dbt_tests"


@pytest.mark.unit
def test_construct_event_serializes_to_json(
    sample_run_results, sample_manifest
) -> None:
    """Test that constructed event can be serialized to JSON.

    Validates that:
        - Event can be converted to JSON string
        - JSON is valid and parseable
        - All required fields present in JSON
        - Dates are ISO 8601 formatted
        - UUIDs are strings

    This is critical for HTTP transmission to Correlator.
    """
    events = construct_test_events(
        run_results=sample_run_results,
        manifest=sample_manifest,
        job_namespace="dbt",
        job_name="dbt_test_run",
        run_id="8b09d9b8-6506-438a-89ab-a51da1f0d891",
    )
    event = events[0]  # Extract first event for testing

    # Try to serialize to JSON
    # v2 events use attrs, so use attr.asdict()
    event_dict = attr.asdict(event)

    json_str = json.dumps(event_dict, default=str)

    # Verify JSON is valid
    parsed = json.loads(json_str)

    assert "eventType" in parsed
    assert "run" in parsed
    assert "job" in parsed
    assert "inputs" in parsed
    assert "outputs" in parsed
    assert "producer" in parsed
    assert "schemaURL" in parsed


# ============================================================================
# Tests for emit_events()
# ============================================================================


@pytest.mark.unit
def test_emit_event_sends_post_request_to_correlator(minimal_test_data) -> None:
    """Test that emit_event sends HTTP POST to correct endpoint.

    Validates that:
        - HTTP POST request is made
        - URL matches correlator_endpoint parameter
        - Content-Type is application/json
        - Request body is serialized event
        - Events are wrapped in array (OpenLineage batch format)

    Note:
        Uses mock to avoid actual HTTP calls in unit tests.
    """
    run_results, manifest = minimal_test_data

    events = construct_test_events(
        run_results, manifest, "dbt", "test_job", "eb31681b-641b-4f73-bf93-cc339decae23"
    )

    # Mock HTTP request
    with patch("requests.post") as mock_post:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "status": "success",
            "summary": {"received": 1, "successful": 1, "failed": 0},
        }
        mock_post.return_value = mock_response

        # Emit event
        emit_events(events, "http://localhost:8080/api/v1/lineage/events")

        # Verify POST was called
        assert mock_post.called
        assert mock_post.call_count == 1

        # Verify URL
        call_args = mock_post.call_args
        assert call_args[0][0] == "http://localhost:8080/api/v1/lineage/events"

        # Verify headers include Content-Type
        headers = call_args[1].get("headers", {})
        assert "Content-Type" in headers or "content-type" in headers

        # Verify body is JSON array (OpenLineage batch format)
        json_data = call_args[1].get("json")
        assert isinstance(json_data, list), "Events must be wrapped in array"
        assert len(json_data) == 1


@pytest.mark.unit
def test_emit_event_handles_success_response(minimal_test_data) -> None:
    """Test successful event emission with 200 OK response.

    Validates that:
        - 200 response is handled correctly
        - No exceptions raised on success
        - Function completes normally
        - Response structure matches OpenLineage spec

    Note:
        This is the happy path - critical to test alongside error cases.
        OpenLineage spec defines success response format.

    Reference:
        Plugin Developer Guide, Response Format section (lines 320-340)
    """
    run_results, manifest = minimal_test_data

    events = construct_test_events(
        run_results, manifest, "dbt", "test_job", "eb31681b-641b-4f73-bf93-cc339decae23"
    )

    with patch("requests.post") as mock_post:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "status": "success",
            "summary": {
                "received": 1,
                "successful": 1,
                "failed": 0,
                "retriable": 0,
                "non_retriable": 0,
            },
            "failed_events": [],
            "correlation_id": "123e4567-e89b-12d3-a456-426614174000",
            "timestamp": "2024-01-01T12:00:00Z",
        }
        mock_post.return_value = mock_response

        # Should not raise any exceptions - this is success case
        emit_events(events, "http://localhost:8080/api/v1/lineage/events")

        # Verify request was made
        assert mock_post.called
        assert mock_post.call_count == 1


@pytest.mark.unit
def test_emit_event_handles_partial_success_response(minimal_test_data) -> None:
    """Test handling of 207 Multi-Status (partial success) response.

    Validates that:
        - 207 response indicates some events succeeded, some failed
        - Should not treat as complete failure
        - Handles gracefully (log warning but don't raise exception)
        - Response includes failed_events details

    Note:
        OpenLineage spec defines 207 for partial success in batch operations.
        This is a valid success scenario, not an error.

    Example scenario:
        - Sent batch of 10 events
        - 8 succeeded, 2 failed validation
        - Status code: 207
        - Plugin should acknowledge partial success

    Reference:
        Plugin Developer Guide, Response Format section (lines 342-366)
        OpenLineage spec: https://openlineage.io/apidocs/openapi/
    """
    run_results, manifest = minimal_test_data

    events = construct_test_events(
        run_results, manifest, "dbt", "test_job", "eb31681b-641b-4f73-bf93-cc339decae23"
    )

    with patch("requests.post") as mock_post:
        mock_response = MagicMock()
        mock_response.status_code = 207
        mock_response.json.return_value = {
            "status": "partial_success",
            "summary": {
                "received": 1,
                "successful": 0,
                "failed": 1,
                "retriable": 1,
                "non_retriable": 0,
            },
            "failed_events": [
                {"index": 0, "reason": "transient database error", "retriable": True}
            ],
            "correlation_id": "123e4567-e89b-12d3-a456-426614174000",
            "timestamp": "2024-01-01T12:00:00Z",
        }
        mock_post.return_value = mock_response

        # Should handle partial success gracefully
        # Implementation may log warning but should not raise exception
        # (207 is success status, not error status)
        emit_events(events, "http://localhost:8080/api/v1/lineage/events")

        # Verify request was made
        assert mock_post.called


@pytest.mark.unit
def test_emit_event_with_api_key_includes_header(minimal_test_data) -> None:
    """Test that API key is included in request headers.

    Validates that:
        - API key is sent in X-API-Key header
        - Header format matches Correlator expectation
        - Request still works with API key

    Reference:
        Plugin Developer Guide (notes/plugin-developer-guide.md)
        Authentication section.
    """
    run_results, manifest = minimal_test_data

    events = construct_test_events(
        run_results, manifest, "dbt", "test_job", "eb31681b-641b-4f73-bf93-cc339decae23"
    )

    with patch("requests.post") as mock_post:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "status": "success",
            "summary": {"received": 1, "successful": 1, "failed": 0},
        }
        mock_post.return_value = mock_response

        # Emit with API key
        emit_events(
            events,
            "http://localhost:8080/api/v1/lineage/events",
            api_key="test-api-key-123",
        )

        # Verify API key in headers
        call_args = mock_post.call_args
        headers = call_args[1].get("headers", {})

        # Check for X-API-Key header (case-insensitive)
        api_key_found = False
        for key, value in headers.items():
            if key.lower() == "x-api-key":
                assert value == "test-api-key-123"
                api_key_found = True
                break

        assert api_key_found, "X-API-Key header not found"


@pytest.mark.unit
def test_emit_event_handles_connection_error(minimal_test_data) -> None:
    """Test error handling when Correlator is unreachable.

    Validates that:
        - ConnectionError is raised with helpful message
        - Error message suggests checking Correlator status
        - Original error is preserved (exception chaining)
    """
    run_results, manifest = minimal_test_data

    events = construct_test_events(
        run_results, manifest, "dbt", "test_job", "eb31681b-641b-4f73-bf93-cc339decae23"
    )

    # Mock connection error
    with patch("requests.post") as mock_post:
        mock_post.side_effect = requests.ConnectionError("Connection refused")

        # Should raise ConnectionError with helpful message
        with pytest.raises(ConnectionError) as exc_info:
            emit_events(events, "http://localhost:8080/api/v1/lineage/events")

        error_message = str(exc_info.value)
        assert "localhost:8080" in error_message or "Correlator" in error_message


@pytest.mark.unit
def test_emit_event_handles_http_error_response(minimal_test_data) -> None:
    """Test error handling for HTTP error responses from Correlator.

    Validates that:
        - ValueError raised for 4xx/5xx responses
        - Error includes status code
        - Error includes response body if available
        - Helpful context provided

    Common errors:
        - 400: Invalid JSON
        - 422: Validation error
        - 500: Server error
    """
    run_results, manifest = minimal_test_data

    events = construct_test_events(
        run_results, manifest, "dbt", "test_job", "eb31681b-641b-4f73-bf93-cc339decae23"
    )

    # Mock 422 validation error
    with patch("requests.post") as mock_post:
        mock_response = MagicMock()
        mock_response.status_code = 422
        mock_response.json.return_value = {
            "status": "error",
            "summary": {"received": 1, "successful": 0, "failed": 1},
            "failed_events": [
                {"index": 0, "reason": "eventTime is required", "retriable": False}
            ],
        }
        mock_response.text = json.dumps(mock_response.json.return_value)
        mock_post.return_value = mock_response

        # Should raise ValueError
        with pytest.raises(ValueError, match="422") as exc_info:
            emit_events(events, "http://localhost:8080/api/v1/lineage/events")

        error_message = str(exc_info.value)
        assert "422" in error_message


@pytest.mark.unit
def test_emit_event_handles_timeout(minimal_test_data) -> None:
    """Test error handling for request timeout.

    Validates that:
        - TimeoutError raised when request times out
        - Error message is helpful
        - Suggests checking network/Correlator performance
    """
    run_results, manifest = minimal_test_data

    events = construct_test_events(
        run_results, manifest, "dbt", "test_job", "eb31681b-641b-4f73-bf93-cc339decae23"
    )

    # Mock timeout
    with patch("requests.post") as mock_post:
        mock_post.side_effect = requests.Timeout("Request timed out")

        # Should raise TimeoutError
        with pytest.raises(TimeoutError) as exc_info:
            emit_events(events, "http://localhost:8080/api/v1/lineage/events")

        error_message = str(exc_info.value)
        # Should mention timeout
        assert (
            "timeout" in error_message.lower() or "timed out" in error_message.lower()
        )


@pytest.mark.unit
def test_emit_events_handles_empty_list() -> None:
    """Test that empty event list doesn't make HTTP call.

    Validates that:
        - Empty list is handled gracefully
        - No HTTP request made for empty batch
        - Returns without error
        - Debug logging occurs

    This is important for efficiency - no unnecessary HTTP calls.
    """
    with patch("requests.post") as mock_post:
        # Call with empty list
        emit_events([], "http://localhost:8080/api/v1/lineage/events")

        # Verify no HTTP call was made
        assert mock_post.call_count == 0, "Should not make HTTP call for empty batch"


@pytest.mark.integration
def test_emit_event_integration_with_correlator() -> None:
    """Integration test: emit event to real Correlator instance.

    Requirements:
        - Correlator running on localhost:8080
        - Database initialized
        - Migrations applied

    Validates that:
        - Event successfully sent to Correlator
        - Response status is 200
        - Response indicates success
        - Events stored in database (manual verification)

    Note:
        This test requires Correlator to be running.
        Use pytest markers to skip if not available:
            pytest -m "not integration"
    """
    pytest.skip("Integration test - requires Correlator running on localhost:8080")

    # This test will be implemented when we want to test with real Correlator
    # For now, we skip to avoid test failures in CI/CD


# ============================================================================
# Tests for construct_lineage_event()
# ============================================================================


@pytest.fixture
def sample_model_lineage():
    """Create sample ModelLineage for testing lineage event construction."""
    return ModelLineage(
        unique_id="model.jaffle_shop.customers",
        name="customers",
        inputs=[
            DatasetInfo(namespace="duckdb://jaffle_shop", name="main.stg_customers"),
            DatasetInfo(namespace="duckdb://jaffle_shop", name="main.stg_orders"),
        ],
        output=DatasetInfo(namespace="duckdb://jaffle_shop", name="main.customers"),
    )


@pytest.fixture
def sample_model_execution_result():
    """Create sample ModelExecutionResult for testing runtime metrics."""
    return ModelExecutionResult(
        unique_id="model.jaffle_shop.customers",
        status="success",
        execution_time_seconds=1.5,
        rows_affected=1500,
        message="OK",
    )


@pytest.mark.unit
def test_construct_lineage_event_creates_complete_event(sample_model_lineage) -> None:
    """Test that construct_lineage_event creates valid OpenLineage COMPLETE event.

    Validates that:
        - eventType is COMPLETE
        - run.runId matches provided run_id
        - job namespace and name are set correctly
        - producer is set to dbt-correlator
        - Event structure is valid OpenLineage RunEvent
    """
    run_id = "550e8400-e29b-41d4-a716-446655440000"
    event_time = "2024-01-01T12:00:00Z"

    event = construct_lineage_event(
        model_lineage=sample_model_lineage,
        run_id=run_id,
        job_namespace="dbt",
        producer="https://github.com/correlator-io/dbt-correlator/0.1.0",
        event_time=event_time,
    )

    # Verify event structure
    assert event.eventType == RunState.COMPLETE
    assert event.run.runId == run_id
    assert event.job.namespace == "dbt"
    assert event.job.name == "model.jaffle_shop.customers"
    assert event.eventTime == event_time
    assert "correlator-io/dbt-correlator" in event.producer


@pytest.mark.unit
def test_construct_lineage_event_includes_inputs(sample_model_lineage) -> None:
    """Test that lineage event includes input datasets from ModelLineage.

    Validates that:
        - inputs array is populated from ModelLineage.inputs
        - Each input has correct namespace and name
        - Input count matches ModelLineage.inputs count
    """
    event = construct_lineage_event(
        model_lineage=sample_model_lineage,
        run_id="550e8400-e29b-41d4-a716-446655440001",
        job_namespace="dbt",
        producer="https://test/producer",
        event_time="2024-01-01T12:00:00Z",
    )

    # Verify inputs
    assert len(event.inputs) == 2
    assert event.inputs[0].namespace == "duckdb://jaffle_shop"
    assert event.inputs[0].name == "main.stg_customers"
    assert event.inputs[1].namespace == "duckdb://jaffle_shop"
    assert event.inputs[1].name == "main.stg_orders"


@pytest.mark.unit
def test_construct_lineage_event_includes_output(sample_model_lineage) -> None:
    """Test that lineage event includes output dataset from ModelLineage.

    Validates that:
        - outputs array has single entry (the model output)
        - Output has correct namespace and name from ModelLineage.output
    """
    event = construct_lineage_event(
        model_lineage=sample_model_lineage,
        run_id="550e8400-e29b-41d4-a716-446655440002",
        job_namespace="dbt",
        producer="https://test/producer",
        event_time="2024-01-01T12:00:00Z",
    )

    # Verify outputs
    assert len(event.outputs) == 1
    assert event.outputs[0].namespace == "duckdb://jaffle_shop"
    assert event.outputs[0].name == "main.customers"


@pytest.mark.unit
def test_construct_lineage_event_includes_runtime_metrics(
    sample_model_lineage, sample_model_execution_result
) -> None:
    """Test that lineage event includes outputStatistics facet with row count.

    Validates that:
        - Output dataset has outputFacets with outputStatistics
        - outputStatistics contains rowCount from ModelExecutionResult
        - Facet structure follows OpenLineage spec

    Reference:
        https://openlineage.io/docs/spec/facets/dataset-facets/output-statistics
    """
    event = construct_lineage_event(
        model_lineage=sample_model_lineage,
        run_id="550e8400-e29b-41d4-a716-446655440002",
        job_namespace="dbt",
        producer="https://test/producer",
        event_time="2024-01-01T12:00:00Z",
        execution_result=sample_model_execution_result,
    )

    # Verify output has outputStatistics facet
    output = event.outputs[0]
    assert output.outputFacets is not None
    assert "outputStatistics" in output.outputFacets

    # Verify facet structure
    stats_facet = output.outputFacets["outputStatistics"]
    assert stats_facet.rowCount == 1500


@pytest.mark.unit
def test_construct_lineage_event_without_runtime_metrics(sample_model_lineage) -> None:
    """Test that lineage event works without runtime metrics.

    Validates that:
        - Event is valid without execution_result parameter
        - Output dataset has no outputStatistics facet
        - Used for `dbt test` command (no model execution metrics)
    """
    event = construct_lineage_event(
        model_lineage=sample_model_lineage,
        run_id="550e8400-e29b-41d4-a716-446655440002",
        job_namespace="dbt",
        producer="https://test/producer",
        event_time="2024-01-01T12:00:00Z",
        # No execution_result provided
    )

    # Verify output exists but has no outputStatistics
    output = event.outputs[0]
    # outputFacets should be None or empty when no metrics provided
    assert output.outputFacets is None or "outputStatistics" not in (
        output.outputFacets or {}
    )


@pytest.mark.unit
def test_construct_lineage_event_with_no_inputs() -> None:
    """Test that lineage event handles models with no inputs.

    Validates that:
        - Event is valid when ModelLineage has empty inputs list
        - outputs still populated correctly
        - No errors raised
    """
    # Model with no dependencies (e.g., seed-based or hardcoded)
    lineage_no_inputs = ModelLineage(
        unique_id="model.jaffle_shop.seed_based_model",
        name="seed_based_model",
        inputs=[],  # No upstream dependencies
        output=DatasetInfo(namespace="duckdb://db", name="main.seed_based_model"),
    )

    event = construct_lineage_event(
        model_lineage=lineage_no_inputs,
        run_id="550e8400-e29b-41d4-a716-446655440002",
        job_namespace="dbt",
        producer="https://test/producer",
        event_time="2024-01-01T12:00:00Z",
    )

    # Verify event structure
    assert len(event.inputs) == 0
    assert len(event.outputs) == 1
    assert event.outputs[0].name == "main.seed_based_model"


@pytest.mark.unit
def test_construct_lineage_event_serializes_to_json(
    sample_model_lineage, sample_model_execution_result
) -> None:
    """Test that lineage event can be serialized to JSON for HTTP transmission.

    Validates that:
        - Event with OutputDataset serializes correctly
        - outputStatistics facet serializes correctly
        - JSON is valid and parseable
        - All required OpenLineage fields present

    This is critical because OutputDataset/outputFacets have different
    serialization than InputDataset/inputFacets used in test events.
    """
    event = construct_lineage_event(
        model_lineage=sample_model_lineage,
        run_id="550e8400-e29b-41d4-a716-446655440005",
        job_namespace="dbt",
        producer="https://github.com/correlator-io/dbt-correlator/0.1.0",
        event_time="2024-01-01T12:00:00Z",
        execution_result=sample_model_execution_result,
    )

    # Serialize to dict (same as emit_events does)
    event_dict = attr.asdict(event)

    # Serialize to JSON string
    json_str = json.dumps(event_dict, default=str)

    # Verify JSON is valid and parseable
    parsed = json.loads(json_str)

    # Verify required OpenLineage fields
    assert "eventType" in parsed
    assert "run" in parsed
    assert "job" in parsed
    assert "inputs" in parsed
    assert "outputs" in parsed
    assert "producer" in parsed

    # Verify output structure (different from input)
    assert len(parsed["outputs"]) == 1
    output = parsed["outputs"][0]
    assert "namespace" in output
    assert "name" in output
    assert "outputFacets" in output

    # Verify outputStatistics facet serialized correctly
    assert "outputStatistics" in output["outputFacets"]
    stats = output["outputFacets"]["outputStatistics"]
    assert stats["rowCount"] == 1500


# ============================================================================
# Tests for construct_lineage_events()
# ============================================================================


@pytest.fixture
def sample_model_lineages():
    """Create list of sample ModelLineage objects for batch testing."""
    return [
        ModelLineage(
            unique_id="model.jaffle_shop.stg_customers",
            name="stg_customers",
            inputs=[
                DatasetInfo(
                    namespace="duckdb://jaffle_shop", name="main.raw_customers"
                ),
            ],
            output=DatasetInfo(
                namespace="duckdb://jaffle_shop", name="main.stg_customers"
            ),
        ),
        ModelLineage(
            unique_id="model.jaffle_shop.customers",
            name="customers",
            inputs=[
                DatasetInfo(
                    namespace="duckdb://jaffle_shop", name="main.stg_customers"
                ),
            ],
            output=DatasetInfo(namespace="duckdb://jaffle_shop", name="main.customers"),
        ),
    ]


@pytest.mark.unit
def test_construct_lineage_events_creates_event_per_model(
    sample_model_lineages,
) -> None:
    """Test that construct_lineage_events creates one event per model.

    Validates that:
        - Returns list of RunEvents
        - Event count matches input model count
        - Each event corresponds to a model from input list
    """
    events = construct_lineage_events(
        model_lineages=sample_model_lineages,
        run_id="550e8400-e29b-41d4-a716-446655440003",
        job_namespace="dbt",
        producer="https://test/producer",
        event_time="2024-01-01T12:00:00Z",
    )

    # Verify event count
    assert len(events) == 2

    # Verify each event corresponds to a model
    job_names = {event.job.name for event in events}
    assert "model.jaffle_shop.stg_customers" in job_names
    assert "model.jaffle_shop.customers" in job_names


@pytest.mark.unit
def test_construct_lineage_events_all_share_same_run_id(sample_model_lineages) -> None:
    """Test that all lineage events share the same run_id.

    Validates that:
        - All events have identical run.runId
        - Critical for correlation in Correlator
    """
    run_id = "550e8400-e29b-41d4-a716-446655440004"

    events = construct_lineage_events(
        model_lineages=sample_model_lineages,
        run_id=run_id,
        job_namespace="dbt",
        producer="https://test/producer",
        event_time="2024-01-01T12:00:00Z",
    )

    # All events should share the same runId
    for event in events:
        assert event.run.runId == run_id


@pytest.mark.unit
def test_construct_lineage_events_with_execution_results(
    sample_model_lineages,
) -> None:
    """Test that execution results are matched to correct models.

    Validates that:
        - Execution results are correctly matched by unique_id
        - Models with results get outputStatistics facet
        - Models without results don't get facet
    """
    # Only provide execution result for one model
    execution_results = {
        "model.jaffle_shop.customers": ModelExecutionResult(
            unique_id="model.jaffle_shop.customers",
            status="success",
            execution_time_seconds=2.5,
            rows_affected=2500,
            message="OK",
        )
    }

    events = construct_lineage_events(
        model_lineages=sample_model_lineages,
        run_id="550e8400-e29b-41d4-a716-446655440002",
        job_namespace="dbt",
        producer="https://test/producer",
        event_time="2024-01-01T12:00:00Z",
        execution_results=execution_results,
    )

    # Find the customers event (should have metrics)
    customers_event = next(
        e for e in events if e.job.name == "model.jaffle_shop.customers"
    )
    stg_customers_event = next(
        e for e in events if e.job.name == "model.jaffle_shop.stg_customers"
    )

    # customers should have outputStatistics
    assert customers_event.outputs[0].outputFacets is not None
    assert "outputStatistics" in customers_event.outputs[0].outputFacets
    assert customers_event.outputs[0].outputFacets["outputStatistics"].rowCount == 2500

    # stg_customers should NOT have outputStatistics
    assert stg_customers_event.outputs[0].outputFacets is None or (
        "outputStatistics" not in (stg_customers_event.outputs[0].outputFacets or {})
    )


@pytest.mark.unit
def test_construct_lineage_events_empty_list() -> None:
    """Test that empty model list returns empty event list.

    Validates that:
        - Empty input returns empty output
        - No errors raised
        - Handles gracefully
    """
    events = construct_lineage_events(
        model_lineages=[],
        run_id="550e8400-e29b-41d4-a716-446655440002",
        job_namespace="dbt",
        producer="https://test/producer",
        event_time="2024-01-01T12:00:00Z",
    )

    assert events == []


# ============================================================================
# Tests for emit_events() OpenLineage Consumer Compatibility
# ============================================================================


@pytest.mark.unit
def test_emit_events_handles_204_no_content(minimal_test_data) -> None:
    """Test handling of 204 No Content response from OL consumer.

    Validates that:
        - 204 response is treated as success
        - No exceptions raised
        - Compatible with generic OpenLineage consumers

    Note:
        Some OpenLineage consumers return 204 with no body as acknowledgment.
    """
    run_results, manifest = minimal_test_data

    events = construct_test_events(
        run_results, manifest, "dbt", "test_job", "eb31681b-641b-4f73-bf93-cc339decae23"
    )

    with patch("requests.post") as mock_post:
        mock_response = MagicMock()
        mock_response.status_code = 204
        mock_response.text = ""  # No content
        mock_post.return_value = mock_response

        # Should succeed without exception
        emit_events(events, "http://localhost:5000/api/v1/lineage")

        assert mock_post.called


@pytest.mark.unit
def test_emit_events_handles_200_with_body(minimal_test_data) -> None:
    """Test handling of 200 response with body from OL consumer.

    Validates that:
        - 200 with JSON body is handled correctly
        - Summary is logged if present
        - Compatible with OpenLineage consumers that return summary
    """
    run_results, manifest = minimal_test_data

    events = construct_test_events(
        run_results, manifest, "dbt", "test_job", "eb31681b-641b-4f73-bf93-cc339decae23"
    )

    with patch("requests.post") as mock_post:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"summary": {"successful": 1, "failed": 0}}'
        mock_response.json.return_value = {"summary": {"successful": 1, "failed": 0}}
        mock_post.return_value = mock_response

        # Should succeed without exception
        emit_events(events, "http://localhost:5000/api/v1/lineage")

        assert mock_post.called


@pytest.mark.unit
def test_emit_events_handles_200_without_body(minimal_test_data) -> None:
    """Test handling of 200 response without body from Correlator.

    Validates that:
        - 200 with empty body is treated as success
        - Compatible with Correlator backend
        - No JSON parsing errors
    """
    run_results, manifest = minimal_test_data

    events = construct_test_events(
        run_results, manifest, "dbt", "test_job", "eb31681b-641b-4f73-bf93-cc339decae23"
    )

    with patch("requests.post") as mock_post:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = ""  # Empty body
        mock_post.return_value = mock_response

        # Should succeed without exception
        emit_events(events, "http://localhost:8080/api/v1/lineage/events")

        assert mock_post.called


@pytest.mark.unit
def test_emit_events_with_lineage_events(sample_model_lineage) -> None:
    """Test that lineage events serialize and emit correctly via HTTP.

    Validates that:
        - Lineage events (with OutputDataset) emit correctly
        - outputFacets serialize properly (different from inputFacets)
        - Request body contains valid JSON array
        - No serialization errors during emission

    This test ensures the full path from construct_lineage_event()
    through emit_events() works correctly.
    """
    # Create lineage event with runtime metrics
    execution_result = ModelExecutionResult(
        unique_id="model.jaffle_shop.customers",
        status="success",
        execution_time_seconds=2.0,
        rows_affected=1000,
        message="OK",
    )

    event = construct_lineage_event(
        model_lineage=sample_model_lineage,
        run_id="550e8400-e29b-41d4-a716-446655440006",
        job_namespace="dbt",
        producer="https://github.com/correlator-io/dbt-correlator/0.1.0",
        event_time="2024-01-01T12:00:00Z",
        execution_result=execution_result,
    )

    with patch("requests.post") as mock_post:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"summary": {"successful": 1, "failed": 0}}'
        mock_response.json.return_value = {"summary": {"successful": 1, "failed": 0}}
        mock_post.return_value = mock_response

        # Emit lineage event
        emit_events([event], "http://localhost:8080/api/v1/lineage/events")

        # Verify request was made
        assert mock_post.called

        # Verify JSON body structure
        call_args = mock_post.call_args
        json_data = call_args[1].get("json")

        assert isinstance(json_data, list)
        assert len(json_data) == 1

        # Verify lineage event structure in request body
        event_data = json_data[0]
        assert event_data["eventType"] == "COMPLETE"
        assert len(event_data["outputs"]) == 1
        assert event_data["outputs"][0]["namespace"] == "duckdb://jaffle_shop"
        assert event_data["outputs"][0]["name"] == "main.customers"

        # Verify outputStatistics facet was serialized
        output_facets = event_data["outputs"][0].get("outputFacets", {})
        assert "outputStatistics" in output_facets
        assert output_facets["outputStatistics"]["rowCount"] == 1000
