"""Tests for dbt artifact parser module.

This module contains unit tests for parsing dbt artifacts (run_results.json
and manifest.json) and extracting dataset information for OpenLineage events.

Test Coverage:
    - parse_run_results(): Parse run_results.json file
    - parse_manifest(): Parse manifest.json file
    - resolve_test_to_model_node(): Resolve test to model node
    - build_dataset_info(): Build dataset info from model node
    - map_test_status(): Map dbt status to boolean

"""

import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from dbt_correlator.parser import (
    DatasetInfo,
    Manifest,
    ModelExecutionResult,
    ModelLineage,
    RunResults,
    RunResultsMetadata,
    TestResult,
    build_dataset_info,
    build_namespace,
    extract_all_model_lineage,
    extract_model_inputs,
    extract_model_name,
    extract_model_results,
    extract_project_name,
    get_executed_models,
    get_models_with_tests,
    map_test_status,
    parse_manifest,
    parse_run_results,
    resolve_test_to_model_node,
)

# Path to test fixtures
FIXTURES_DIR = Path(__file__).parent / "fixtures"
DBT_TEST_RESULTS_PATH = FIXTURES_DIR / "dbt_test_results.json"
MANIFEST_PATH = FIXTURES_DIR / "manifest.json"
DBT_RUN_RESULTS_PATH = FIXTURES_DIR / "dbt_run_results.json"

# Valid test status values (dbt test statuses)
VALID_TEST_STATUSES = {"pass", "fail", "error", "skipped", "warn"}

# Pattern for dbt unique_id (test.project.test_name.hash or unit_test.project.model.test_name)
# Handles both regular tests and unit tests
UNIQUE_ID_PATTERN = re.compile(r"^(test|unit_test)\.\w+\.[\w\.]+$")


@pytest.mark.unit
def test_parse_run_results() -> None:
    """Test parsing of dbt run_results.json file.

    Validates that:
        - File is read and parsed correctly
        - Metadata is extracted (invocation_id, generated_at, etc.)
        - Test results are extracted with status, timing, failures
        - Schema version compatibility is handled

    Uses:
        - tests/fixtures/dbt_test_results.json (jaffle shop, 30 tests, dbt 1.10.15)
    """
    # Act: Parse the run_results.json fixture
    result = parse_run_results(str(DBT_TEST_RESULTS_PATH))

    # Assert: Verify it returns RunResults instance
    assert isinstance(result, RunResults), "Should return RunResults instance"

    # Assert: Verify metadata is extracted correctly
    assert result.metadata.invocation_id == "1e651364-45a1-4a76-9f21-4b69fa49a65f"
    assert result.metadata.dbt_version == "1.10.15"
    assert result.metadata.generated_at.year == 2025
    assert result.metadata.generated_at.month == 12
    assert result.metadata.generated_at.day == 9
    assert result.metadata.elapsed_time == pytest.approx(0.325, abs=0.001)

    # Assert: Verify test results are extracted (30 tests in fixture)
    assert len(result.results) == 30, "Should extract all 30 test results"

    # Assert: Verify first test result structure
    first_test = result.results[0]
    assert (
        first_test.unique_id
        == "test.jaffle_shop.accepted_values_customers_customer_type__new__returning.d12f0947c8"
    )
    assert first_test.status == "pass"
    # Fixture value: 0.030359268188476562 (more precise check)
    assert first_test.execution_time_seconds == pytest.approx(0.030359, abs=0.0001)
    assert first_test.failures == 0
    assert first_test.thread_id == "Thread-1 (worker)"
    assert first_test.compiled_code is not None
    assert "with all_values as" in first_test.compiled_code
    assert first_test.adapter_response is not None
    assert first_test.adapter_response.get("_message") == "OK"


@pytest.mark.unit
def test_parse_run_results_with_multiple_tests() -> None:
    """Test parsing run_results.json with multiple test results.

    Validates handling of:
        - Multiple tests in single run (30 tests in fixture)
        - All tests have 'pass' status in this fixture
        - Varied execution times across tests
        - Different test types (unique, not_null, relationships, etc.)

    Uses:
        - tests/fixtures/dbt_test_results.json (all passing tests from jaffle shop)
    """
    # Act: Parse the run_results.json fixture
    result = parse_run_results(str(DBT_TEST_RESULTS_PATH))

    # Assert: All 30 tests extracted
    assert len(result.results) == 30, "Should extract all 30 tests"

    # Assert: All tests have 'pass' status in this fixture
    statuses = {test.status for test in result.results}
    assert statuses == {"pass"}, "All tests should have 'pass' status in fixture"

    # Assert: Each test has required fields with proper validation
    for test in result.results:
        # Validate unique_id exists and matches pattern
        assert test.unique_id, "Each test must have unique_id"
        assert UNIQUE_ID_PATTERN.match(
            test.unique_id
        ), f"unique_id must match pattern (test|unit_test).<project>.<name>.<hash>, got: {test.unique_id}"

        # Validate status is one of valid values
        assert test.status, "Each test must have status"
        assert (
            test.status in VALID_TEST_STATUSES
        ), f"Status must be one of {VALID_TEST_STATUSES}, got: {test.status}"

        # Validate numeric fields
        assert test.execution_time_seconds >= 0, "Execution time must be non-negative"
        assert test.failures is not None, "Each test must have failures count"
        assert isinstance(test.failures, int), "Failures must be integer"
        assert test.failures >= 0, "Failures count must be non-negative"

        # Validate thread_id
        assert test.thread_id, "Each test must have thread_id"

    # Assert: Execution times vary across tests
    execution_times = [test.execution_time_seconds for test in result.results]
    assert len(set(execution_times)) > 1, "Tests should have varied execution times"
    assert min(execution_times) >= 0, "Min execution time should be non-negative"
    assert max(execution_times) < 10, "Max execution time should be reasonable (<10s)"

    # Assert: Different test types present (check unique_id patterns)
    test_types = set()
    for test in result.results:
        if "accepted_values" in test.unique_id:
            test_types.add("accepted_values")
        elif "not_null" in test.unique_id:
            test_types.add("not_null")
        elif "unique" in test.unique_id:
            test_types.add("unique")
        elif "relationships" in test.unique_id:
            test_types.add("relationships")

    assert len(test_types) >= 2, f"Should have multiple test types, found: {test_types}"


@pytest.mark.unit
def test_parse_run_results_missing_file() -> None:
    """Test error handling when run_results.json is missing.

    Validates that:
        - FileNotFoundError is raised
        - Error message includes full file path
        - Error message is helpful for debugging
    """
    # Arrange: Create path to non-existent file
    non_existent_path = "/tmp/does_not_exist_12345/run_results.json"

    # Act & Assert: Should raise FileNotFoundError
    with pytest.raises(FileNotFoundError) as exc_info:
        parse_run_results(non_existent_path)

    # Assert: Error message should include full path and be helpful
    error_message = str(exc_info.value)
    assert (
        non_existent_path in error_message
    ), f"Error should include full path for debugging, got: {error_message}"
    assert (
        len(error_message) > 20
    ), f"Error message too short to be helpful: {error_message}"


@pytest.mark.unit
def test_parse_run_results_malformed_json() -> None:
    """Test error handling when run_results.json contains invalid JSON.

    Validates that:
        - JSONDecodeError or ValueError is raised
        - Error message is helpful for debugging
        - Parser doesn't crash or return partial data
    """
    # Arrange: Create temporary file with malformed JSON
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False
    ) as temp_file:
        temp_file.write('{"metadata": {"invalid json here}')
        malformed_path = temp_file.name

    try:
        # Act & Assert: Should raise JSONDecodeError or ValueError
        with pytest.raises((json.JSONDecodeError, ValueError)) as exc_info:
            parse_run_results(malformed_path)

        # Assert: Error message should mention JSON parsing issue
        error_message = str(exc_info.value)
        assert len(error_message) > 0, "Error message should not be empty"
        # Error should indicate JSON parsing problem
        assert any(
            keyword in error_message.lower()
            for keyword in ["json", "parse", "decode", "invalid"]
        ), f"Error should indicate JSON parsing issue, got: {error_message}"
    finally:
        # Cleanup: Remove temporary file
        os.unlink(malformed_path)


@pytest.mark.unit
def test_parse_run_results_missing_metadata() -> None:
    """Test error handling when required metadata fields are missing.

    Validates that:
        - KeyError or ValueError raised for missing metadata
        - Error message is helpful
        - Parser doesn't return partial/invalid data
    """
    # Arrange: Create file with missing metadata
    incomplete_data = {
        "results": [
            {
                "unique_id": "test.my_project.test_1.abc123",
                "status": "pass",
                "execution_time": 0.1,
                "failures": 0,
                "thread_id": "Thread-1",
            }
        ],
        "elapsed_time": 0.1,
        # Missing "metadata" key entirely
    }

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False
    ) as temp_file:
        json.dump(incomplete_data, temp_file)
        incomplete_path = temp_file.name

    try:
        # Act & Assert: Should raise KeyError or ValueError
        with pytest.raises((KeyError, ValueError)) as exc_info:
            parse_run_results(incomplete_path)

        # Assert: Error message should mention metadata
        error_message = str(exc_info.value).lower()
        assert (
            "metadata" in error_message
        ), f"Error should mention 'metadata', got: {exc_info.value}"
    finally:
        # Cleanup
        os.unlink(incomplete_path)


@pytest.mark.unit
def test_parse_run_results_empty_results() -> None:
    """Test parsing run_results.json with zero test results.

    Validates that:
        - Parser handles empty results array gracefully
        - Metadata still extracted correctly
        - Returns RunResults with empty results list
    """
    # Arrange: Create file with empty results array
    empty_results_data = {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/run-results/v6.json",
            "dbt_version": "1.10.15",
            "generated_at": "2025-12-09T18:34:51.064443Z",
            "invocation_id": "test-invocation-empty-12345",
        },
        "elapsed_time": 0.0,
        "results": [],  # Empty array - no tests ran
    }

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False
    ) as temp_file:
        json.dump(empty_results_data, temp_file)
        empty_path = temp_file.name

    try:
        # Act: Parse file with empty results
        result = parse_run_results(empty_path)

        # Assert: Should return valid RunResults instance
        assert isinstance(result, RunResults), "Should return RunResults instance"
        assert len(result.results) == 0, "Should handle empty results array"

        # Assert: Metadata should still be extracted correctly
        assert result.metadata.invocation_id == "test-invocation-empty-12345"
        assert result.metadata.dbt_version == "1.10.15"
        assert result.metadata.elapsed_time == 0.0
    finally:
        # Cleanup
        os.unlink(empty_path)


@pytest.mark.unit
def test_parse_run_results_openlineage_compliance() -> None:
    """Test that parsed data meets OpenLineage event requirements.

    Validates:
        - generated_at is valid datetime object (for OpenLineage eventTime)
        - invocation_id is valid UUID format (for OpenLineage runId)
        - dbt_version is present with semver format (for OpenLineage producer)
        - All fields required for OpenLineage event construction are present

    This ensures parser output is directly usable by emitter module.
    """
    # Act: Parse the run_results.json fixture
    result = parse_run_results(str(DBT_TEST_RESULTS_PATH))

    # Assert: Timestamp is valid datetime object (OpenLineage eventTime requirement)
    assert isinstance(
        result.metadata.generated_at, datetime
    ), "generated_at must be datetime object for OpenLineage eventTime"
    assert (
        result.metadata.generated_at.year > 2020
    ), "generated_at must be reasonable timestamp"

    # Assert: Invocation ID is valid UUID format (OpenLineage runId requirement)
    try:
        UUID(result.metadata.invocation_id)
    except ValueError as e:
        pytest.fail(
            f"invocation_id must be valid UUID for OpenLineage runId, "
            f"got: {result.metadata.invocation_id}, error: {e}"
        )

    # Assert: dbt_version is present and looks like semver (OpenLineage producer)
    assert result.metadata.dbt_version, "dbt_version required for OpenLineage producer"
    assert (
        result.metadata.dbt_version.count(".") >= 2
    ), f"dbt_version should be semver (x.y.z), got: {result.metadata.dbt_version}"

    # Assert: Elapsed time is present and reasonable
    assert result.metadata.elapsed_time >= 0, "elapsed_time must be non-negative"


@pytest.mark.unit
def test_parse_manifest() -> None:
    """Test parsing of dbt manifest.json file.

    Validates that:
        - File is read and parsed correctly
        - Nodes dictionary is extracted (40 nodes: 13 models + 27 tests)
        - Sources dictionary is extracted (6 sources)
        - Metadata is available

    Uses:
        - tests/fixtures/manifest.json (jaffle shop, dbt 1.10.15)
    """
    # Act: Parse the manifest.json fixture
    result = parse_manifest(str(MANIFEST_PATH))

    # Assert: Verify it returns Manifest instance
    assert isinstance(result, Manifest), "Should return Manifest instance"

    # Assert: Verify nodes dictionary is extracted (40 total nodes)
    assert isinstance(result.nodes, dict), "Nodes should be a dictionary"
    assert len(result.nodes) == 40, "Should have 40 nodes (13 models + 27 tests)"

    # Assert: Verify test nodes are present
    test_nodes = [key for key in result.nodes if key.startswith("test.")]
    assert len(test_nodes) == 27, "Should have 27 test nodes"

    # Assert: Verify model nodes are present
    model_nodes = [key for key in result.nodes if key.startswith("model.")]
    assert len(model_nodes) >= 13, "Should have at least 13 model nodes"

    # Assert: Verify a specific test node exists and has expected structure
    test_node_key = "test.jaffle_shop.unique_customers_customer_id.c5af1ff4b1"
    assert test_node_key in result.nodes, f"Test node {test_node_key} should exist"
    test_node = result.nodes[test_node_key]
    assert test_node["database"] == "jaffle_shop", "Test node should have database"
    assert test_node["schema"] == "main", "Test node should have schema"
    assert (
        test_node["name"] == "unique_customers_customer_id"
    ), "Test node should have name"
    assert "refs" in test_node, "Test node should have refs"
    assert len(test_node["refs"]) > 0, "Test node should reference at least one model"

    # Assert: Verify sources dictionary is extracted (6 sources)
    assert isinstance(result.sources, dict), "Sources should be a dictionary"
    assert len(result.sources) == 6, "Should have 6 sources"

    # Assert: Verify metadata is extracted
    assert isinstance(result.metadata, dict), "Metadata should be a dictionary"
    assert result.metadata["dbt_version"] == "1.10.15"
    assert result.metadata["project_name"] == "jaffle_shop"
    assert result.metadata["adapter_type"] == "duckdb"
    assert "invocation_id" in result.metadata
    assert "generated_at" in result.metadata


@pytest.mark.unit
def test_parse_manifest_missing_file() -> None:
    """Test error handling when manifest.json is missing.

    Validates that:
        - FileNotFoundError is raised
        - Error message includes full file path
        - Error message is helpful for debugging
    """
    # Arrange: Create path to non-existent file
    non_existent_path = "/tmp/does_not_exist_12345/manifest.json"

    # Act & Assert: Should raise FileNotFoundError
    with pytest.raises(FileNotFoundError) as exc_info:
        parse_manifest(non_existent_path)

    # Assert: Error message should include full path and be helpful
    error_message = str(exc_info.value)
    assert (
        non_existent_path in error_message
    ), f"Error should include full path for debugging, got: {error_message}"
    assert (
        len(error_message) > 20
    ), f"Error message too short to be helpful: {error_message}"


@pytest.mark.unit
def test_parse_manifest_malformed_json() -> None:
    """Test error handling when manifest.json contains invalid JSON.

    Validates that:
        - JSONDecodeError or ValueError is raised
        - Error message is helpful for debugging
        - Parser doesn't crash or return partial data
    """
    # Arrange: Create temporary file with malformed JSON
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False
    ) as temp_file:
        temp_file.write('{"nodes": {"test.project": incomplete json')
        malformed_path = temp_file.name

    try:
        # Act & Assert: Should raise JSONDecodeError or ValueError
        with pytest.raises((json.JSONDecodeError, ValueError)) as exc_info:
            parse_manifest(malformed_path)

        # Assert: Error message should mention JSON parsing issue
        error_message = str(exc_info.value)
        assert len(error_message) > 0, "Error message should not be empty"
        assert any(
            keyword in error_message.lower()
            for keyword in ["json", "parse", "decode", "invalid"]
        ), f"Error should indicate JSON parsing issue, got: {error_message}"
    finally:
        # Cleanup: Remove temporary file
        os.unlink(malformed_path)


@pytest.mark.unit
def test_resolve_test_to_model_node_and_build_dataset_info() -> None:
    """Test resolving test to model and building dataset info.

    Validates that:
        - resolve_test_to_model_node returns the correct model node
        - build_dataset_info creates correct DatasetInfo from model node
        - Database connection → namespace (duckdb://database_name)
        - Schema + table → name (schema.table)

    Uses:
        - tests/fixtures/manifest.json (jaffle shop with DuckDB)
        - Test: unique_customers_customer_id (references customers model)
        - Expected: namespace="duckdb://jaffle_shop", name="main.customers"
    """
    # Arrange: Parse manifest fixture
    manifest = parse_manifest(str(MANIFEST_PATH))

    # Use real test unique_id from fixture that references customers model
    test_unique_id = "test.jaffle_shop.unique_customers_customer_id.c5af1ff4b1"
    test_node = manifest.nodes[test_unique_id]

    # Act: Resolve test to model node, then build dataset info
    model_node = resolve_test_to_model_node(test_node, manifest)
    dataset_info = build_dataset_info(model_node, manifest)

    # Assert: Returns DatasetInfo instance
    assert isinstance(dataset_info, DatasetInfo), "Should return DatasetInfo instance"

    # Assert: Model node is correct
    assert model_node["name"] == "customers", "Should resolve to customers model"

    # Assert: Namespace is correct (DuckDB format: duckdb://database_name)
    assert (
        dataset_info.namespace == "duckdb://jaffle_shop"
    ), f"Expected namespace 'duckdb://jaffle_shop', got '{dataset_info.namespace}'"

    # Assert: Name is correct (schema.table format)
    assert (
        dataset_info.name == "main.customers"
    ), f"Expected name 'main.customers', got '{dataset_info.name}'"


@pytest.mark.unit
def test_resolve_test_to_model_node_missing_reference() -> None:
    """Test error handling when test has invalid model reference.

    Validates that:
        - KeyError or ValueError raised for test with invalid model ref
        - Error message is helpful for debugging

    Uses:
        - tests/fixtures/manifest.json (jaffle shop)
        - Test node with refs pointing to non-existent model
    """
    # Arrange: Parse manifest fixture
    manifest = parse_manifest(str(MANIFEST_PATH))

    # Create a fake test node with invalid ref (includes unique_id since function extracts it)
    fake_test_node = {
        "unique_id": "test.jaffle_shop.fake_test.abc123",
        "refs": [{"name": "nonexistent_model_12345"}],
    }

    # Act & Assert: Should raise KeyError with helpful message
    with pytest.raises(KeyError) as exc_info:
        resolve_test_to_model_node(fake_test_node, manifest)

    # Assert: Error message should mention the model not found
    error_message = str(exc_info.value)
    assert (
        "nonexistent_model_12345" in error_message
        or "not found" in error_message.lower()
    )


# Helper function tests


@pytest.mark.unit
def test_extract_project_name_valid() -> None:
    """Test extracting project name from valid test unique_id.

    Validates:
        - Correctly parses project name from second position
        - Works with standard format: test.project.test_name.hash
    """
    # Arrange: Valid test unique_id
    test_unique_id = "test.jaffle_shop.unique_customers_customer_id.c5af1ff4b1"

    # Act: Extract project name
    project_name = extract_project_name(test_unique_id)

    # Assert: Should return correct project name
    assert (
        project_name == "jaffle_shop"
    ), f"Expected 'jaffle_shop', got '{project_name}'"


@pytest.mark.unit
def test_extract_project_name_invalid_format() -> None:
    """Test error handling for invalid test unique_id format.

    Validates that:
        - ValueError is raised for malformed unique_id
        - Error message explains expected format
    """
    # Arrange: Invalid unique_id (only one part - no dot separator)
    invalid_unique_id = "test"  # Missing project, test_name, and hash

    # Act & Assert: Should raise ValueError
    with pytest.raises(ValueError) as exc_info:  # noqa: PT011
        extract_project_name(invalid_unique_id)

    # Assert: Error message should mention format
    error_message = str(exc_info.value)
    assert "format" in error_message.lower(), "Error should mention format issue"
    assert invalid_unique_id in error_message, "Error should include the invalid ID"


@pytest.mark.unit
def test_extract_project_name_empty_string() -> None:
    """Test error handling for empty unique_id string.

    Validates graceful handling of edge case.
    """
    # Act & Assert: Should raise ValueError
    with pytest.raises(ValueError) as exc_info:  # noqa: PT011
        extract_project_name("")

    # Assert: Error message should be helpful
    error_message = str(exc_info.value)
    assert len(error_message) > 0, "Error message should not be empty"


@pytest.mark.unit
def test_get_model_name_from_test_valid() -> None:
    """Test extracting model name from test node with valid refs.

    Validates:
        - Correctly extracts name from first ref
        - Handles standard dbt test node structure
    """
    # Arrange: Test node with refs
    test_node = {"refs": [{"name": "customers"}]}
    test_unique_id = "test.jaffle_shop.unique_customers_customer_id.c5af1ff4b1"

    # Act: Extract model name
    model_name = extract_model_name(test_node, test_unique_id)

    # Assert: Should return correct model name
    assert model_name == "customers", f"Expected 'customers', got '{model_name}'"


@pytest.mark.unit
def test_get_model_name_from_test_multiple_refs() -> None:
    """Test that function returns first ref when multiple refs exist.

    Documents current MVP behavior: return first ref only.
    """
    # Arrange: Test node with multiple refs (multi-table test scenario)
    test_node = {"refs": [{"name": "orders"}, {"name": "customers"}]}
    test_unique_id = "test.jaffle_shop.referential_integrity.abc123"

    # Act: Extract model name
    model_name = extract_model_name(test_node, test_unique_id)

    # Assert: Should return first ref (MVP behavior)
    assert (
        model_name == "orders"
    ), f"Should return first ref 'orders', got '{model_name}'"


@pytest.mark.unit
def test_get_model_name_from_test_no_refs() -> None:
    """Test error handling when test node has no refs array.

    Validates that:
        - ValueError is raised
        - Error message is helpful
    """
    # Arrange: Test node without refs
    test_node = {"name": "some_test"}  # Missing refs
    test_unique_id = "test.jaffle_shop.orphan_test.abc123"

    # Act & Assert: Should raise ValueError
    with pytest.raises(ValueError) as exc_info:  # noqa: PT011
        extract_model_name(test_node, test_unique_id)

    # Assert: Error message should mention refs
    error_message = str(exc_info.value)
    assert "refs" in error_message.lower(), "Error should mention missing refs"
    assert test_unique_id in error_message, "Error should include test ID"


@pytest.mark.unit
def test_get_model_name_from_test_empty_refs() -> None:
    """Test error handling when test node has empty refs array.

    Validates handling of edge case where refs exists but is empty.
    """
    # Arrange: Test node with empty refs array
    test_node: dict[str, Any] = {"refs": []}  # Empty refs
    test_unique_id = "test.jaffle_shop.test_with_no_refs.abc123"

    # Act & Assert: Should raise ValueError
    with pytest.raises(ValueError) as exc_info:  # noqa: PT011
        extract_model_name(test_node, test_unique_id)

    # Assert: Error message should be helpful
    error_message = str(exc_info.value)
    assert len(error_message) > 0, "Error message should not be empty"


@pytest.mark.unit
def test_get_model_name_from_test_ref_without_name() -> None:
    """Test error handling when ref exists but has no name field.

    Validates handling of malformed ref structure.
    """
    # Arrange: Test node with ref but no name
    test_node = {"refs": [{"package": "some_package"}]}  # Missing 'name' key
    test_unique_id = "test.jaffle_shop.malformed_ref.abc123"

    # Act & Assert: Should raise ValueError
    with pytest.raises(ValueError) as exc_info:  # noqa: PT011
        extract_model_name(test_node, test_unique_id)

    # Assert: Error message should mention name issue
    error_message = str(exc_info.value)
    assert "name" in error_message.lower(), "Error should mention missing name"


@pytest.mark.unit
def test_map_test_status_pass() -> None:
    """Test mapping 'pass' status to True.

    Validates:
        - "pass" → True (test succeeded)
    """
    # Act: Map 'pass' status
    result = map_test_status("pass")

    # Assert: Should return True
    assert result is True, "'pass' should map to True"


@pytest.mark.unit
def test_map_test_status_fail() -> None:
    """Test mapping 'fail' status to False.

    Validates:
        - "fail" → False (test failed)
    """
    # Act: Map 'fail' status
    result = map_test_status("fail")

    # Assert: Should return False
    assert result is False, "'fail' should map to False"


@pytest.mark.unit
def test_map_test_status_error() -> None:
    """Test mapping 'error' status to False.

    Validates:
        - "error" → False (test encountered error)
    """
    # Act: Map 'error' status
    result = map_test_status("error")

    # Assert: Should return False
    assert result is False, "'error' should map to False"


@pytest.mark.unit
def test_map_test_status_skipped() -> None:
    """Test mapping 'skipped' status to False.

    Validates:
        - "skipped" → False (treat as failure for correlation)
        - Skipped tests are considered failures for incident correlation purposes
    """
    # Act: Map 'skipped' status
    result = map_test_status("skipped")

    # Assert: Should return False
    assert (
        result is False
    ), "'skipped' should map to False (treated as failure for correlation)"


# =============================================================================
# Lineage Extraction Tests
# =============================================================================


@pytest.mark.unit
def test_build_namespace_from_manifest() -> None:
    """Test building namespace from manifest metadata.

    Validates:
        - Namespace built from adapter_type and database
        - Format: {adapter_type}://{database}
    """
    # Arrange: Manifest metadata with adapter_type
    manifest = parse_manifest(str(MANIFEST_PATH))

    # Act: Build namespace from manifest (duckdb, jaffle_shop database)
    namespace = build_namespace(manifest, database="jaffle_shop")

    # Assert: Correct namespace format
    assert namespace == "duckdb://jaffle_shop"


@pytest.mark.unit
def test_build_namespace_with_override() -> None:
    """Test that namespace_override takes precedence over manifest.

    Validates:
        - When namespace_override is provided, it is used directly
        - Manifest adapter_type is ignored when override is set
    """
    # Arrange: Manifest and a custom override
    manifest = parse_manifest(str(MANIFEST_PATH))
    custom_namespace = "postgresql://prod-db.example.com:5432/analytics"

    # Act: Build namespace with override
    namespace = build_namespace(
        manifest, database="jaffle_shop", namespace_override=custom_namespace
    )

    # Assert: Override takes precedence
    assert namespace == custom_namespace


@pytest.mark.unit
def test_build_namespace_unknown_adapter() -> None:
    """Test namespace building when adapter_type is missing.

    Validates:
        - Defaults to 'unknown' when adapter_type not in metadata
        - Still produces valid namespace format
    """
    # Arrange: Manifest without adapter_type
    manifest = Manifest(
        nodes={},
        sources={},
        metadata={"project_name": "test_project"},  # No adapter_type
    )

    # Act: Build namespace
    namespace = build_namespace(manifest, database="test_db")

    # Assert: Uses 'unknown' as default adapter
    assert namespace == "unknown://test_db"


@pytest.mark.unit
def test_build_namespace_empty_override_ignored() -> None:
    """Test that empty string override is ignored.

    Validates:
        - Empty string override falls back to manifest-based namespace
        - Only non-empty override values take precedence
    """
    # Arrange: Manifest with empty override
    manifest = parse_manifest(str(MANIFEST_PATH))

    # Act: Build namespace with empty override
    namespace = build_namespace(manifest, database="jaffle_shop", namespace_override="")

    # Assert: Falls back to manifest-based namespace
    assert namespace == "duckdb://jaffle_shop"


@pytest.mark.unit
def test_build_dataset_info_from_model() -> None:
    """Test building DatasetInfo from model node.

    Validates:
        - DatasetInfo built from model node with correct namespace and name
        - Uses manifest adapter_type for namespace
        - Name is schema.table format
    """
    # Arrange: Parse manifest and get a model node
    manifest = parse_manifest(str(MANIFEST_PATH))
    model_unique_id = "model.jaffle_shop.customers"
    model_node = manifest.nodes[model_unique_id]

    # Act: Build dataset info from model node
    dataset_info = build_dataset_info(model_node, manifest)

    # Assert: Correct namespace and name
    assert dataset_info.namespace == "duckdb://jaffle_shop"
    assert dataset_info.name == "main.customers"


@pytest.mark.unit
def test_build_dataset_info_with_namespace_override() -> None:
    """Test that namespace_override takes precedence in build_dataset_info.

    Validates:
        - When namespace_override is provided, it is used directly
        - Model's database is still used for the name
    """
    # Arrange: Parse manifest and get a model node
    manifest = parse_manifest(str(MANIFEST_PATH))
    model_unique_id = "model.jaffle_shop.customers"
    model_node = manifest.nodes[model_unique_id]
    custom_namespace = "postgresql://prod.example.com:5432/analytics"

    # Act: Build dataset info with override
    dataset_info = build_dataset_info(
        model_node, manifest, namespace_override=custom_namespace
    )

    # Assert: Override namespace used, name unchanged
    assert dataset_info.namespace == custom_namespace
    assert dataset_info.name == "main.customers"


@pytest.mark.unit
def test_build_dataset_info_with_alias() -> None:
    """Test build_dataset_info uses alias when present.

    Validates:
        - Model alias is used in dataset name instead of model name
    """
    # Arrange: Create model node with alias
    model_node = {
        "database": "analytics",
        "schema": "dbt_prod",
        "name": "stg_customers",
        "alias": "customers",  # Alias should be used
    }
    manifest = Manifest(
        nodes={},
        sources={},
        metadata={"adapter_type": "postgres"},
    )

    # Act: Build dataset info
    dataset_info = build_dataset_info(model_node, manifest)

    # Assert: Uses alias for table name
    assert dataset_info.namespace == "postgres://analytics"
    assert dataset_info.name == "dbt_prod.customers"  # Uses alias, not stg_customers


@pytest.mark.unit
def test_extract_model_inputs_single_model_dependency() -> None:
    """Test extracting inputs from model with single model dependency.

    Validates:
        - Model depending on another model returns correct DatasetInfo
        - Uses build_dataset_info() for model dependencies
    """
    # Arrange: Parse manifest, get model with single model dependency
    manifest = parse_manifest(str(MANIFEST_PATH))
    # model.jaffle_shop.supplies depends on model.jaffle_shop.stg_supplies
    model_node = manifest.nodes["model.jaffle_shop.supplies"]

    # Act: Extract model inputs
    inputs = extract_model_inputs(model_node, manifest)

    # Assert: Single input from stg_supplies model
    assert len(inputs) == 1
    assert inputs[0].namespace == "duckdb://jaffle_shop"
    assert inputs[0].name == "main.stg_supplies"


@pytest.mark.unit
def test_extract_model_inputs_multiple_model_dependencies() -> None:
    """Test extracting inputs from model with multiple model dependencies.

    Validates:
        - Model depending on multiple models returns all as DatasetInfo
        - Order preserved from depends_on.nodes
    """
    # Arrange: Parse manifest, get model with multiple dependencies
    manifest = parse_manifest(str(MANIFEST_PATH))
    # model.jaffle_shop.customers depends on stg_customers and orders
    model_node = manifest.nodes["model.jaffle_shop.customers"]

    # Act: Extract model inputs
    inputs = extract_model_inputs(model_node, manifest)

    # Assert: Two inputs from model dependencies
    assert len(inputs) == 2
    input_names = [inp.name for inp in inputs]
    assert "main.stg_customers" in input_names
    assert "main.orders" in input_names


@pytest.mark.unit
def test_extract_model_inputs_no_dependencies() -> None:
    """Test extracting inputs from model with no depends_on.nodes.

    Validates:
        - Model with empty depends_on returns empty list
        - Handles edge case gracefully
    """
    # Arrange: Create model node with no dependencies
    model_node = {
        "database": "jaffle_shop",
        "schema": "main",
        "name": "orphan_model",
        "depends_on": {"nodes": []},
    }
    manifest = Manifest(
        nodes={},
        sources={},
        metadata={"adapter_type": "duckdb"},
    )

    # Act: Extract model inputs
    inputs = extract_model_inputs(model_node, manifest)

    # Assert: Empty list
    assert len(inputs) == 0


@pytest.mark.unit
def test_extract_model_inputs_with_namespace_override() -> None:
    """Test that namespace_override is applied to all inputs.

    Validates:
        - All input DatasetInfo use the override namespace
    """
    # Arrange: Parse manifest, model with dependency
    manifest = parse_manifest(str(MANIFEST_PATH))
    model_node = manifest.nodes["model.jaffle_shop.supplies"]
    custom_namespace = "postgresql://prod:5432/analytics"

    # Act: Extract model inputs with override
    inputs = extract_model_inputs(
        model_node, manifest, namespace_override=custom_namespace
    )

    # Assert: Override applied
    assert len(inputs) == 1
    assert inputs[0].namespace == custom_namespace
    assert inputs[0].name == "main.stg_supplies"


@pytest.mark.unit
def test_build_dataset_info_from_source() -> None:
    """Test building DatasetInfo from source node.

    Validates:
        - Source nodes use 'identifier' field for table name
        - DatasetInfo built correctly from source node
    """
    # Arrange: Parse manifest and get a source node
    manifest = parse_manifest(str(MANIFEST_PATH))
    source_unique_id = "source.jaffle_shop.ecom.raw_customers"
    source_node = manifest.sources[source_unique_id]

    # Act: Build dataset info from source node
    dataset_info = build_dataset_info(source_node, manifest)

    # Assert: Correct namespace and name (uses identifier for table)
    assert dataset_info.namespace == "duckdb://jaffle_shop"
    assert dataset_info.name == "raw.raw_customers"


@pytest.mark.unit
def test_build_dataset_info_source_with_different_identifier() -> None:
    """Test that identifier takes precedence over name for sources.

    Validates:
        - When source has different identifier vs name, identifier is used
    """
    # Arrange: Create source node where identifier differs from name
    source_node = {
        "database": "analytics",
        "schema": "raw_data",
        "name": "customers_source",
        "identifier": "raw_customers_table",  # Different from name
    }
    manifest = Manifest(
        nodes={},
        sources={},
        metadata={"adapter_type": "postgres"},
    )

    # Act: Build dataset info
    dataset_info = build_dataset_info(source_node, manifest)

    # Assert: Uses identifier, not name
    assert dataset_info.namespace == "postgres://analytics"
    assert dataset_info.name == "raw_data.raw_customers_table"


@pytest.mark.unit
def test_extract_model_inputs_with_source_dependency() -> None:
    """Test extracting inputs from model with source dependency.

    Validates:
        - Model depending on source returns DatasetInfo for the source
        - Sources are looked up in manifest.sources
    """
    # Arrange: Parse manifest, get model that depends on source
    manifest = parse_manifest(str(MANIFEST_PATH))
    # model.jaffle_shop.stg_customers depends on source.jaffle_shop.ecom.raw_customers
    model_node = manifest.nodes["model.jaffle_shop.stg_customers"]

    # Act: Extract model inputs (includes source)
    inputs = extract_model_inputs(model_node, manifest)

    # Assert: Should have one input from source
    assert len(inputs) == 1
    assert inputs[0].namespace == "duckdb://jaffle_shop"
    assert inputs[0].name == "raw.raw_customers"


@pytest.mark.unit
def test_extract_model_inputs_mixed_model_and_source() -> None:
    """Test extracting inputs from model with both model and source dependencies.

    Validates:
        - Model with mixed dependencies returns all as DatasetInfo
        - Both models and sources are correctly resolved
    """
    # Arrange: Create a model that depends on both a model and a source
    model_node = {
        "database": "analytics",
        "schema": "marts",
        "name": "final_customers",
        "depends_on": {
            "nodes": [
                "model.project.stg_customers",
                "source.project.raw.external_data",
            ]
        },
    }

    # Create manifest with both model and source
    stg_customers_node = {
        "database": "analytics",
        "schema": "staging",
        "name": "stg_customers",
    }
    external_data_source = {
        "database": "analytics",
        "schema": "raw",
        "name": "external_data",
        "identifier": "external_data",
    }

    manifest = Manifest(
        nodes={"model.project.stg_customers": stg_customers_node},
        sources={"source.project.raw.external_data": external_data_source},
        metadata={"adapter_type": "snowflake"},
    )

    # Act: Extract model inputs
    inputs = extract_model_inputs(model_node, manifest)

    # Assert: Should have two inputs (one model, one source)
    assert len(inputs) == 2
    input_names = [inp.name for inp in inputs]
    assert "staging.stg_customers" in input_names
    assert "raw.external_data" in input_names


@pytest.mark.unit
def test_get_models_with_tests_returns_unique_models() -> None:
    """Test that get_models_with_tests returns unique model IDs from test results.

    Validates:
        - Extracts model unique_ids from test results
        - Returns unique set (no duplicates even if multiple tests per model)
    """
    # Arrange: Parse run_results and manifest from fixtures
    run_results = parse_run_results(str(DBT_TEST_RESULTS_PATH))
    manifest = parse_manifest(str(MANIFEST_PATH))

    # Act: Get models with tests
    models = get_models_with_tests(run_results, manifest)

    # Assert: Returns set of model unique_ids
    assert isinstance(models, set)
    assert len(models) > 0

    # All items should be model unique_ids
    for model_id in models:
        assert model_id.startswith("model.")
        assert model_id in manifest.nodes


@pytest.mark.unit
def test_get_models_with_tests_handles_multiple_tests_per_model() -> None:
    """Test that models with multiple tests appear only once.

    Validates:
        - Model appearing in multiple test refs is only returned once
        - Deduplication works correctly
    """
    # Arrange: Parse fixtures - customers model has multiple tests
    run_results = parse_run_results(str(DBT_TEST_RESULTS_PATH))
    manifest = parse_manifest(str(MANIFEST_PATH))

    # Act: Get models with tests
    models = get_models_with_tests(run_results, manifest)

    # Assert: model.jaffle_shop.customers should appear exactly once (not duplicated)
    # even though it has multiple tests (unique, not_null, accepted_values)
    exact_customers_count = sum(1 for m in models if m == "model.jaffle_shop.customers")
    assert exact_customers_count == 1, "customers model should appear exactly once"

    # Assert: fewer unique models than tests (30 tests across fewer models)
    assert len(models) < len(run_results.results)


@pytest.mark.unit
def test_get_models_with_tests_empty_results() -> None:
    """Test handling of empty run_results.

    Validates:
        - Empty run_results returns empty set
        - No errors on empty input
    """
    # Arrange: Create empty run_results
    empty_results = RunResults(
        metadata=RunResultsMetadata(
            generated_at=datetime.now(timezone.utc),
            invocation_id="test-id",
            dbt_version="1.10.0",
            elapsed_time=0.0,
        ),
        results=[],
    )
    manifest = parse_manifest(str(MANIFEST_PATH))

    # Act: Get models with tests
    models = get_models_with_tests(empty_results, manifest)

    # Assert: Empty set returned
    assert models == set()


@pytest.mark.unit
def test_get_executed_models_returns_model_ids() -> None:
    """Test that get_executed_models returns model IDs from run results.

    Validates:
        - Extracts model unique_ids from dbt run results
        - Only returns model.* prefixed results
    """
    # Arrange: Create run_results with model executions
    run_results = RunResults(
        metadata=RunResultsMetadata(
            generated_at=datetime.now(timezone.utc),
            invocation_id="run-id-123",
            dbt_version="1.10.0",
            elapsed_time=5.0,
        ),
        results=[
            TestResult(
                unique_id="model.jaffle_shop.customers",
                status="success",
                execution_time_seconds=1.5,
            ),
            TestResult(
                unique_id="model.jaffle_shop.orders",
                status="success",
                execution_time_seconds=2.0,
            ),
            TestResult(
                unique_id="model.jaffle_shop.stg_customers",
                status="success",
                execution_time_seconds=0.5,
            ),
        ],
    )

    # Act: Get executed models
    models = get_executed_models(run_results)

    # Assert: Returns set of model unique_ids
    assert isinstance(models, set)
    assert len(models) == 3
    assert "model.jaffle_shop.customers" in models
    assert "model.jaffle_shop.orders" in models
    assert "model.jaffle_shop.stg_customers" in models


@pytest.mark.unit
def test_get_executed_models_filters_non_models() -> None:
    """Test that get_executed_models only returns model results.

    Validates:
        - Ignores test.* results
        - Ignores seed.* results
        - Only returns model.* results
    """
    # Arrange: Create run_results with mixed resource types
    run_results = RunResults(
        metadata=RunResultsMetadata(
            generated_at=datetime.now(timezone.utc),
            invocation_id="build-id-456",
            dbt_version="1.10.0",
            elapsed_time=10.0,
        ),
        results=[
            TestResult(
                unique_id="model.jaffle_shop.customers",
                status="success",
                execution_time_seconds=1.5,
            ),
            TestResult(
                unique_id="test.jaffle_shop.unique_customers_id.abc123",
                status="pass",
                execution_time_seconds=0.1,
            ),
            TestResult(
                unique_id="seed.jaffle_shop.raw_customers",
                status="success",
                execution_time_seconds=0.3,
            ),
            TestResult(
                unique_id="model.jaffle_shop.orders",
                status="success",
                execution_time_seconds=2.0,
            ),
        ],
    )

    # Act: Get executed models
    models = get_executed_models(run_results)

    # Assert: Only model.* results returned
    assert len(models) == 2
    assert "model.jaffle_shop.customers" in models
    assert "model.jaffle_shop.orders" in models
    assert "test.jaffle_shop.unique_customers_id.abc123" not in models
    assert "seed.jaffle_shop.raw_customers" not in models


@pytest.mark.unit
def test_get_executed_models_empty_results() -> None:
    """Test handling of empty run_results.

    Validates:
        - Empty run_results returns empty set
        - No errors on empty input
    """
    # Arrange: Create empty run_results
    empty_results = RunResults(
        metadata=RunResultsMetadata(
            generated_at=datetime.now(timezone.utc),
            invocation_id="empty-run-id",
            dbt_version="1.10.0",
            elapsed_time=0.0,
        ),
        results=[],
    )

    # Act: Get executed models
    models = get_executed_models(empty_results)

    # Assert: Empty set returned
    assert models == set()


@pytest.mark.unit
def test_extract_all_model_lineage_returns_lineage_for_all_models() -> None:
    """Test extracting lineage for all models in manifest.

    Validates:
        - Returns ModelLineage for each model in manifest
        - Each lineage has correct inputs and output
    """
    # Arrange: Parse manifest
    manifest = parse_manifest(str(MANIFEST_PATH))

    # Act: Extract all model lineage (no filter)
    lineages = extract_all_model_lineage(manifest)

    # Assert: Returns list of ModelLineage
    assert isinstance(lineages, list)
    assert len(lineages) > 0
    assert all(isinstance(lineage, ModelLineage) for lineage in lineages)

    # Assert: All models in manifest have lineage
    model_ids = {lineage.unique_id for lineage in lineages}
    manifest_model_ids = {node for node in manifest.nodes if node.startswith("model.")}
    assert model_ids == manifest_model_ids


@pytest.mark.unit
def test_extract_all_model_lineage_with_filter() -> None:
    """Test extracting lineage for filtered set of models.

    Validates:
        - Only returns lineage for models in filter set
        - Ignores models not in filter
    """
    # Arrange: Parse manifest, create filter
    manifest = parse_manifest(str(MANIFEST_PATH))
    filter_models = {
        "model.jaffle_shop.customers",
        "model.jaffle_shop.orders",
    }

    # Act: Extract lineage with filter
    lineages = extract_all_model_lineage(manifest, model_ids=filter_models)

    # Assert: Only filtered models returned
    assert len(lineages) == 2
    lineage_ids = {lineage.unique_id for lineage in lineages}
    assert lineage_ids == filter_models


@pytest.mark.unit
def test_extract_all_model_lineage_has_correct_inputs_and_output() -> None:
    """Test that extracted lineage has correct inputs and output.

    Validates:
        - Lineage output matches model's dataset info
        - Lineage inputs match model's dependencies
    """
    # Arrange: Parse manifest, filter to single model
    manifest = parse_manifest(str(MANIFEST_PATH))
    # customers depends on stg_customers and orders
    filter_models = {"model.jaffle_shop.customers"}

    # Act: Extract lineage
    lineages = extract_all_model_lineage(manifest, model_ids=filter_models)

    # Assert: Single lineage with correct structure
    assert len(lineages) == 1
    lineage = lineages[0]

    # Check output
    assert lineage.unique_id == "model.jaffle_shop.customers"
    assert lineage.name == "customers"
    assert lineage.output.namespace == "duckdb://jaffle_shop"
    assert lineage.output.name == "main.customers"

    # Check inputs (stg_customers and orders)
    assert len(lineage.inputs) == 2
    input_names = {inp.name for inp in lineage.inputs}
    assert "main.stg_customers" in input_names
    assert "main.orders" in input_names


@pytest.mark.unit
def test_extract_all_model_lineage_with_namespace_override() -> None:
    """Test that namespace_override is applied to all lineage.

    Validates:
        - Override applied to output namespace
        - Override applied to all input namespaces
    """
    # Arrange: Parse manifest with custom namespace
    manifest = parse_manifest(str(MANIFEST_PATH))
    filter_models = {"model.jaffle_shop.supplies"}
    custom_namespace = "postgresql://prod:5432/analytics"

    # Act: Extract lineage with override
    lineages = extract_all_model_lineage(
        manifest, model_ids=filter_models, namespace_override=custom_namespace
    )

    # Assert: Override applied
    assert len(lineages) == 1
    lineage = lineages[0]
    assert lineage.output.namespace == custom_namespace
    for inp in lineage.inputs:
        assert inp.namespace == custom_namespace


@pytest.mark.unit
def test_extract_all_model_lineage_empty_filter() -> None:
    """Test that empty filter returns empty list.

    Validates:
        - Empty model_ids set returns empty list
        - No errors on empty filter
    """
    # Arrange: Parse manifest with empty filter
    manifest = parse_manifest(str(MANIFEST_PATH))

    # Act: Extract lineage with empty filter
    lineages = extract_all_model_lineage(manifest, model_ids=set())

    # Assert: Empty list returned
    assert lineages == []


# =============================================================================
# Runtime Metrics Extraction Tests
# =============================================================================


@pytest.mark.unit
def test_model_execution_result_dataclass_creation() -> None:
    """Test ModelExecutionResult dataclass creation and attributes.

    Validates:
        - Dataclass can be instantiated with all fields
        - All fields are accessible
        - Optional fields default to None
    """
    # Arrange & Act: Create instance with all fields
    result = ModelExecutionResult(
        unique_id="model.jaffle_shop.customers",
        status="success",
        execution_time_seconds=1.5,
        rows_affected=100,
        message="OK",
    )

    # Assert: All fields accessible
    assert result.unique_id == "model.jaffle_shop.customers"
    assert result.status == "success"
    assert result.execution_time_seconds == 1.5
    assert result.rows_affected == 100
    assert result.message == "OK"


@pytest.mark.unit
def test_model_execution_result_optional_fields() -> None:
    """Test ModelExecutionResult with optional fields as None.

    Validates:
        - rows_affected can be None (some adapters don't provide it)
        - message can be None
    """
    # Arrange & Act: Create instance with optional fields as None
    result = ModelExecutionResult(
        unique_id="model.jaffle_shop.orders",
        status="success",
        execution_time_seconds=0.5,
        rows_affected=None,
        message=None,
    )

    # Assert: Optional fields are None
    assert result.rows_affected is None
    assert result.message is None


@pytest.mark.unit
def test_extract_model_results_from_fixture() -> None:
    """Test parsing model execution results from real dbt run output.

    Validates:
        - Parses dbt_run_results.json correctly
        - Returns dictionary keyed by unique_id
        - Extracts execution_time correctly
    """
    # Arrange: Parse run_results from dbt run
    run_results = parse_run_results(str(DBT_RUN_RESULTS_PATH))

    # Act: Parse model results
    model_results = extract_model_results(run_results)

    # Assert: Returns dictionary
    assert isinstance(model_results, dict)
    assert len(model_results) > 0

    # Assert: All keys are model unique_ids
    for unique_id in model_results:
        assert unique_id.startswith("model.")

    # Assert: Contains expected models from jaffle_shop
    assert "model.jaffle_shop.customers" in model_results
    assert "model.jaffle_shop.orders" in model_results
    assert "model.jaffle_shop.stg_customers" in model_results


@pytest.mark.unit
def test_extract_model_results_extracts_execution_time() -> None:
    """Test that extract_model_results extracts execution_time correctly.

    Validates:
        - execution_time_seconds is extracted from run_results
        - Value is a positive float
    """
    # Arrange: Parse run_results
    run_results = parse_run_results(str(DBT_RUN_RESULTS_PATH))

    # Act: Parse model results
    model_results = extract_model_results(run_results)

    # Assert: Execution time is extracted as positive float
    customers_result = model_results["model.jaffle_shop.customers"]
    assert customers_result.execution_time_seconds > 0
    assert isinstance(customers_result.execution_time_seconds, float)


@pytest.mark.unit
def test_extract_model_results_extracts_status() -> None:
    """Test that extract_model_results extracts status correctly.

    Validates:
        - status field is extracted (success, error, skipped)
    """
    # Arrange: Parse run_results
    run_results = parse_run_results(str(DBT_RUN_RESULTS_PATH))

    # Act: Parse model results
    model_results = extract_model_results(run_results)

    # Assert: Status is extracted
    customers_result = model_results["model.jaffle_shop.customers"]
    assert customers_result.status == "success"


@pytest.mark.unit
def test_extract_model_results_handles_missing_rows_affected() -> None:
    """Test that extract_model_results handles missing rows_affected.

    Validates:
        - rows_affected is None when adapter doesn't provide it
        - DuckDB adapter doesn't return row counts
    """
    # Arrange: Parse run_results (DuckDB doesn't return rows_affected)
    run_results = parse_run_results(str(DBT_RUN_RESULTS_PATH))

    # Act: Parse model results
    model_results = extract_model_results(run_results)

    # Assert: rows_affected is None for DuckDB
    customers_result = model_results["model.jaffle_shop.customers"]
    assert customers_result.rows_affected is None


@pytest.mark.unit
def test_extract_model_results_extracts_rows_affected_when_present() -> None:
    """Test that extract_model_results extracts rows_affected when adapter provides it.

    Validates:
        - rows_affected is extracted from adapter_response when present
        - Some adapters (Postgres, Snowflake) return this value
    """
    # Arrange: Create run_results with rows_affected in adapter_response
    run_results = RunResults(
        metadata=RunResultsMetadata(
            generated_at=datetime.now(timezone.utc),
            invocation_id="test-run-id",
            dbt_version="1.10.0",
            elapsed_time=5.0,
        ),
        results=[
            TestResult(
                unique_id="model.project.customers",
                status="success",
                execution_time_seconds=1.5,
                message="CREATE TABLE",
                adapter_response={
                    "_message": "CREATE TABLE",
                    "rows_affected": 1500,  # Postgres-style response
                },
            ),
        ],
    )

    # Act: Parse model results
    model_results = extract_model_results(run_results)

    # Assert: rows_affected is extracted
    assert "model.project.customers" in model_results
    assert model_results["model.project.customers"].rows_affected == 1500


@pytest.mark.unit
def test_extract_model_results_filters_non_model_results() -> None:
    """Test that extract_model_results ignores non-model results.

    Validates:
        - test.* results are ignored
        - seed.* results are ignored
        - Only model.* results are included
    """
    # Arrange: Create run_results with mixed types (like dbt build output)
    run_results = RunResults(
        metadata=RunResultsMetadata(
            generated_at=datetime.now(timezone.utc),
            invocation_id="build-run-id",
            dbt_version="1.10.0",
            elapsed_time=10.0,
        ),
        results=[
            TestResult(
                unique_id="model.project.customers",
                status="success",
                execution_time_seconds=1.5,
            ),
            TestResult(
                unique_id="test.project.unique_customers_id.abc123",
                status="pass",
                execution_time_seconds=0.05,
            ),
            TestResult(
                unique_id="seed.project.raw_data",
                status="success",
                execution_time_seconds=0.2,
            ),
            TestResult(
                unique_id="model.project.orders",
                status="success",
                execution_time_seconds=2.0,
            ),
        ],
    )

    # Act: Parse model results
    model_results = extract_model_results(run_results)

    # Assert: Only model results included
    assert len(model_results) == 2
    assert "model.project.customers" in model_results
    assert "model.project.orders" in model_results
    assert "test.project.unique_customers_id.abc123" not in model_results
    assert "seed.project.raw_data" not in model_results


@pytest.mark.unit
def test_extract_model_results_empty_run_results() -> None:
    """Test that extract_model_results handles empty run_results.

    Validates:
        - Empty run_results returns empty dictionary
        - No errors on empty input
    """
    # Arrange: Create empty run_results
    run_results = RunResults(
        metadata=RunResultsMetadata(
            generated_at=datetime.now(timezone.utc),
            invocation_id="empty-run-id",
            dbt_version="1.10.0",
            elapsed_time=0.0,
        ),
        results=[],
    )

    # Act: Parse model results
    model_results = extract_model_results(run_results)

    # Assert: Empty dictionary returned
    assert model_results == {}


@pytest.mark.unit
def test_extract_model_results_handles_error_status() -> None:
    """Test that extract_model_results handles error status models.

    Validates:
        - Error status models are included
        - Message contains error information
    """
    # Arrange: Create run_results with error model
    run_results = RunResults(
        metadata=RunResultsMetadata(
            generated_at=datetime.now(timezone.utc),
            invocation_id="error-run-id",
            dbt_version="1.10.0",
            elapsed_time=2.0,
        ),
        results=[
            TestResult(
                unique_id="model.project.broken_model",
                status="error",
                execution_time_seconds=0.5,
                message="Compilation Error: column 'missing_col' not found",
            ),
        ],
    )

    # Act: Parse model results
    model_results = extract_model_results(run_results)

    # Assert: Error model is included with error status
    assert "model.project.broken_model" in model_results
    broken_result = model_results["model.project.broken_model"]
    assert broken_result.status == "error"
    assert "missing_col" in (broken_result.message or "")


@pytest.mark.unit
def test_extract_model_results_handles_skipped_status() -> None:
    """Test that extract_model_results handles skipped status models.

    Validates:
        - Skipped models are included in results
        - status field is correctly set to "skipped"
        - Skipped models have execution_time but usually no message
    """
    # Arrange: Create run_results with skipped model
    run_results = RunResults(
        metadata=RunResultsMetadata(
            generated_at=datetime.now(timezone.utc),
            invocation_id="skipped-run-id",
            dbt_version="1.10.0",
            elapsed_time=1.0,
        ),
        results=[
            TestResult(
                unique_id="model.project.upstream_model",
                status="success",
                execution_time_seconds=1.0,
            ),
            TestResult(
                unique_id="model.project.dependent_model",
                status="skipped",
                execution_time_seconds=0.0,
                message="SKIP relation project.upstream_model does not exist",
            ),
        ],
    )

    # Act: Parse model results
    model_results = extract_model_results(run_results)

    # Assert: Both models are included
    assert len(model_results) == 2
    assert "model.project.upstream_model" in model_results
    assert "model.project.dependent_model" in model_results

    # Assert: Skipped model has correct status
    skipped_result = model_results["model.project.dependent_model"]
    assert skipped_result.status == "skipped"
    assert skipped_result.execution_time_seconds == 0.0
    assert skipped_result.message is not None
    assert "SKIP" in skipped_result.message
