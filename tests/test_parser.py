"""Tests for dbt artifact parser module.

This module contains unit tests for parsing dbt artifacts (run_results.json
and manifest.json) and extracting dataset information for OpenLineage events.

Test Coverage:
    - parse_run_results(): Parse run_results.json file
    - parse_manifest(): Parse manifest.json file
    - extract_dataset_info(): Resolve dataset from test node
    - map_test_status(): Map dbt status to boolean

Implementation: Task 1.2 - dbt Artifact Parser
"""

import json
import os
import re
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from dbt_correlator.parser import (
    DatasetInfo,
    DatasetLocation,
    Manifest,
    RunResults,
    _extract_dataset_location,
    _extract_model_name,
    _extract_project_name,
    extract_dataset_info,
    map_test_status,
    parse_manifest,
    parse_run_results,
)

# Path to test fixtures
FIXTURES_DIR = Path(__file__).parent / "fixtures"
RUN_RESULTS_PATH = FIXTURES_DIR / "run_results.json"
MANIFEST_PATH = FIXTURES_DIR / "manifest.json"

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
        - tests/fixtures/run_results.json (jaffle shop, 30 tests, dbt 1.10.15)
    """
    # Act: Parse the run_results.json fixture
    result = parse_run_results(str(RUN_RESULTS_PATH))

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
    assert first_test.execution_time == pytest.approx(0.030359, abs=0.0001)
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
        - tests/fixtures/run_results.json (all passing tests from jaffle shop)
    """
    # Act: Parse the run_results.json fixture
    result = parse_run_results(str(RUN_RESULTS_PATH))

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
        assert test.execution_time >= 0, "Execution time must be non-negative"
        assert test.failures is not None, "Each test must have failures count"
        assert isinstance(test.failures, int), "Failures must be integer"
        assert test.failures >= 0, "Failures count must be non-negative"

        # Validate thread_id
        assert test.thread_id, "Each test must have thread_id"

    # Assert: Execution times vary across tests
    execution_times = [test.execution_time for test in result.results]
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
    result = parse_run_results(str(RUN_RESULTS_PATH))

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
def test_extract_dataset_info() -> None:
    """Test dataset namespace and name extraction from test node.

    Validates that:
        - Database connection → namespace (duckdb://database_name)
        - Schema + table → name (schema.table)
        - ref() references are resolved correctly from manifest

    Uses:
        - tests/fixtures/manifest.json (jaffle shop with DuckDB)
        - Test: unique_customers_customer_id (references customers model)
        - Expected: namespace="duckdb://jaffle_shop", name="main.customers"
    """
    # Arrange: Parse manifest fixture
    manifest = parse_manifest(str(MANIFEST_PATH))

    # Use real test unique_id from fixture that references customers model
    test_unique_id = "test.jaffle_shop.unique_customers_customer_id.c5af1ff4b1"

    # Act: Extract dataset info from test node
    dataset_info = extract_dataset_info(test_unique_id, manifest)

    # Assert: Returns DatasetInfo instance
    assert isinstance(dataset_info, DatasetInfo), "Should return DatasetInfo instance"

    # Assert: Namespace is correct (DuckDB format: duckdb://database_name)
    assert (
        dataset_info.namespace == "duckdb://jaffle_shop"
    ), f"Expected namespace 'duckdb://jaffle_shop', got '{dataset_info.namespace}'"

    # Assert: Name is correct (schema.table format)
    assert (
        dataset_info.name == "main.customers"
    ), f"Expected name 'main.customers', got '{dataset_info.name}'"


@pytest.mark.unit
def test_extract_dataset_info_missing_reference() -> None:
    """Test error handling when test node cannot be found or has invalid reference.

    Validates that:
        - KeyError or ValueError raised for invalid test_unique_id
        - Error message is helpful for debugging
        - Handles case where test node doesn't exist in manifest

    Uses:
        - tests/fixtures/manifest.json (jaffle shop)
        - Invalid test_unique_id that doesn't exist
    """
    # Arrange: Parse manifest fixture
    manifest = parse_manifest(str(MANIFEST_PATH))

    # Use invalid test unique_id that doesn't exist in manifest
    invalid_test_unique_id = "test.jaffle_shop.nonexistent_test_12345.abc123"

    # Act & Assert: Should raise KeyError or ValueError with helpful message
    with pytest.raises((KeyError, ValueError)) as exc_info:
        extract_dataset_info(invalid_test_unique_id, manifest)

    # Assert: Error message should mention the test_unique_id or be helpful
    error_message = str(exc_info.value)
    assert len(error_message) > 0, "Error message should not be empty"
    # The error message should ideally mention the test_unique_id or indicate it's not found


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
    project_name = _extract_project_name(test_unique_id)

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
        _extract_project_name(invalid_unique_id)

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
        _extract_project_name("")

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
    model_name = _extract_model_name(test_node, test_unique_id)

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
    model_name = _extract_model_name(test_node, test_unique_id)

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
        _extract_model_name(test_node, test_unique_id)

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
        _extract_model_name(test_node, test_unique_id)

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
        _extract_model_name(test_node, test_unique_id)

    # Assert: Error message should mention name issue
    error_message = str(exc_info.value)
    assert "name" in error_message.lower(), "Error should mention missing name"


@pytest.mark.unit
def test_extract_dataset_location_valid() -> None:
    """Test extracting database, schema, table from valid model node.

    Validates:
        - Correctly extracts all three fields
        - Returns DatasetLocation with proper values
    """
    # Arrange: Valid model node
    model_node = {"database": "jaffle_shop", "schema": "main", "name": "customers"}
    model_unique_id = "model.jaffle_shop.customers"

    # Act: Extract location
    location = _extract_dataset_location(model_node, model_unique_id)

    # Assert: Should return DatasetLocation instance
    assert isinstance(
        location, DatasetLocation
    ), "Should return DatasetLocation instance"

    # Assert: Should have correct values
    assert (
        location.database == "jaffle_shop"
    ), f"Expected 'jaffle_shop', got '{location.database}'"
    assert location.schema == "main", f"Expected 'main', got '{location.schema}'"
    assert (
        location.table == "customers"
    ), f"Expected 'customers', got '{location.table}'"


@pytest.mark.unit
def test_extract_dataset_location_with_alias() -> None:
    """Test that alias is preferred over name when present.

    Validates handling of dbt model alias feature.
    """
    # Arrange: Model node with alias
    model_node = {
        "database": "analytics",
        "schema": "dbt_prod",
        "name": "stg_customers",
        "alias": "customers",  # Alias overrides name
    }
    model_unique_id = "model.my_project.stg_customers"

    # Act: Extract location
    location = _extract_dataset_location(model_node, model_unique_id)

    # Assert: Should use alias instead of name
    assert (
        location.table == "customers"
    ), f"Should use alias 'customers', got '{location.table}'"


@pytest.mark.unit
def test_extract_dataset_location_missing_database() -> None:
    """Test error handling when database field is missing.

    Validates that:
        - KeyError is raised
        - Error message is helpful
    """
    # Arrange: Model node missing database
    model_node = {"schema": "main", "name": "customers"}  # Missing database
    model_unique_id = "model.jaffle_shop.customers"

    # Act & Assert: Should raise KeyError
    with pytest.raises(KeyError) as exc_info:
        _extract_dataset_location(model_node, model_unique_id)

    # Assert: Error message should mention missing fields
    error_message = str(exc_info.value)
    assert len(error_message) > 0, "Error message should not be empty"
    assert model_unique_id in error_message, "Error should include model ID"


@pytest.mark.unit
def test_extract_dataset_location_missing_schema() -> None:
    """Test error handling when schema field is missing.

    Validates proper error handling for incomplete model nodes.
    """
    # Arrange: Model node missing schema
    model_node = {"database": "jaffle_shop", "name": "customers"}  # Missing schema
    model_unique_id = "model.jaffle_shop.customers"

    # Act & Assert: Should raise KeyError
    with pytest.raises(KeyError) as exc_info:
        _extract_dataset_location(model_node, model_unique_id)

    # Assert: Error message should be helpful
    error_message = str(exc_info.value)
    assert len(error_message) > 0, "Error message should not be empty"


@pytest.mark.unit
def test_extract_dataset_location_missing_name_and_alias() -> None:
    """Test error handling when both name and alias are missing.

    Validates edge case handling.
    """
    # Arrange: Model node missing both name and alias
    model_node = {"database": "jaffle_shop", "schema": "main"}  # Missing name
    model_unique_id = "model.jaffle_shop.unknown"

    # Act & Assert: Should raise KeyError
    with pytest.raises(KeyError) as exc_info:
        _extract_dataset_location(model_node, model_unique_id)

    # Assert: Error message should be helpful
    error_message = str(exc_info.value)
    assert len(error_message) > 0, "Error message should not be empty"


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
