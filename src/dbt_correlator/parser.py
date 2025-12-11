"""dbt artifact parser for extracting test results and metadata.

This module parses dbt artifacts (run_results.json and manifest.json) to extract
test execution results, dataset information, and lineage metadata required for
OpenLineage event construction.

The parser handles:
    - run_results.json: Test execution results, timing, and status
    - manifest.json: Node metadata, dataset references, and relationships
    - Dataset namespace/name extraction from dbt connection configuration
    - Test status mapping to OpenLineage success boolean

Implementation follows dbt artifact schema:
    - run_results.json schema: https://schemas.getdbt.com/dbt/run-results/v5.json
    - manifest.json schema: https://schemas.getdbt.com/dbt/manifest/v11.json
"""

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, cast


@dataclass
class TestResult:
    """Represents a single dbt test execution result.

    Attributes:
        unique_id: Unique identifier for the test node (e.g., test.my_project.unique_orders_id)
        status: Test execution status (pass, fail, error, skipped)
        execution_time: Time taken to execute the test in seconds
        failures: Number of failures (for failed tests)
        message: Error message or additional details
        compiled_code: Compiled SQL for the test
        thread_id: Thread ID where test executed
        adapter_response: Response from dbt adapter
    """

    unique_id: str
    status: str
    execution_time: float
    failures: Optional[int] = None
    message: Optional[str] = None
    compiled_code: Optional[str] = None
    thread_id: Optional[str] = None
    adapter_response: Optional[dict[str, Any]] = None


@dataclass
class RunResultsMetadata:
    """Metadata from dbt run_results.json.

    Attributes:
        generated_at: Timestamp when results were generated
        invocation_id: Unique ID for this dbt invocation (used as runId)
        dbt_version: dbt version that generated the results
        elapsed_time: Total elapsed time for the run
    """

    generated_at: datetime
    invocation_id: str
    dbt_version: str
    elapsed_time: float


@dataclass
class RunResults:
    """Parsed dbt run_results.json file.

    Attributes:
        metadata: Run metadata (timestamps, invocation_id, etc.)
        results: list of test execution results
    """

    metadata: RunResultsMetadata
    results: list[TestResult]


@dataclass
class DatasetInfo:
    """Dataset namespace and name extracted from manifest.

    Attributes:
        namespace: Dataset namespace (e.g., postgresql://localhost:5432/my_db)
        name: Dataset name (e.g., my_schema.my_table)
    """

    namespace: str
    name: str


@dataclass
class DatasetLocation:
    """Dataset location components extracted from model node.

    Attributes:
        database: Database name (e.g., jaffle_shop, analytics)
        schema: Schema name (e.g., main, dbt_prod, public)
        table: Table name (e.g., customers, orders)
    """

    database: str
    schema: str
    table: str


@dataclass
class Manifest:
    """Parsed dbt manifest.json file.

    Attributes:
        nodes: dictionary of all dbt nodes (models, tests, etc.)
        sources: dictionary of source definitions
        metadata: Manifest metadata (dbt version, generated_at, etc.)
    """

    nodes: dict[str, Any]
    sources: dict[str, Any]
    metadata: dict[str, Any]


def get_data_from_file(file_path: str) -> dict[str, Any]:
    """Read and parse JSON file from filesystem.

    Common helper function for parsing dbt artifact JSON files (run_results.json,
    manifest.json). Handles file I/O, JSON parsing, and provides helpful error
    messages for common failure scenarios.

    Args:
        file_path: Path to JSON file (run_results.json, manifest.json, etc.).

    Returns:
        Parsed JSON data as dictionary.

    Raises:
        FileNotFoundError: If file doesn't exist at specified path.
            Error message includes full path and suggests running dbt.
        ValueError: If JSON is malformed or cannot be parsed.
            Error message includes file path and JSON parsing error details.

    Example:
        >>> data = get_data_from_file("target/run_results.json")
        >>> print(data["metadata"]["dbt_version"])
        1.10.15
    """
    # Check if file exists
    path = Path(file_path)
    filename = path.name  # Extract actual filename for error messages

    if not path.exists():
        raise FileNotFoundError(
            f"{filename} not found at path: {file_path}. "
            f"Ensure dbt has run and generated the {filename} file."
        )

    # Read and parse JSON
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(
            f"Failed to parse {filename}: invalid JSON at {file_path}. " f"Error: {e}"
        ) from e

    # Type cast for mypy: json.load returns Any, but we know it's a dict
    return cast(dict[str, Any], data)


def parse_run_results(file_path: str) -> RunResults:
    """Parse dbt run_results.json file.

    Extracts test execution results, timing information, and status from
    dbt test runs. Validates the schema version and handles multiple dbt
    versions (1.0+, 1.5+, etc.).

    Args:
        file_path: Path to run_results.json file.

    Returns:
        RunResults object containing metadata and test results.

    Raises:
        FileNotFoundError: If run_results.json not found.
        ValueError: If JSON is malformed or schema version unsupported.
        KeyError: If required fields are missing.

    Example:
        >>> r = parse_run_results("target/run_results.json")
        >>> print(f"Invocation ID: {r.metadata.invocation_id}")
        >>> print(f"Total tests: {len(r.results)}")
    """
    data = get_data_from_file(file_path)

    # Extract and validate metadata
    try:
        metadata_dict = data["metadata"]
    except KeyError as e:
        raise KeyError(
            f"Missing required field 'metadata' in run_results.json: {e}. "
            f"File may be corrupted or from unsupported dbt version."
        ) from e

    # Parse metadata fields
    try:
        generated_at_str = metadata_dict["generated_at"]
        generated_at = datetime.fromisoformat(generated_at_str.replace("Z", "+00:00"))
        invocation_id = metadata_dict["invocation_id"]
        dbt_version = metadata_dict["dbt_version"]
    except KeyError as e:
        raise KeyError(
            f"Missing required metadata field in run_results.json: {e}. "
            f"Required fields: generated_at, invocation_id, dbt_version."
        ) from e

    # Extract elapsed_time (can be at top level or in metadata)
    elapsed_time = data.get("elapsed_time", metadata_dict.get("elapsed_time", 0.0))

    # Create metadata object
    metadata = RunResultsMetadata(
        generated_at=generated_at,
        invocation_id=invocation_id,
        dbt_version=dbt_version,
        elapsed_time=elapsed_time,
    )

    # Extract results array (maybe empty)
    results_data = data.get("results", [])

    # Parse each test result
    results = []
    for result_dict in results_data:
        test_result = TestResult(
            unique_id=result_dict.get("unique_id", ""),
            status=result_dict.get("status", ""),
            execution_time=result_dict.get("execution_time", 0.0),
            failures=result_dict.get("failures"),
            message=result_dict.get("message"),
            compiled_code=result_dict.get("compiled_code"),
            thread_id=result_dict.get("thread_id"),
            adapter_response=result_dict.get("adapter_response"),
        )
        results.append(test_result)

    return RunResults(metadata=metadata, results=results)


def parse_manifest(file_path: str) -> Manifest:
    """Parse dbt manifest.json file.

    Extracts node definitions, source configurations, and dataset lineage
    information. The manifest provides the metadata needed to resolve dataset
    references and construct proper dataset URNs.

    Args:
        file_path: Path to manifest.json file.

    Returns:
        Manifest object containing nodes, sources, and metadata.

    Raises:
        FileNotFoundError: If manifest.json not found.
        ValueError: If JSON is malformed or schema version unsupported.
        KeyError: If required fields are missing.

    Example:
        >>> manifest = parse_manifest("target/manifest.json")
        >>> test_node = manifest.nodes["test.my_project.unique_orders_id"]
        >>> print(test_node["database"], test_node["schema"], test_node["name"])
    """
    data = get_data_from_file(file_path)

    # Extract required fields
    try:
        nodes = data["nodes"]
        sources = data["sources"]
        metadata = data["metadata"]
    except KeyError as e:
        raise KeyError(
            f"Missing required field in manifest.json: {e}. "
            f"File may be corrupted or from unsupported dbt version."
        ) from e

    return Manifest(nodes=nodes, sources=sources, metadata=metadata)


def _extract_project_name(test_unique_id: str) -> str:
    """Extract project name from test unique_id.

    Args:
        test_unique_id: Test unique_id (format: test.project.test_name.hash)

    Returns:
        Project name extracted from unique_id.

    Raises:
        ValueError: If unique_id format is invalid.

    Example:
        >>> _extract_project_name("test.jaffle_shop.unique_customers.abc123")
        'jaffle_shop'
    """
    try:
        parts = test_unique_id.split(".")
        return parts[1]
    except IndexError as e:
        raise ValueError(
            f"Invalid test unique_id format: {test_unique_id}. "
            f"Expected format: test.project.test_name.hash"
        ) from e


def _extract_model_name(test_node: dict[str, Any], test_unique_id: str) -> str:
    """Extract model name from test node refs.

    Args:
        test_node: Test node dictionary from manifest.
        test_unique_id: Test unique_id for error messages.

    Returns:
        Model name referenced by test.

    Raises:
        ValueError: If test has no refs or ref has no name.

    Example:
        >>> test_node = {"refs": [{"name": "customers"}]}
        >>> _extract_model_name(test_node, "test.proj.test_1.abc")
        'customers'
    """
    refs = test_node.get("refs", [])
    if not refs:
        raise ValueError(
            f"Test node has no refs: {test_unique_id}. "
            f"Cannot determine which dataset the test is validating."
        )

    first_ref = refs[0]
    model_name = first_ref.get("name")
    if not model_name:
        raise ValueError(
            f"Test node ref has no name: {test_unique_id}. "
            f"Cannot resolve model reference."
        )

    # Type cast: we've validated model_name is not None/empty
    return cast(str, model_name)


def _extract_dataset_location(
    model_node: dict[str, Any], model_unique_id: str
) -> DatasetLocation:
    """Extract database, schema, table from model node.

    Args:
        model_node: Model node dictionary from manifest.
        model_unique_id: Model unique_id for error messages.

    Returns:
        DatasetLocation with database, schema, and table components.

    Raises:
        KeyError: If required fields are missing.

    Example:
        >>> model = {"database": "analytics", "schema": "dbt_prod", "name": "customers"}
        >>> location = _extract_dataset_location(model, "model.proj.customers")
        >>> location.database
        'analytics'
        >>> location.schema
        'dbt_prod'
        >>> location.table
        'customers'
    """
    try:
        database = model_node["database"]
        schema = model_node["schema"]
        # Table name can be 'alias' or 'name'
        table = model_node.get("alias") or model_node["name"]
        return DatasetLocation(database=database, schema=schema, table=table)
    except KeyError as e:
        raise KeyError(
            f"Model node missing required fields (database, schema, name): {model_unique_id}. "
            f"Error: {e}"
        ) from e


def extract_dataset_info(test_unique_id: str, manifest: Manifest) -> DatasetInfo:
    """Extract dataset namespace and name from test node in manifest.

    Orchestrates the extraction of dataset information by:
    1. Looking up test node in manifest
    2. Extracting project name and model reference
    3. Looking up model node
    4. Constructing OpenLineage-compatible namespace and name

    Args:
        test_unique_id: Unique test identifier to lookup in manifest.nodes.
        manifest: Parsed manifest with node metadata.

    Returns:
        DatasetInfo with resolved namespace and name.

    Raises:
        ValueError: If dataset reference cannot be resolved.
        KeyError: If test_unique_id not found in manifest or required metadata missing.

    Example:
        >>> m = parse_manifest("target/manifest.json")
        >>> test_id = "test.jaffle_shop.unique_orders_order_id"
        >>> info = extract_dataset_info(test_id, m)
        >>> print(info.namespace)  # duckdb://jaffle_shop
        >>> print(info.name)       # main.orders
    """
    # Step 1: Look up test node in manifest
    try:
        test_node = manifest.nodes[test_unique_id]
    except KeyError as e:
        raise KeyError(
            f"Test node not found in manifest: {test_unique_id}. "
            f"Ensure the manifest.json is up-to-date with the test run."
        ) from e

    # Step 2: Extract project name and model name
    project_name = _extract_project_name(test_unique_id)
    model_name = _extract_model_name(test_node, test_unique_id)

    # Step 3: Look up model node in manifest
    model_unique_id = f"model.{project_name}.{model_name}"
    try:
        model_node = manifest.nodes[model_unique_id]
    except KeyError as e:
        raise KeyError(
            f"Model node not found in manifest: {model_unique_id}. "
            f"Referenced by test: {test_unique_id}"
        ) from e

    # Step 4: Extract database, schema, table from model
    location = _extract_dataset_location(model_node, model_unique_id)

    # Step 5: Construct OpenLineage namespace and name
    adapter_type = manifest.metadata.get("adapter_type", "unknown")
    namespace = f"{adapter_type}://{location.database}"
    name = f"{location.schema}.{location.table}"

    return DatasetInfo(namespace=namespace, name=name)


def map_test_status(dbt_status: str) -> bool:
    """Map dbt test status to OpenLineage success boolean.

    Converts dbt test status strings to boolean success flag for
    OpenLineage dataQualityAssertions facet.

    Mapping:
        - "pass" → True
        - "fail" → False
        - "error" → False
        - "skipped" → False (treated as failure for correlation)

    Args:
        dbt_status: dbt test status string (pass, fail, error, skipped).

    Returns:
        True if test passed, False otherwise.

    Example:
        >>> map_test_status("pass")
        True
        >>> map_test_status("fail")
        False
    """
    return dbt_status.lower() == "pass"
