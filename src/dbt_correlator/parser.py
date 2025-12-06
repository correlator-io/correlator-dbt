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

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional


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
        >>> results = parse_run_results("target/run_results.json")
        >>> print(f"Invocation ID: {results.metadata.invocation_id}")
        >>> print(f"Total tests: {len(results.results)}")

    Note:
        Implementation in Task 1.2: dbt Artifact Parser.
    """
    raise NotImplementedError(
        "parse_run_results() will be implemented in Task 1.2: dbt Artifact Parser"
    )


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

    Note:
        Implementation in Task 1.2: dbt Artifact Parser.
    """
    raise NotImplementedError(
        "parse_manifest() will be implemented in Task 1.2: dbt Artifact Parser"
    )


def extract_dataset_info(test_node: dict[str, Any], manifest: Manifest) -> DatasetInfo:
    """Extract dataset namespace and name from test node.

    Resolves dataset references from test nodes using manifest metadata.
    Handles:
        - Database connection info → namespace (e.g., postgresql://host:port/db)
        - Schema and table → name (e.g., my_schema.my_table)
        - ref() and source() references
        - Multi-database/multi-schema scenarios

    Args:
        test_node: Test node dictionary from run_results.json.
        manifest: Parsed manifest with node metadata.

    Returns:
        DatasetInfo with resolved namespace and name.

    Raises:
        ValueError: If dataset reference cannot be resolved.
        KeyError: If required metadata missing from manifest.

    Example:
        >>> manifest = parse_manifest("target/manifest.json")
        >>> test_node = {"refs": [["orders"]], "database": "analytics", "schema": "dbt_prod"}
        >>> info = extract_dataset_info(test_node, manifest)
        >>> print(info.namespace)  # postgresql://localhost:5432/analytics
        >>> print(info.name)       # dbt_prod.orders

    Note:
        Implementation in Task 1.2: dbt Artifact Parser.
    """
    raise NotImplementedError(
        "extract_dataset_info() will be implemented in Task 1.2: dbt Artifact Parser"
    )


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

    Note:
        Implementation in Task 1.2: dbt Artifact Parser.
    """
    raise NotImplementedError(
        "map_test_status() will be implemented in Task 1.2: dbt Artifact Parser"
    )
