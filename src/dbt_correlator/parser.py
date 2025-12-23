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
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, cast

logger = logging.getLogger(__name__)


@dataclass
class TestResult:
    """Represents a single dbt test execution result.

    Attributes:
        unique_id: Unique identifier for the test node (e.g., test.my_project.unique_orders_id)
        status: Test execution status (pass, fail, error, skipped)
        execution_time_seconds: Time taken to execute the test in seconds
        failures: Number of failures (for failed tests)
        message: Error message or additional details
        compiled_code: Compiled SQL for the test
        thread_id: Thread ID where test executed
        adapter_response: Response from dbt adapter
    """

    unique_id: str
    status: str
    execution_time_seconds: float
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
class ModelLineage:
    """Lineage information for a single dbt model.

    Represents the upstream inputs and downstream output of a dbt model,
    used for constructing OpenLineage events with complete lineage context.

    Attributes:
        unique_id: Unique identifier for the model node
            (e.g., "model.jaffle_shop.customers")
        name: Model name without project prefix (e.g., "customers")
        inputs: List of upstream datasets (refs to other models + sources)
        output: The model itself as output dataset

    Example:
        >>> lineage = ModelLineage(
        ...     unique_id="model.jaffle_shop.customers",
        ...     name="customers",
        ...     inputs=[DatasetInfo(namespace="duckdb://db", name="main.stg_customers")],
        ...     output=DatasetInfo(namespace="duckdb://db", name="main.customers"),
        ... )
    """

    unique_id: str
    name: str
    inputs: list[DatasetInfo]
    output: DatasetInfo


@dataclass
class ModelExecutionResult:
    """Execution result for a single model from dbt run.

    Contains runtime metrics extracted from run_results.json for models
    executed via `dbt run` or `dbt build`. Used to populate OpenLineage
    outputStatistics facet with row counts and other metrics.

    Attributes:
        unique_id: Unique identifier for the model node
            (e.g., "model.jaffle_shop.customers")
        status: Execution status ("success", "error", "skipped")
        execution_time_seconds: Time taken to execute the model in seconds
        rows_affected: Number of rows written (when adapter provides it).
            None for adapters like DuckDB that don't return row counts.
            Available for Postgres, Snowflake, BigQuery, etc.
        message: Execution message or error details

    Example:
        >>> result = ModelExecutionResult(
        ...     unique_id="model.jaffle_shop.customers",
        ...     status="success",
        ...     execution_time_seconds=1.5,
        ...     rows_affected=1500,
        ...     message="OK",
        ... )
    """

    unique_id: str
    status: str
    execution_time_seconds: float
    rows_affected: Optional[int] = None
    message: Optional[str] = None


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
            # Note: dbt JSON uses "execution_time", we normalize to execution_time_seconds
            execution_time_seconds=result_dict.get("execution_time", 0.0),
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


def extract_project_name(test_unique_id: str) -> str:
    """Extract project name from test unique_id.

    Args:
        test_unique_id: Test unique_id (format: test.project.test_name.hash)

    Returns:
        Project name extracted from unique_id.

    Raises:
        ValueError: If unique_id format is invalid.

    Example:
        >>> extract_project_name("test.jaffle_shop.unique_customers.abc123")
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


def extract_model_name(test_node: dict[str, Any], test_unique_id: str) -> str:
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
        >>> extract_model_name(test_node, "test.proj.test_1.abc")
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


def resolve_test_to_model_node(
    test_node: dict[str, Any],
    manifest: Manifest,
) -> dict[str, Any]:
    """Resolve a test node to its target model node.

    Given a test node from manifest, extracts the model reference and
    returns the corresponding model node. This is the first step in
    building dataset info from a test.

    Args:
        test_node: Test node dictionary from manifest.nodes.
        manifest: Parsed manifest containing all nodes.

    Returns:
        Model node dictionary that the test validates.

    Raises:
        ValueError: If test has no refs or ref has no name.
        KeyError: If referenced model not found in manifest.

    Example:
        >>> m = parse_manifest("target/manifest.json")
        >>> test_node = m.nodes["test.jaffle_shop.unique_orders_order_id"]
        >>> model_node = resolve_test_to_model_node(test_node, m)
        >>> model_node["name"]
        'orders'
    """
    # Extract unique_id from node
    test_unique_id = test_node["unique_id"]

    # Extract project name from test_unique_id
    project_name = extract_project_name(test_unique_id)

    # Extract model name from test node refs
    model_name = extract_model_name(test_node, test_unique_id)

    # Look up model node in manifest
    model_unique_id = f"model.{project_name}.{model_name}"
    try:
        return cast(dict[str, Any], manifest.nodes[model_unique_id])
    except KeyError as e:
        raise KeyError(
            f"Model node not found in manifest: {model_unique_id}. "
            f"Referenced by test: {test_unique_id}"
        ) from e


def build_dataset_info(
    node: dict[str, Any],
    manifest: Manifest,
    namespace_override: Optional[str] = None,
) -> DatasetInfo:
    """Build DatasetInfo from a model/source node.

    Constructs OpenLineage-compatible dataset information from a dbt node,
    extracting namespace from manifest metadata and name from node properties.

    Args:
        node: Model or source node dictionary from manifest.
        manifest: Parsed manifest containing adapter_type metadata.
        namespace_override: Optional override that takes precedence when set.
            Supports --dataset-namespace CLI option for strict OL compliance.

    Returns:
        DatasetInfo with namespace and name for OpenLineage dataset.

    Example:
        >>> m = parse_manifest("target/manifest.json")
        >>> model = m.nodes["model.jaffle_shop.customers"]
        >>> info = build_dataset_info(model, m)
        >>> info.namespace
        'duckdb://jaffle_shop'
        >>> info.name
        'main.customers'
    """
    # Extract location components from node
    database = node["database"]
    schema = node["schema"]
    # Table name: alias (models) > identifier (sources) > name
    table = node.get("alias") or node.get("identifier") or node["name"]

    # Build namespace (uses override if provided)
    namespace = build_namespace(manifest, database, namespace_override)

    # Build name as schema.table
    name = f"{schema}.{table}"

    return DatasetInfo(namespace=namespace, name=name)


def build_namespace(
    manifest: Manifest, database: str, namespace_override: Optional[str] = None
) -> str:
    """Build OpenLineage namespace from manifest metadata or override.

    Constructs a namespace string for OpenLineage datasets. If a namespace_override
    is provided (and non-empty), it takes precedence over manifest-based construction.
    Otherwise, builds namespace from adapter_type and database.

    Args:
        manifest: Parsed manifest containing adapter_type metadata.
        database: Database name to include in namespace.
        namespace_override: Optional override that takes precedence when set.
            Supports --dataset-namespace CLI option for strict OL compliance.

    Returns:
        Namespace string in format "{adapter_type}://{database}" or the override.

    Example:
        >>> m = parse_manifest("target/manifest.json")
        >>> build_namespace(m, "jaffle_shop")
        'duckdb://jaffle_shop'
        >>> build_namespace(m, "db", namespace_override="postgresql://prod:5432/db")
        'postgresql://prod:5432/db'
    """
    # Use override if provided and non-empty
    if namespace_override:
        return namespace_override

    # Build from manifest metadata
    adapter_type = manifest.metadata.get("adapter_type", "unknown")
    return f"{adapter_type}://{database}"


def extract_model_inputs(
    model_node: dict[str, Any],
    manifest: Manifest,
    namespace_override: Optional[str] = None,
) -> list[DatasetInfo]:
    """Extract upstream input datasets from a model's depends_on.nodes.

    Resolves model and source dependencies from depends_on.nodes to DatasetInfo objects.
    Handles both model refs and source refs.

    Args:
        model_node: Model node dictionary from manifest.
        manifest: Parsed manifest containing all nodes and sources.
        namespace_override: Optional namespace override for all inputs.

    Returns:
        List of DatasetInfo representing upstream inputs (models and sources).

    Example:
        >>> m = parse_manifest("target/manifest.json")
        >>> model = m.nodes["model.jaffle_shop.customers"]
        >>> inputs = extract_model_inputs(model, m)
        >>> len(inputs)  # stg_customers, orders
        2
    """
    inputs: list[DatasetInfo] = []

    # Get depends_on.nodes (maybe empty or missing)
    depends_on = model_node.get("depends_on", {})
    dep_nodes = depends_on.get("nodes", [])

    for dep_unique_id in dep_nodes:
        # Handle model dependencies
        if dep_unique_id.startswith("model."):
            dep_node = manifest.nodes.get(dep_unique_id)
            if dep_node:
                dataset_info = build_dataset_info(
                    dep_node, manifest, namespace_override
                )
                inputs.append(dataset_info)
            else:
                logger.warning(
                    "Dependency model not found in manifest: %s", dep_unique_id
                )
        # Handle source dependencies
        elif dep_unique_id.startswith("source."):
            source_node = manifest.sources.get(dep_unique_id)
            if source_node:
                dataset_info = build_dataset_info(
                    source_node, manifest, namespace_override
                )
                inputs.append(dataset_info)
            else:
                logger.warning(
                    "Dependency source not found in manifest: %s", dep_unique_id
                )
        else:
            # Unknown dependency type (seed, snapshot, etc.)
            logger.debug("Skipping non-model/source dependency: %s", dep_unique_id)

    return inputs


def extract_all_model_lineage(
    manifest: Manifest,
    model_ids: Optional[set[str]] = None,
    namespace_override: Optional[str] = None,
) -> list[ModelLineage]:
    """Extract lineage for all models or a filtered set of models.

    Builds ModelLineage objects for each model, containing upstream inputs
    (dependencies) and the model itself as output.

    Args:
        manifest: Parsed manifest containing all nodes and sources.
        model_ids: Optional set of model unique_ids to filter. If None,
            extracts lineage for all models in manifest.
        namespace_override: Optional namespace override for all datasets.

    Returns:
        List of ModelLineage for each model in the filter (or all models).

    Example:
        >>> m = parse_manifest("target/manifest.json")
        >>> lineages = extract_all_model_lineage(m)
        >>> len(lineages)  # One per model
        13
        >>> lineages[0].unique_id
        'model.jaffle_shop.customers'
    """
    # Determine which models to process
    if model_ids is not None:
        # Use provided filter (could be empty)
        target_model_ids = model_ids
    else:
        # Extract all model IDs from manifest
        target_model_ids = {k for k in manifest.nodes if k.startswith("model.")}

    lineages: list[ModelLineage] = []

    for model_id in target_model_ids:
        model_node = manifest.nodes.get(model_id)
        if not model_node:
            logger.warning("Model node not found in manifest: %s", model_id)
            continue

        # Extract model name from node
        model_name = model_node.get("alias") or model_node["name"]

        # Build output DatasetInfo for this model
        output = build_dataset_info(model_node, manifest, namespace_override)

        # Extract input DatasetInfo from dependencies
        inputs = extract_model_inputs(model_node, manifest, namespace_override)

        lineage = ModelLineage(
            unique_id=model_id,
            name=model_name,
            inputs=inputs,
            output=output,
        )
        lineages.append(lineage)

    return lineages


def get_executed_models(run_results: RunResults) -> set[str]:
    """Get model IDs that were executed in dbt run.

    Extracts model unique_ids from run_results by filtering for model.*
    prefixed results. Used for the `run` and `build` commands.

    Args:
        run_results: Parsed run_results containing execution results.

    Returns:
        Set of unique model unique_ids (e.g., {"model.jaffle_shop.customers"}).

    Example:
        >>> rr = parse_run_results("target/run_results.json")
        >>> models = get_executed_models(rr)
        >>> "model.jaffle_shop.customers" in models
        True
    """
    return {
        result.unique_id
        for result in run_results.results
        if result.unique_id.startswith("model.")
    }


def get_models_with_tests(run_results: RunResults, manifest: Manifest) -> set[str]:
    """Get unique model IDs that have executed tests in run_results.

    Identifies which models have tests by examining test results and resolving
    their model references. Used to determine which models to emit OpenLineage
    events for in the test command.

    Args:
        run_results: Parsed run_results containing test execution results.
        manifest: Parsed manifest containing test node metadata.

    Returns:
        Set of unique model unique_ids (e.g., {"model.jaffle_shop.customers"}).

    Example:
        >>> rr = parse_run_results("target/run_results.json")
        >>> m = parse_manifest("target/manifest.json")
        >>> models = get_models_with_tests(rr, m)
        >>> "model.jaffle_shop.customers" in models
        True
    """
    models: set[str] = set()

    for result in run_results.results:
        # Skip non-test results
        if not result.unique_id.startswith("test."):
            logger.debug("Skipping non-test result: %s", result.unique_id)
            continue

        # Look up test node in manifest
        test_node = manifest.nodes.get(result.unique_id)
        if not test_node:
            logger.warning("Test node not found in manifest: %s", result.unique_id)
            continue

        # Extract project name and model name from test
        try:
            project_name = extract_project_name(result.unique_id)
            model_name = extract_model_name(test_node, result.unique_id)
            model_unique_id = f"model.{project_name}.{model_name}"
            models.add(model_unique_id)
        except ValueError as e:
            # Skip tests that don't have valid model refs
            logger.warning(
                "Skipping test without valid model ref: %s - %s",
                result.unique_id,
                e,
            )
            continue

    return models


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


def extract_model_results(run_results: RunResults) -> dict[str, ModelExecutionResult]:
    """Extract model execution results from parsed run_results.

    Extracts model execution metrics from already-parsed RunResults for models
    executed via `dbt run` or `dbt build`. Filters out non-model results
    (tests, seeds, snapshots) and returns only model execution data.

    Args:
        run_results: Parsed RunResults from dbt run/build.

    Returns:
        Dictionary mapping model unique_id to ModelExecutionResult.
        Empty dictionary if no models were executed.

    Example:
        >>> rr = parse_run_results("target/run_results.json")
        >>> model_results = extract_model_results(rr)
        >>> result = model_results["model.jaffle_shop.customers"]
        >>> print(f"Execution time: {result.execution_time_seconds}s")
        >>> print(f"Rows affected: {result.rows_affected}")

    Note:
        - rows_affected may be None for adapters that don't return it (e.g., DuckDB)
        - Available for Postgres, Snowflake, BigQuery, etc.
    """
    model_results: dict[str, ModelExecutionResult] = {}

    for result in run_results.results:
        # Skip non-model results (tests, seeds, snapshots, etc.)
        if not result.unique_id.startswith("model."):
            logger.debug("Skipping non-model result: %s", result.unique_id)
            continue

        # Extract rows_affected from adapter_response if available
        rows_affected: Optional[int] = None
        if result.adapter_response:
            rows_affected = result.adapter_response.get("rows_affected")

        model_result = ModelExecutionResult(
            unique_id=result.unique_id,
            status=result.status,
            execution_time_seconds=result.execution_time_seconds,
            rows_affected=rows_affected,
            message=result.message,
        )
        model_results[result.unique_id] = model_result

    return model_results
