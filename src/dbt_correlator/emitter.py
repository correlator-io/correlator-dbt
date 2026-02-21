"""OpenLineage event emitter for dbt plugin.

This module constructs and emits OpenLineage events to Correlator backend,
supporting both test results and model lineage with runtime metrics.

Event Types:
    1. Test Events (dbt test):
       - dataQualityAssertions facet with test results per dataset
       - Input datasets only (tests validate, don't produce outputs)
       - eventType=RUNNING (intermediate data carrier)

    2. Lineage Events (dbt run/build):
       - Input/output datasets from model dependencies
       - outputStatistics facet with row counts (when available)
       - eventType=RUNNING (intermediate data carrier)

    3. Wrapping Events:
       - START: Job begins execution
       - COMPLETE/FAIL: Terminal state based on dbt exit code

The emitter handles:
    - Creating wrapping events (START/COMPLETE/FAIL)
    - Grouping test results by dataset
    - Constructing dataQualityAssertions facets for test results
    - Constructing outputStatistics facets for runtime metrics
    - Building model lineage with inputs/outputs
    - Batch emission of all events to OpenLineage consumers

Architecture:
    Execution integration with wrapping pattern (like dbt-ol):
    START → [dbt execution] → RUNNING events (data) → COMPLETE/FAIL → Batch HTTP POST

    Note: Intermediate events (test results, lineage) use RUNNING state,
    not COMPLETE. COMPLETE/FAIL are terminal states reserved for wrapping
    events that indicate the final job outcome.

OpenLineage Specification:
    - Core spec: https://openlineage.io/docs/spec/object-model
    - dataQualityAssertions facet: https://openlineage.io/docs/spec/facets/dataset-facets/data-quality-assertions
    - outputStatistics facet: https://openlineage.io/docs/spec/facets/dataset-facets/output-statistics
    - Run cycle: https://openlineage.io/docs/spec/run-cycle
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Optional

import attr
import requests
from openlineage.client.event_v2 import (
    InputDataset,
    Job,
    OutputDataset,
    Run,
    RunEvent,
    RunState,
)
from openlineage.client.generated.data_quality_assertions_dataset import (
    Assertion,
    DataQualityAssertionsDatasetFacet,
)
from openlineage.client.generated.output_statistics_output_dataset import (
    OutputStatisticsOutputDatasetFacet,
)
from openlineage.client.generated.parent_run import Job as ParentJob
from openlineage.client.generated.parent_run import (
    ParentRunFacet,
    Root,
    RootJob,
    RootRun,
)
from openlineage.client.generated.parent_run import Run as ParentRun
from uuid6 import uuid7

from . import __version__
from .parser import (
    Manifest,
    ModelExecutionResult,
    ModelLineage,
    RunResults,
    build_dataset_info,
    map_test_status,
    resolve_test_to_model_node,
)

logger = logging.getLogger(__name__)

# Plugin version for producer field
PRODUCER = f"https://github.com/correlator-io/correlator-dbt/{__version__}"


@dataclass
class ParentRunMetadata:
    """Parent run context for establishing job hierarchy.

    Used for both orchestrator context (Airflow task/DAG) and wrapping job context
    (dbt invocation as parent of model events).

    Populated from OPENLINEAGE_PARENT_ID and OPENLINEAGE_ROOT_PARENT_ID
    environment variables when orchestrated by Airflow.
    """

    run_id: str
    job_name: str
    job_namespace: str
    root_run_id: Optional[str] = None
    root_job_name: Optional[str] = None
    root_job_namespace: Optional[str] = None


def _serialize_attr_value(
    inst: Any,  # noqa: ARG001
    field: Any,  # noqa: ARG001
    value: Any,
) -> Any:
    """Serialize attrs field values, converting Enums to their string values.

    This function is used as the value_serializer for attr.asdict() to handle
    Enum types (like EventType) that are not JSON serializable by default.

    Args:
        inst: The attrs instance being serialized (unused, required by API).
        field: The attrs field being serialized (unused, required by API).
        value: The value to serialize.

    Returns:
        The serialized value (Enum.value for Enums, original value otherwise).
    """
    if isinstance(value, Enum):
        return value.value
    return value


def _serialize_event_with_extended_fields(event: RunEvent) -> dict[str, Any]:
    """Serialize RunEvent, merging extended assertion fields.

    The SDK's Assertion class doesn't support additional properties,
    so we store extended fields separately and merge them during serialization.
    This enables durationMs and message fields in dataQualityAssertions.

    Args:
        event: RunEvent to serialize.

    Returns:
        Serialized event dict with extended fields merged into assertions.
    """
    event_dict = attr.asdict(event, value_serializer=_serialize_attr_value)  # type: ignore[call-arg]

    # Merge extended fields into assertions
    for input_dataset in event_dict.get("inputs", []):
        input_facets = input_dataset.get("inputFacets", {})
        dqa_facet = input_facets.get("dataQualityAssertions")
        if dqa_facet and "assertions" in dqa_facet and event.inputs:
            # Find corresponding InputDataset object to get extended fields
            for orig_input in event.inputs:
                if (
                    orig_input.namespace == input_dataset["namespace"]
                    and orig_input.name == input_dataset["name"]
                ):
                    if orig_input.inputFacets:
                        orig_facet = orig_input.inputFacets.get("dataQualityAssertions")
                        if orig_facet and hasattr(orig_facet, "_extended_fields"):
                            extended = orig_facet._extended_fields  # type: ignore[attr-defined]
                            for i, assertion in enumerate(dqa_facet["assertions"]):
                                if i < len(extended):
                                    assertion.update(extended[i])
                    break

    return event_dict


def _build_parent_facet(
    parent: ParentRunMetadata,
    producer: str,
) -> ParentRunFacet:
    """Build ParentRunFacet for establishing parent-child job hierarchy.

    Uses OpenLineage SDK classes which provide:
    - UUID validation for runId (fails fast on invalid input)
    - Automatic _schemaURL population
    - Automatic _producer population

    Args:
        parent: Parent run context with run_id, job_name, job_namespace,
            and optional root_* fields for root tracking.
        producer: Producer URL for facet metadata.

    Returns:
        ParentRunFacet instance ready to be added to run.facets.

    Note:
        All three root_* fields on parent must be set to include root tracking.
        If any are missing, root is omitted.
    """
    root = None
    if parent.root_run_id and parent.root_job_namespace and parent.root_job_name:
        root = Root(  # type: ignore[call-arg]
            run=RootRun(runId=parent.root_run_id),  # type: ignore[call-arg]
            job=RootJob(namespace=parent.root_job_namespace, name=parent.root_job_name),  # type: ignore[call-arg]
        )

    return ParentRunFacet(  # type: ignore[call-arg]
        run=ParentRun(runId=parent.run_id),  # type: ignore[call-arg]
        job=ParentJob(namespace=parent.job_namespace, name=parent.job_name),  # type: ignore[call-arg]
        root=root,
        producer=producer,
    )


def create_wrapping_event(
    event_type: str,
    run_id: str,
    job_name: str,
    job_namespace: str,
    timestamp: datetime,
    parent: Optional[ParentRunMetadata] = None,
) -> RunEvent:
    """Create START/COMPLETE/FAIL wrapping event.

    Wrapping events mark the beginning and end of a dbt invocation, following
    the OpenLineage run cycle pattern. They have no inputs/outputs.

    When orchestrated (e.g., by Airflow), parent context links the wrapping
    event to the orchestrator task and DAG for full hierarchy tracking.

    Args:
        event_type: Event type ("START", "COMPLETE", or "FAIL").
        run_id: Unique run identifier (UUID).
        job_name: Job name (e.g., "dbt_test").
        job_namespace: OpenLineage job namespace (e.g., "dbt").
        timestamp: Event timestamp (UTC).
        parent: Optional parent run context (e.g., Airflow task/DAG).

    Returns:
        OpenLineage RunEvent with wrapping structure.

    Example:
        >>> start_event = create_wrapping_event(
        ...     "START", run_id, "dbt_test", "dbt", datetime.now(timezone.utc)
        ... )
        >>> start_event.eventType
        'START'
    """
    run_facets: dict[str, ParentRunFacet] = {}
    if parent:
        run_facets["parent"] = _build_parent_facet(parent=parent, producer=PRODUCER)

    return RunEvent(  # type: ignore[call-arg]
        eventType=getattr(RunState, event_type),
        eventTime=timestamp.isoformat(),
        run=Run(runId=run_id, facets=run_facets if run_facets else None),  # type: ignore[call-arg]
        job=Job(namespace=job_namespace, name=job_name),  # type: ignore[call-arg]
        producer=PRODUCER,
        inputs=[],
        outputs=[],
    )


def group_tests_by_dataset(
    run_results: RunResults,
    manifest: Manifest,
    namespace_override: Optional[str] = None,
) -> dict[str, list[dict[str, Any]]]:
    """Group test results by their target dataset.

    Analyzes test nodes to determine which dataset each test validates,
    then groups tests by dataset for facet construction.

    Handles:
        - Single test referencing one dataset
        - Tests with multiple refs (creates multiple dataset entries)
        - Source tests vs model tests
        - Tests without clear dataset reference (logs warning)

    Args:
        run_results: Parsed dbt run_results.json.
        manifest: Parsed dbt manifest.json.
        namespace_override: Optional namespace override for datasets.
            When provided, overrides the manifest-derived namespace.

    Returns:
        Dictionary mapping dataset key to list of test results.
        Key: Dataset key in format "namespace|name" (pipe separator to avoid
             conflicts with "://" in namespace URLs)
        Value: List of test result dictionaries with test metadata

    Example:
        >>> grouped = group_tests_by_dataset(run_results, manifest)
        >>> for dataset_key, tests in grouped.items():
        ...     print(f"Dataset: {dataset_key}, Tests: {len(tests)}")
        Dataset: duckdb://jaffle_shop|main.customers, Tests: 3
        Dataset: duckdb://jaffle_shop|main.orders, Tests: 5
    """
    grouped: dict[str, list[dict[str, Any]]] = {}

    for result in run_results.results:
        # Get test node from manifest
        test_node = manifest.nodes.get(result.unique_id)
        if not test_node:
            logger.warning(f"Test node not found in manifest: {result.unique_id}")
            continue

        # Get test metadata
        test_metadata = test_node.get("test_metadata", {})
        test_name = test_metadata.get("name", "unknown_test")
        test_kwargs = test_metadata.get("kwargs", {})
        column_name = test_kwargs.get("column_name")

        # Resolve test to model, then build dataset info
        try:
            model_node = resolve_test_to_model_node(test_node, manifest)
            model_unique_id = model_node.get("unique_id", "")
            dataset_info = build_dataset_info(model_node, manifest, namespace_override)
            # Use pipe separator to avoid conflicts with "://" in namespace URLs
            dataset_key = f"{dataset_info.namespace}|{dataset_info.name}"
        except (KeyError, ValueError) as e:
            logger.warning(
                f"Could not extract dataset info for test {result.unique_id}: {e}"
            )
            continue

        # Add test result to grouped dict
        if dataset_key not in grouped:
            grouped[dataset_key] = []

        grouped[dataset_key].append(
            {
                "unique_id": result.unique_id,
                "status": result.status,
                "failures": result.failures,
                "message": result.message,
                "execution_time_seconds": result.execution_time_seconds,
                "test_name": test_name,
                "column_name": column_name,
                "model_unique_id": model_unique_id,
            }
        )

    return grouped


def construct_test_events(
    run_results: RunResults,
    manifest: Manifest,
    job_namespace: str,
    job_name: str,
    run_id: str,
    namespace_override: Optional[str] = None,
    parent: Optional[ParentRunMetadata] = None,
) -> list[RunEvent]:
    """Construct single OpenLineage RUNNING event with all test assertions.

    Creates one RunEvent with multiple input datasets, each carrying their
    dataQualityAssertions facet. This is a dbt-correlator-specific pattern
    designed to integrate with the wrapping event lifecycle.

    Extended assertion fields (durationMs, message) are stored on the facet
    for later merge during serialization - allowed by OpenLineage schema
    and extracted by Correlator.

    Args:
        run_results: Parsed dbt run_results.json.
        manifest: Parsed dbt manifest.json.
        job_namespace: OpenLineage job namespace (e.g., "dbt", "production").
        job_name: OpenLineage job name (e.g., "jaffle_shop.test").
        run_id: Run ID (same as wrapping job).
        namespace_override: Optional dataset namespace override.
        parent: Optional parent run context (e.g., orchestrator).
            Test events share the wrapping job's identity, so parent is
            the orchestrator (not the wrapping job).

    Returns:
        List containing single RunEvent, or empty list if no tests.

    Example:
        >>> events = construct_test_events(
        ...     run_results, manifest, "dbt", "jaffle_shop.test", run_id
        ... )
        >>> len(events)
        1  # Single event with all test results
        >>> len(events[0].inputs)
        4  # Multiple datasets with tests

    Note:
        Events use RUNNING state - the terminal state (COMPLETE/FAIL) is
        determined by the wrapping event based on dbt exit code.

    Event Structure:
        - Single job name (no per-dataset suffix)
        - Single run_id (same as wrapping job)
        - ParentRunFacet set to orchestrator when orchestrated
        - Multiple inputs, each with dataQualityAssertions facet
    """
    grouped = group_tests_by_dataset(run_results, manifest, namespace_override)

    if not grouped:
        return []

    # Build all input datasets with their assertions
    inputs: list[InputDataset] = []

    for dataset_key, tests in grouped.items():
        # Parse dataset key: namespace|name (pipe separator avoids "://" conflicts)
        try:
            dataset_namespace, dataset_name = dataset_key.split("|", 1)
        except ValueError:
            logger.warning(f"Invalid dataset key format: {dataset_key}")
            continue

        # Build assertions using SDK classes
        assertions: list[Assertion] = []
        extended_fields: list[dict[str, Any]] = []  # Store extended fields separately

        for test in tests:
            # Map dbt status to OpenLineage success boolean
            success = map_test_status(test["status"])

            # Build assertion name
            assertion_name = test["test_name"]
            if test["column_name"]:
                assertion_name = f"{test['test_name']}({test['column_name']})"

            # Create SDK Assertion object (standard fields only)
            assertion = Assertion(  # type: ignore[call-arg]
                assertion=assertion_name,
                success=success,
                column=test["column_name"] if test["column_name"] else None,
            )
            assertions.append(assertion)

            # Store extended fields for post-serialization merge
            exec_time = test.get("execution_time_seconds")
            extended_fields.append(
                {
                    "durationMs": int((exec_time or 0) * 1000),
                    "message": test.get("message"),
                }
            )

        # Create facet using SDK class
        dqa_facet = DataQualityAssertionsDatasetFacet(assertions=assertions)  # type: ignore[call-arg]

        # Store extended fields for later merge during serialization
        # We attach them to the facet object for access during emit
        dqa_facet._extended_fields = extended_fields  # type: ignore[attr-defined]

        dataset = InputDataset(  # type: ignore[call-arg]
            namespace=dataset_namespace,
            name=dataset_name,
            inputFacets={"dataQualityAssertions": dqa_facet},
        )
        inputs.append(dataset)

    # Build run facets for orchestrator parent
    run_facets: dict[str, ParentRunFacet] = {}
    if parent:
        run_facets["parent"] = _build_parent_facet(parent=parent, producer=PRODUCER)

    event = RunEvent(  # type: ignore[call-arg]
        eventType=RunState.RUNNING,
        eventTime=run_results.metadata.generated_at.isoformat(),
        run=Run(runId=run_id, facets=run_facets if run_facets else None),  # type: ignore[call-arg]
        job=Job(namespace=job_namespace, name=job_name),  # type: ignore[call-arg]
        producer=PRODUCER,
        inputs=inputs,
        outputs=[],
    )

    return [event]


def _has_extended_fields(event: RunEvent) -> bool:
    """Check if event has inputs with extended dataQualityAssertions fields."""
    if not event.inputs:
        return False
    for inp in event.inputs:
        if inp.inputFacets and "dataQualityAssertions" in inp.inputFacets:
            facet = inp.inputFacets["dataQualityAssertions"]
            if hasattr(facet, "_extended_fields"):
                return True
    return False


def _serialize_event(event: RunEvent) -> dict[str, Any]:
    """Serialize a single event, handling extended fields if present."""
    if _has_extended_fields(event):
        return _serialize_event_with_extended_fields(event)
    return attr.asdict(event, value_serializer=_serialize_attr_value)  # type: ignore[call-arg]


def emit_events(
    events: list[RunEvent],
    endpoint: str,
    api_key: Optional[str] = None,
) -> None:
    """Emit batch of OpenLineage events to backend.

    Sends all events in a single HTTP POST using OpenLineage batch format.
    More efficient than individual emission (50x fewer requests for 50 events).

    Supports any OpenLineage-compatible backend.

    Args:
        events: List of OpenLineage RunEvents to emit.
        endpoint: OpenLineage API endpoint URL.
        api_key: Optional API key for authentication (X-API-Key header).

    Raises:
        ConnectionError: If unable to connect to endpoint.
        TimeoutError: If request times out.
        ValueError: If response indicates error (4xx/5xx status codes).

    Example:
        >>> events = [start_event, *test_events, complete_event]
        >>> emit_events(events, "http://localhost:8080/api/v1/lineage/events")

    Note:
        - Uses OpenLineage batch format (array of events)
        - Handles 207 partial success gracefully (logs warning)
        - No retry logic (consistent with dbt-ol pattern)
        - Fire-and-forget: lineage emission doesn't block dbt execution
    """
    if not events:
        logger.debug("No events to emit")
        return

    # Prepare headers
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["X-API-Key"] = api_key

    # Serialize events to JSON array (batch format)
    event_dicts = [_serialize_event(event) for event in events]

    try:
        # Single HTTP POST with all events
        response = requests.post(
            endpoint,
            json=event_dicts,
            headers=headers,
            timeout=30,
        )

        # Handle responses - support various OpenLineage consumers
        _handle_emit_response(response, len(events))

    except requests.ConnectionError as e:
        raise ConnectionError(
            f"Failed to connect to OpenLineage backend at {endpoint}: {e}"
        ) from e
    except requests.Timeout as e:
        raise TimeoutError(
            f"Request to OpenLineage backend timed out after 30s: {e}"
        ) from e


def _handle_emit_response(response: requests.Response, event_count: int) -> None:
    """Handle HTTP response from OpenLineage backend.

    Args:
        response: HTTP response from backend.
        event_count: Number of events that were sent.

    Raises:
        ValueError: If response indicates error (4xx/5xx status codes).
    """
    # Success responses: 200, 204
    if response.status_code in (200, 204):
        logger.info(f"Successfully emitted {event_count} events")
        _log_response_summary(response)
        return

    # Partial success: 207
    if response.status_code == 207:
        body = response.json()
        logger.warning(
            f"Partial success: {body['summary']['successful']}/{body['summary']['received']} events succeeded. "
            f"Failed events: {body.get('failed_events', [])}"
        )
        return

    # Error response (4xx/5xx)
    raise ValueError(
        f"OpenLineage backend returned {response.status_code}: {response.text}"
    )


def _log_response_summary(response: requests.Response) -> None:
    """Log summary from response body if available."""
    if response.status_code != 200 or not response.text:
        return
    try:
        body = response.json()
        if "summary" in body:
            summary = body["summary"]
            logger.info(
                f"Response: {summary.get('successful', 0)} successful, "
                f"{summary.get('failed', 0)} failed"
            )
    except (ValueError, KeyError):
        pass  # No JSON body or no summary - that's fine


def construct_lineage_event(
    model_lineage: ModelLineage,
    run_id: str,
    job_namespace: str,
    producer: str,
    event_time: str,
    execution_result: Optional[ModelExecutionResult] = None,
    parent: Optional[ParentRunMetadata] = None,
) -> RunEvent:
    """Construct OpenLineage RUNNING event for model lineage.

    Creates a single RunEvent representing a model's execution with its
    input dependencies and output dataset. Events use RUNNING state
    (not COMPLETE) because they are intermediate data carriers. Optionally
    includes runtime metrics (row count) when execution results are available.

    Args:
        model_lineage: Lineage information containing inputs and output.
        run_id: Unique run identifier to link events for correlation.
        job_namespace: OpenLineage namespace (e.g., "dbt").
        producer: Producer URL for OpenLineage event.
        event_time: ISO 8601 timestamp for the event.
        execution_result: Optional execution metrics from dbt run/build.
            When provided, adds outputStatistics facet with row count.
        parent: Optional parent run context for job hierarchy
            (e.g., wrapping job as parent of model events).

    Returns:
        OpenLineage RunEvent with RUNNING status, inputs, and output.

    Example:
        >>> event = construct_lineage_event(
        ...     model_lineage=lineage,
        ...     run_id="550e8400-e29b-41d4-a716-446655440000",
        ...     job_namespace="dbt",
        ...     producer="https://github.com/correlator-io/dbt-correlator/0.1.0",
        ...     event_time="2024-01-01T12:00:00Z",
        ...     execution_result=model_result,
        ... )
        >>> event.outputs[0].outputFacets["outputStatistics"].rowCount
        1500

    Note:
        Job name is set to model_lineage.unique_id to identify the model.
        Each model gets a unique run_id to prevent Correlator from aggregating
        events into self-referential loops.
        Events use RUNNING state - the terminal state (COMPLETE/FAIL) is
        determined by the wrapping event based on dbt exit code.
    """
    # Build input datasets from ModelLineage.inputs
    inputs = [
        InputDataset(  # type: ignore[call-arg]
            namespace=inp.namespace,
            name=inp.name,
        )
        for inp in model_lineage.inputs
    ]

    # Build output dataset with optional outputStatistics facet
    output_facets: Optional[dict[str, OutputStatisticsOutputDatasetFacet]] = None
    if execution_result and execution_result.rows_affected is not None:
        output_facets = {
            "outputStatistics": OutputStatisticsOutputDatasetFacet(  # type: ignore[call-arg]
                rowCount=execution_result.rows_affected
            )
        }

    output = OutputDataset(  # type: ignore[call-arg]
        namespace=model_lineage.output.namespace,
        name=model_lineage.output.name,
        outputFacets=output_facets,
    )

    # Build run facets for parent hierarchy
    run_facets: dict[str, ParentRunFacet] = {}
    if parent:
        run_facets["parent"] = _build_parent_facet(parent=parent, producer=producer)

    return RunEvent(  # type: ignore[call-arg]
        eventType=RunState.RUNNING,
        eventTime=event_time,
        run=Run(runId=run_id, facets=run_facets if run_facets else None),  # type: ignore[call-arg]
        job=Job(namespace=job_namespace, name=model_lineage.unique_id),  # type: ignore[call-arg]
        producer=producer,
        inputs=inputs,
        outputs=[output],
    )


def construct_lineage_events(
    model_lineages: list[ModelLineage],
    job_namespace: str,
    producer: str,
    event_time: str,
    execution_results: Optional[dict[str, ModelExecutionResult]] = None,
    parent: Optional[ParentRunMetadata] = None,
) -> list[RunEvent]:
    """Construct RUNNING lineage events for multiple models with unique runIds.

    Creates one RunEvent per model with its inputs and output. Events use
    RUNNING state (not COMPLETE) because they are intermediate data carriers.
    Each model gets a unique runId to prevent Correlator from aggregating
    them into a single job_run_id (which would create self-referential loops).

    Args:
        model_lineages: List of ModelLineage objects to construct events for.
        job_namespace: OpenLineage namespace (e.g., "dbt").
        producer: Producer URL for OpenLineage events.
        event_time: ISO 8601 timestamp for all events.
        execution_results: Optional dict mapping model unique_id to
            ModelExecutionResult. Only models with matching results
            will have outputStatistics facet populated.
        parent: Optional parent run context for job hierarchy
            (e.g., wrapping job as parent of model events).

    Returns:
        List of OpenLineage RunEvents (eventType=RUNNING), one per model.

    Example:
        >>> events = construct_lineage_events(
        ...     model_lineages=lineages,
        ...     job_namespace="dbt",
        ...     producer="https://github.com/correlator-io/dbt-correlator/0.1.0",
        ...     event_time="2024-01-01T12:00:00Z",
        ...     execution_results=model_results,
        ... )
        >>> len(events)
        4  # One event per model

    Note:
        Empty model_lineages list returns empty list.
        Used by `run` and `build` commands for lineage emission.
        Events use RUNNING state - the terminal state (COMPLETE/FAIL) is
        determined by the wrapping event based on dbt exit code.

    Bug Fix:
        Previously all events shared a single runId. Correlator aggregates
        by runId, creating self-referential loops when the same dataset
        appears as both input (dependency) and output (producer) across
        different models. Unique runIds per model prevent this aggregation.
    """
    events = []

    for lineage in model_lineages:
        # Generate unique runId for this model (UUID7 per OpenLineage spec)
        model_run_id = str(uuid7())

        # Get execution result for this model if available
        exec_result = None
        if execution_results:
            exec_result = execution_results.get(lineage.unique_id)

        event = construct_lineage_event(
            model_lineage=lineage,
            run_id=model_run_id,
            job_namespace=job_namespace,
            producer=producer,
            event_time=event_time,
            execution_result=exec_result,
            parent=parent,
        )
        events.append(event)

    return events
