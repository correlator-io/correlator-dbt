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
from openlineage.client.generated.parent_run import ParentRunFacet
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


def _build_parent_facet(
    parent_run_id: str,
    parent_job_namespace: str,
    parent_job_name: str,
    producer: str,
) -> ParentRunFacet:
    """Build ParentRunFacet for establishing parent-child job hierarchy.

    Uses OpenLineage SDK classes which provide:
    - UUID validation for runId (fails fast on invalid input)
    - Automatic _schemaURL population
    - Automatic _producer population

    Args:
        parent_run_id: UUID of the parent run (wrapping job).
        parent_job_namespace: OpenLineage namespace of parent job.
        parent_job_name: Name of the parent job (e.g., "jaffle_shop.build").
        producer: Producer URL for facet metadata.

    Returns:
        ParentRunFacet instance ready to be added to run.facets.
    """
    return ParentRunFacet(  # type: ignore[call-arg]
        run=ParentRun(runId=parent_run_id),  # type: ignore[call-arg]
        job=ParentJob(namespace=parent_job_namespace, name=parent_job_name),  # type: ignore[call-arg]
        producer=producer,
    )


def create_wrapping_event(
    event_type: str,
    run_id: str,
    job_name: str,
    job_namespace: str,
    timestamp: datetime,
) -> RunEvent:
    """Create START/COMPLETE/FAIL wrapping event.

    Wrapping events mark the beginning and end of a dbt test run, following
    the OpenLineage run cycle pattern. They have no inputs/outputs.

    Args:
        event_type: Event type ("START", "COMPLETE", or "FAIL").
        run_id: Unique run identifier (UUID).
        job_name: Job name (e.g., "dbt_test").
        job_namespace: OpenLineage job namespace (e.g., "dbt").
        timestamp: Event timestamp (UTC).

    Returns:
        OpenLineage RunEvent with wrapping structure.

    Example:
        >>> start_event = create_wrapping_event(
        ...     "START", run_id, "dbt_test", "dbt", datetime.now(timezone.utc)
        ... )
        >>> start_event.eventType
        'START'
    """
    return RunEvent(  # type: ignore[call-arg]
        eventType=getattr(RunState, event_type),
        eventTime=timestamp.isoformat(),
        run=Run(runId=run_id),  # type: ignore[call-arg]
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
    model_run_ids: Optional[dict[str, str]] = None,
    parent_run_id: Optional[str] = None,
    parent_job_namespace: Optional[str] = None,
    parent_job_name: Optional[str] = None,
) -> list[RunEvent]:
    """Construct OpenLineage RUNNING events with dataQualityAssertions facets.

    Creates one RunEvent per dataset that has tests, with all test results
    embedded in the dataQualityAssertions facet. Events use RUNNING state
    (not COMPLETE) because they are intermediate data carriers.

    Each test event has a unique job name (job_name + dataset_name) to ensure
    unique idempotency keys in Correlator. This prevents events from being
    treated as duplicates when multiple datasets have tests.

    Args:
        run_results: Parsed dbt run_results.json.
        manifest: Parsed dbt manifest.json.
        job_namespace: OpenLineage job namespace (e.g., "dbt", "production").
        job_name: Base job name for OpenLineage job (e.g., "jaffle_shop.test").
            The dataset name is appended to create unique job names per dataset.
        run_id: Fallback run identifier when model_run_ids is not provided.
            Used for `dbt test` alone (single shared runId for all test events).
        namespace_override: Optional namespace override for datasets.
            When provided, overrides the manifest-derived namespace.
        model_run_ids: Optional mapping of model_unique_id -> runId.
            For `dbt build`: test events share runId with their model.
            For `dbt test`: not provided, uses fallback run_id.
        parent_run_id: Optional UUID of parent run for job hierarchy.
        parent_job_namespace: Optional namespace of parent job.
        parent_job_name: Optional name of parent job.

    Returns:
        List of OpenLineage RunEvents (eventType=RUNNING) with
        dataQualityAssertions facets.

    Example:
        >>> events = construct_test_events(
        ...     run_results, manifest, "dbt", "jaffle_shop.test", run_id
        ... )
        >>> len(events)
        4  # One event per dataset with tests
        >>> events[0].job.name
        'jaffle_shop.test.marts.customers'

    Note:
        For `dbt test`: All events share the same run_id (fallback).
        For `dbt build`: Test events share runId with the model they test.
        Events use RUNNING state - the terminal state (COMPLETE/FAIL) is
        determined by the wrapping event based on dbt exit code.

    Parent Hierarchy:
        For all commands (dbt test, dbt build), test events' parent is the
        wrapping job. This provides a shallow (single-level) hierarchy:
        wrapping job (jaffle_shop.test)
          +-- test job (jaffle_shop.test.marts.customers)
          +-- test job (jaffle_shop.test.marts.orders)
    """
    grouped = group_tests_by_dataset(run_results, manifest, namespace_override)
    events = []

    for dataset_key, tests in grouped.items():
        # Parse dataset key: namespace|name (pipe separator avoids "://" conflicts)
        try:
            dataset_namespace, dataset_name = dataset_key.split("|", 1)
        except ValueError:
            logger.warning(f"Invalid dataset key format: {dataset_key}")
            continue

        # Determine runId for this test event
        # For dbt build: use the model's runId from model_run_ids mapping
        # For dbt test: use the fallback run_id (single shared runId)
        event_run_id = run_id  # Default fallback
        if model_run_ids and tests:
            # Get model_unique_id from first test (all tests in group are for same model)
            model_unique_id = tests[0].get("model_unique_id", "")
            if model_unique_id and model_unique_id in model_run_ids:
                event_run_id = model_run_ids[model_unique_id]

        # Build assertions from test results
        assertions = []
        for test in tests:
            # Map dbt status to OpenLineage success boolean
            success = map_test_status(test["status"])

            # Build assertion name
            assertion_name = test["test_name"]
            if test["column_name"]:
                assertion_name = f"{test['test_name']}({test['column_name']})"

            assertion = Assertion(  # type: ignore[call-arg]
                assertion=assertion_name,
                success=success,
                column=test["column_name"] if test["column_name"] else None,
            )
            assertions.append(assertion)

        # Create dataQualityAssertions facet
        dqa_facet = DataQualityAssertionsDatasetFacet(assertions=assertions)  # type: ignore[call-arg]

        # Create dataset with facet
        dataset = InputDataset(  # type: ignore[call-arg]
            namespace=dataset_namespace,
            name=dataset_name,
            inputFacets={"dataQualityAssertions": dqa_facet},
        )

        # Create unique job name per dataset to avoid idempotency collision.
        # Without this, all test events would have the same idempotency key
        # (job.namespace + job.name + run.runId + eventType + eventTime)
        # and only the first would be stored by Correlator.
        unique_job_name = f"{job_name}.{dataset_name}"

        # Build run facets for parent hierarchy
        run_facets: dict[str, ParentRunFacet] = {}
        if parent_run_id and parent_job_namespace and parent_job_name:
            run_facets["parent"] = _build_parent_facet(
                parent_run_id=parent_run_id,
                parent_job_namespace=parent_job_namespace,
                parent_job_name=parent_job_name,
                producer=PRODUCER,
            )

        # Create event
        event = RunEvent(  # type: ignore[call-arg]
            eventType=RunState.RUNNING,
            eventTime=run_results.metadata.generated_at.isoformat(),
            run=Run(runId=event_run_id, facets=run_facets if run_facets else None),  # type: ignore[call-arg]
            job=Job(namespace=job_namespace, name=unique_job_name),  # type: ignore[call-arg]
            producer=PRODUCER,
            inputs=[dataset],
            outputs=[],
        )
        events.append(event)

    return events


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
    # v2 events use attrs, so use attr.asdict() for serialization
    # Note: value_serializer is valid but not in type stubs
    event_dicts = [
        attr.asdict(event, value_serializer=_serialize_attr_value)  # type: ignore[call-arg]
        for event in events
    ]

    try:
        # Single HTTP POST with all events
        response = requests.post(
            endpoint,
            json=event_dicts,
            headers=headers,
            timeout=30,
        )

        # Handle responses - support various OpenLineage consumers
        # - 200 with body: Correlator or OL consumer with summary
        # - 200 without body: Simple acknowledgment
        # - 204 No Content: Standard OL consumer acknowledgment
        # - 207 Partial Success: Some events failed
        if response.status_code in (200, 204):
            logger.info(f"Successfully emitted {len(events)} events")
            # Log summary if available in response body (useful for debugging)
            if response.status_code == 200 and response.text:
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
        elif response.status_code == 207:
            # Partial success - some events failed
            body = response.json()
            logger.warning(
                f"Partial success: {body['summary']['successful']}/{body['summary']['received']} events succeeded. "
                f"Failed events: {body.get('failed_events', [])}"
            )
        else:
            # Error response (4xx/5xx)
            raise ValueError(
                f"OpenLineage backend returned {response.status_code}: {response.text}"
            )

    except requests.ConnectionError as e:
        raise ConnectionError(
            f"Failed to connect to OpenLineage backend at {endpoint}: {e}"
        ) from e
    except requests.Timeout as e:
        raise TimeoutError(
            f"Request to OpenLineage backend timed out after 30s: {e}"
        ) from e


def construct_lineage_event(
    model_lineage: ModelLineage,
    run_id: str,
    job_namespace: str,
    producer: str,
    event_time: str,
    execution_result: Optional[ModelExecutionResult] = None,
    parent_run_id: Optional[str] = None,
    parent_job_namespace: Optional[str] = None,
    parent_job_name: Optional[str] = None,
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
        parent_run_id: Optional UUID of parent run for job hierarchy.
        parent_job_namespace: Optional namespace of parent job.
        parent_job_name: Optional name of parent job.

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
        All three parent_* parameters must be provided together to add
        ParentRunFacet establishing parent-child relationship with wrapping job.
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
    if parent_run_id and parent_job_namespace and parent_job_name:
        run_facets["parent"] = _build_parent_facet(
            parent_run_id=parent_run_id,
            parent_job_namespace=parent_job_namespace,
            parent_job_name=parent_job_name,
            producer=producer,
        )

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
    parent_run_id: Optional[str] = None,
    parent_job_namespace: Optional[str] = None,
    parent_job_name: Optional[str] = None,
) -> tuple[list[RunEvent], dict[str, str]]:
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
        parent_run_id: Optional UUID of parent run for job hierarchy.
        parent_job_namespace: Optional namespace of parent job.
        parent_job_name: Optional name of parent job.

    Returns:
        Tuple of:
            - List of OpenLineage RunEvents (eventType=RUNNING), one per model
            - Dict mapping model unique_id to its runId (for test event correlation)

    Example:
        >>> events, model_run_ids = construct_lineage_events(
        ...     model_lineages=lineages,
        ...     job_namespace="dbt",
        ...     producer="https://github.com/correlator-io/dbt-correlator/0.1.0",
        ...     event_time="2024-01-01T12:00:00Z",
        ...     execution_results=model_results,
        ... )
        >>> len(events)
        4  # One event per model
        >>> len(model_run_ids)
        4  # Mapping of model_unique_id -> runId

    Note:
        Empty model_lineages list returns empty list and empty dict.
        Used by `run` and `build` commands for lineage emission.
        Events use RUNNING state - the terminal state (COMPLETE/FAIL) is
        determined by the wrapping event based on dbt exit code.
        All three parent_* parameters must be provided together to add
        ParentRunFacet establishing parent-child relationship with wrapping job.

    Bug Fix:
        Previously all events shared a single runId. Correlator aggregates
        by runId, creating self-referential loops when the same dataset
        appears as both input (dependency) and output (producer) across
        different models. Unique runIds per model prevent this aggregation.
    """
    events = []
    model_run_ids: dict[str, str] = {}

    for lineage in model_lineages:
        # Generate unique runId for this model (UUID7 per OpenLineage spec)
        model_run_id = str(uuid7())
        model_run_ids[lineage.unique_id] = model_run_id

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
            parent_run_id=parent_run_id,
            parent_job_namespace=parent_job_namespace,
            parent_job_name=parent_job_name,
        )
        events.append(event)

    return events, model_run_ids
