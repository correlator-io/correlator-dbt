"""OpenLineage event emitter for dbt test results.

This module constructs OpenLineage events with embedded test results using the
dataQualityAssertions dataset facet, and emits them to Correlator backend.

The emitter handles:
    - Creating wrapping events (START/COMPLETE/FAIL)
    - Grouping test results by dataset
    - Constructing dataQualityAssertions facets
    - Building complete OpenLineage RunEvent structures
    - Batch emission of all events to Correlator

Architecture:
    Execution integration with wrapping pattern (like dbt-ol):
    START → [dbt test execution] → Test Events → COMPLETE/FAIL → Batch HTTP POST

OpenLineage Specification:
    - Core spec: https://openlineage.io/docs/spec/object-model
    - dataQualityAssertions facet: https://openlineage.io/docs/spec/facets/dataset-facets/data-quality-assertions
    - Run cycle: https://openlineage.io/docs/spec/run-cycle
"""

import logging
from datetime import datetime
from typing import Any, Optional

import attr
import requests
from openlineage.client.event_v2 import InputDataset, Job, Run, RunEvent, RunState
from openlineage.client.generated.data_quality_assertions_dataset import (
    Assertion,
    DataQualityAssertionsDatasetFacet,
)

from .parser import Manifest, RunResults, extract_dataset_info, map_test_status

logger = logging.getLogger(__name__)

# Plugin version for producer field
__version__ = "0.1.0"
PRODUCER = f"https://github.com/correlator-io/dbt-correlator/{__version__}"


def create_wrapping_event(
    event_type: str,
    run_id: str,
    job_name: str,
    namespace: str,
    timestamp: datetime,
) -> RunEvent:
    """Create START/COMPLETE/FAIL wrapping event.

    Wrapping events mark the beginning and end of a dbt test run, following
    the OpenLineage run cycle pattern. They have no inputs/outputs.

    Args:
        event_type: Event type ("START", "COMPLETE", or "FAIL").
        run_id: Unique run identifier (UUID).
        job_name: Job name (e.g., "dbt_test").
        namespace: OpenLineage namespace (e.g., "dbt").
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
        job=Job(namespace=namespace, name=job_name),  # type: ignore[call-arg]
        producer=PRODUCER,
        inputs=[],
        outputs=[],
    )


def group_tests_by_dataset(
    run_results: RunResults,
    manifest: Manifest,
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

    Returns:
        Dictionary mapping dataset URN to list of test results.
        Key: Dataset URN (namespace:name)
        Value: List of test result dictionaries with test metadata

    Example:
        >>> grouped = group_tests_by_dataset(run_results, manifest)
        >>> for dataset_urn, tests in grouped.items():
        ...     print(f"Dataset: {dataset_urn}, Tests: {len(tests)}")
        Dataset: jaffle_shop:main.customers, Tests: 3
        Dataset: jaffle_shop:main.orders, Tests: 5
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

        # Use parser's extract_dataset_info for consistent URN format
        try:
            dataset_info = extract_dataset_info(result.unique_id, manifest)
            dataset_urn = f"{dataset_info.namespace}:{dataset_info.name}"
        except (KeyError, ValueError) as e:
            logger.warning(
                f"Could not extract dataset info for test {result.unique_id}: {e}"
            )
            continue

        # Add test result to grouped dict
        if dataset_urn not in grouped:
            grouped[dataset_urn] = []

        grouped[dataset_urn].append(
            {
                "unique_id": result.unique_id,
                "status": result.status,
                "failures": result.failures,
                "message": result.message,
                "test_name": test_name,
                "column_name": column_name,
            }
        )

    return grouped


def construct_events(
    run_results: RunResults,
    manifest: Manifest,
    namespace: str,
    job_name: str,
    run_id: str,
) -> list[RunEvent]:
    """Construct OpenLineage test events with dataQualityAssertions facets.

    Creates one RunEvent per dataset that has tests, with all test results
    embedded in the dataQualityAssertions facet.

    Args:
        run_results: Parsed dbt run_results.json.
        manifest: Parsed dbt manifest.json.
        namespace: OpenLineage namespace (e.g., "dbt", "production").
        job_name: Job name for OpenLineage job (e.g., "dbt_test").
        run_id: Unique run identifier to link with wrapping events.

    Returns:
        List of OpenLineage RunEvents with dataQualityAssertions facets.

    Example:
        >>> events = construct_events(
        ...     run_results, manifest, "dbt", "dbt_test", run_id
        ... )
        >>> len(events)
        13  # One event per dataset with tests
        >>> events[0].inputs[0].facets["dataQualityAssertions"]
        DataQualityAssertionsDatasetFacet(assertions=[...])

    Note:
        All events share the same run_id for correlation in Correlator.
        Used between START and COMPLETE wrapping events in execution flow.
    """
    grouped = group_tests_by_dataset(run_results, manifest)
    events = []

    for dataset_urn, tests in grouped.items():
        # Parse dataset URN: namespace:schema.table
        try:
            namespace_part, name_part = dataset_urn.split(":", 1)
        except ValueError:
            logger.warning(f"Invalid dataset URN format: {dataset_urn}")
            continue

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
            namespace=namespace_part,
            name=name_part,
            inputFacets={"dataQualityAssertions": dqa_facet},
        )

        # Create event
        event = RunEvent(  # type: ignore[call-arg]
            eventType=RunState.COMPLETE,
            eventTime=run_results.metadata.generated_at.isoformat(),
            run=Run(runId=run_id),  # type: ignore[call-arg]
            job=Job(namespace=namespace, name=job_name),  # type: ignore[call-arg]
            producer=PRODUCER,
            inputs=[dataset],
            outputs=[],
        )
        events.append(event)

    return events


def emit_events(
    events: list[RunEvent],
    correlator_endpoint: str,
    api_key: Optional[str] = None,
) -> None:
    """Emit batch of OpenLineage events to Correlator backend.

    Sends all events in a single HTTP POST using OpenLineage batch format.
    More efficient than individual emission (50x fewer requests for 50 events).

    Args:
        events: List of OpenLineage RunEvents to emit.
        correlator_endpoint: OpenLineage API endpoint URL.
        api_key: Optional API key for authentication (X-API-Key header).

    Raises:
        ConnectionError: If unable to connect to Correlator endpoint.
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
    event_dicts = [attr.asdict(event) for event in events]

    try:
        # Single HTTP POST with all events
        response = requests.post(
            correlator_endpoint,
            json=event_dicts,
            headers=headers,
            timeout=30,
        )

        # Handle responses
        if response.status_code == 200:
            logger.info(f"Successfully emitted {len(events)} events to Correlator")
        elif response.status_code == 207:
            # Partial success - some events failed
            body = response.json()
            logger.warning(
                f"Partial success: {body['summary']['successful']}/{body['summary']['received']} events succeeded. "
                f"Failed events: {body.get('failed_events', [])}"
            )
        else:
            # Error response
            raise ValueError(
                f"Correlator returned {response.status_code}: {response.text}"
            )

    except requests.ConnectionError as e:
        raise ConnectionError(
            f"Failed to connect to Correlator at {correlator_endpoint}: {e}"
        ) from e
    except requests.Timeout as e:
        raise TimeoutError(f"Request to Correlator timed out after 30s: {e}") from e
