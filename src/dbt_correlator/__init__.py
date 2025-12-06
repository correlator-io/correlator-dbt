"""dbt-correlator: Emit dbt test results as OpenLineage events.

This package provides a CLI tool that parses dbt test artifacts and emits
OpenLineage events with embedded test results (dataQualityAssertions facet)
for automated incident correlation.

Key Features:
    - Parse dbt run_results.json and manifest.json
    - Construct OpenLineage events with test results
    - Emit events to Correlator or any OpenLineage-compatible backend
    - Zero-friction integration with existing dbt workflows

Usage:
    $ dbt test
    $ dbt-correlator test --openlineage-url http://localhost:8080/api/v1/lineage

Architecture:
    - parser: Parse dbt artifacts
    - emitter: Construct and emit OpenLineage events
    - config: Configuration management
    - cli: Command-line interface

For detailed documentation, see: https://github.com/correlator-io/correlator-dbt
"""

__version__ = "0.1.0"
__author__ = "Emmanuel King Kasulani"
__email__ = "kasulani@gmail.com"
__license__ = "Apache-2.0"

# Public API exports (will be implemented in later tasks)
__all__ = [
    "__version__",
    "construct_event",
    "emit_event",
    "parse_manifest",
    "parse_run_results",
]

from .emitter import construct_event, emit_event
from .parser import parse_manifest, parse_run_results
