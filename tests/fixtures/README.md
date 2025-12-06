# Test Fixtures

This directory contains test data and sample dbt artifacts used for testing.

## Structure

```
fixtures/
├── README.md              # This file
├── run_results.json       # Sample dbt run_results.json (Task 1.2)
├── manifest.json          # Sample dbt manifest.json (Task 1.2)
└── sample_dbt_project/    # Sample dbt project for integration tests (Task 1.6)
```

## Files (To Be Added)

### `run_results.json`
Sample dbt test execution results file. Will be added in Task 1.2 (dbt Artifact Parser).

**Contains:**
- Test execution results (pass, fail, error, skipped)
- Timing information
- Invocation ID
- dbt version metadata

### `manifest.json`
Sample dbt manifest file with node definitions. Will be added in Task 1.2 (dbt Artifact Parser).

**Contains:**
- Model definitions
- Test definitions
- Source definitions
- Database connection metadata

### `sample_dbt_project/`
Complete sample dbt project for end-to-end integration testing. Will be added in Task 1.6 (Integration Testing).

**Contains:**
- dbt_project.yml
- profiles.yml
- models/ (staging + mart models)
- tests/ (schema tests + custom SQL tests)

## Usage in Tests

```python
import pytest
import json
from pathlib import Path

@pytest.fixture
def sample_run_results():
    """Load sample run_results.json fixture."""
    fixture_path = Path(__file__).parent / "fixtures" / "run_results.json"
    with open(fixture_path) as f:
        return json.load(f)

def test_with_fixture(sample_run_results):
    result = parse_run_results_dict(sample_run_results)
    assert result is not None
```

---

**Note:** Fixture files will be populated during Task 1.2 (Parser) and Task 1.6 (Integration Testing).

