# dbt Jaffle Shop Setup Guide

**Purpose:** Set up the dbt jaffle shop example project to generate sample artifacts for parser testing.

**Last Updated:** December 9, 2025
**Database Adapter:** DuckDB (lightweight, no server required)

---

## Prerequisites

- Python 3.9+ installed
- uv package manager
- Git installed

---

## Step 1: Install dbt-core and dbt-duckdb

### Using uv (Recommended - Project Isolated)

Install dbt as dev dependencies in the project's virtual environment:

```bash
# Navigate to project root
cd /Users/eka/Desktop/personal/correlator-io/correlator-dbt

# Ensure virtual environment exists - use `make start` if needed
source .venv/bin/activate

# Add dbt as dev dependencies using uv
uv add --dev dbt-core dbt-duckdb

# Verify installation
dbt --version
```

**What this does:**
- Installs dbt-core and dbt-duckdb
- Automatically adds them to `[project.optional-dependencies].dev` in `pyproject.toml`
- Updates `uv.lock` file
- Keeps dbt isolated to development environment (not required for production users)

**Expected output:**
```
Core:
  - installed: 1.7.x or 1.8.x
  - latest:    1.7.x or 1.8.x - Up to date!

Plugins:
  - duckdb: 1.7.x or 1.8.x - Up to date!
```

---

## Step 2: Clone Jaffle Shop Repository

Clone the official jaffle shop example into the test fixtures directory:

```bash
# Navigate to test fixtures directory
cd tests/fixtures/sample_dbt_project

# Clone jaffle shop repository
git clone https://github.com/dbt-labs/jaffle-shop.git jaffle_shop

# Navigate into jaffle shop
cd jaffle_shop
```

**Directory structure after cloning:**
```
tests/fixtures/sample_dbt_project/
└── jaffle_shop/
    ├── dbt_project.yml
    ├── models/
    ├── seeds/
    └── README.md
```

**Add jaffle_shop to .gitignore:**

```bash
# Navigate to project root
cd /Users/eka/Desktop/personal/correlator-io/correlator-dbt

# Add jaffle_shop to .gitignore
echo "tests/fixtures/sample_dbt_project/jaffle_shop/" >> .gitignore
```

This ensures the cloned jaffle shop project is not committed to the repository.

---

## Step 3: Configure DuckDB Profile

Create a local dbt profile for the jaffle shop project using DuckDB (project-specific configuration):

```bash
# Create profiles.yml in jaffle_shop directory
cd tests/fixtures/sample_dbt_project/jaffle_shop

cat > profiles.yml << 'EOF'
jaffle_shop:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: 'jaffle_shop.duckdb'
      threads: 4
EOF
```

This keeps the profile local to the project and isolated from global dbt configurations.
---

## Step 4: Run dbt Commands

Execute the complete dbt workflow to generate artifacts:

```bash
# Ensure you're in the jaffle_shop directory
cd tests/fixtures/sample_dbt_project/jaffle_shop

# Install dbt dependencies (if any)
dbt deps

# Load seed data (sample CSV files)
dbt seed --full-refresh --vars '{"load_source_data": true}'

# Build all models
dbt run

# Run all tests to generate test results
dbt test

# Verify artifacts were created
ls -la target/
```

**Expected artifacts in `target/` directory:**
- `run_results.json` - Test execution results
- `manifest.json` - Complete project manifest with all nodes
- `compiled/` - Compiled SQL
- `run/` - Executed SQL

---

## Step 5: Copy Artifacts to Test Fixtures

Copy the generated artifacts to the test fixtures directory for parser testing:

```bash
# From the jaffle_shop directory
cd tests/fixtures/sample_dbt_project/jaffle_shop

# Copy artifacts to parent fixtures directory
cp target/run_results.json ../../run_results.json
cp target/manifest.json ../../manifest.json

# Verify files were copied
ls -la ../../*.json
```

**Artifacts location after copying:**
```
tests/fixtures/
├── run_results.json    # Sample test results (COMMIT THIS)
├── manifest.json       # Sample manifest (COMMIT THIS)
└── sample_dbt_project/
    └── jaffle_shop/    # Original jaffle shop project (NOT COMMITTED - in .gitignore)
```

**Commit the test fixtures:**

```bash
# Navigate to project root
cd /Users/eka/Desktop/personal/correlator-io/correlator-dbt

# Stage the test fixture files
git add tests/fixtures/run_results.json
git add tests/fixtures/manifest.json

# Commit with descriptive message
git commit -m "feat: add dbt jaffle shop test fixtures for parser testing

- Add run_results.json with test execution results
- Add manifest.json with node definitions
- Generated from dbt jaffle shop example project
- Used for Task 1.2 (dbt Artifact Parser) test fixtures"
```

---

## Step 6: Verify Artifacts Structure

Inspect the generated artifacts to ensure they are valid and contain the required data.

> **Note:** Artifact structure varies by dbt version. Always check the `dbt_schema_version` in the artifact metadata for the authoritative schema definition.

### Validate Artifacts are Valid JSON

```bash
# Navigate to fixtures directory
cd tests/fixtures

# Validate run_results.json
python -m json.tool run_results.json > /dev/null && echo "✓ run_results.json is valid JSON" || echo "✗ Invalid JSON"

# Validate manifest.json
python -m json.tool manifest.json > /dev/null && echo "✓ manifest.json is valid JSON" || echo "✗ Invalid JSON"
```

### Check Schema Versions

```bash
# Check run_results.json schema version
python -c "import json; print('run_results schema:', json.load(open('run_results.json'))['metadata']['dbt_schema_version'])"

# Check manifest.json schema version
python -c "import json; print('manifest schema:', json.load(open('manifest.json'))['metadata']['dbt_schema_version'])"

# Example output:
# run_results schema: https://schemas.getdbt.com/dbt/run-results/v6.json
# manifest schema: https://schemas.getdbt.com/dbt/manifest/v12.json
```

### Inspect run_results.json

```bash
# View metadata and first test result
python -m json.tool run_results.json | head -50

# Count test results
python -c "import json; results = json.load(open('run_results.json'))['results']; print(f'Test results: {len(results)}')"

# Check test statuses
python -c "import json; results = json.load(open('run_results.json'))['results']; statuses = [r['status'] for r in results]; print('Statuses:', set(statuses))"
```

**What to verify:**
- ✓ `metadata` section exists with `dbt_version`, `generated_at`, `invocation_id`
- ✓ `results` array contains test execution data
- ✓ Each result has `status` (pass/fail/error/warn/skipped)
- ✓ Each result has `unique_id` (test identifier)
- ✓ Each result has `compiled_code` (SQL that was executed)

### Inspect manifest.json

```bash
# View metadata
python -c "import json; m = json.load(open('manifest.json')); print('dbt version:', m['metadata']['dbt_version']); print('adapter:', m['metadata']['adapter_type'])"

# Count nodes by type
python -c "import json; nodes = json.load(open('manifest.json'))['nodes']; types = {};
for k, v in nodes.items():
    t = v['resource_type'];
    types[t] = types.get(t, 0) + 1;
print('Node counts:', types)"
```

**What to verify:**
- ✓ `metadata` section exists with `dbt_version`, `project_name`, `adapter_type`
- ✓ `nodes` object contains model and test definitions
- ✓ Test nodes can be identified by `resource_type: "test"`
- ✓ Nodes contain dataset references (database, schema, name)

### Understanding Schema Structure

To understand the exact structure for the dbt version used:

1. **Visit the schema URLs** found in the artifacts:
   - For `run_results.json`: Visit the `dbt_schema_version` URL (e.g., https://schemas.getdbt.com/dbt/run-results/v6.json)
   - For `manifest.json`: Visit the `dbt_schema_version` URL (e.g., https://schemas.getdbt.com/dbt/manifest/v12.json)

2. **Reference official dbt documentation:**
   - [run-results.json reference](https://docs.getdbt.com/reference/artifacts/run-results-json)
   - [manifest.json reference](https://docs.getdbt.com/reference/artifacts/manifest-json)

3. **Inspect actual artifacts:**
   ```bash
   # Use jq for easier inspection (if installed)
   jq '.results[0]' run_results.json  # First test result
   jq '.nodes | keys | .[0]' manifest.json  # First node key
   jq '.nodes[.nodes | keys | .[0]]' manifest.json  # First node details
   ```

---

## Troubleshooting

### Issue: `dbt command not found`

**Solution:**
```bash
# Ensure virtual environment is activated - use `make start` to create the virtual environment if it does not exist
cd /Users/eka/Desktop/personal/correlator-io/correlator-dbt
source .venv/bin/activate

# Verify Python installation
python --version

# Add/reinstall dbt with uv
uv add --dev dbt-core dbt-duckdb

# Verify installation
dbt --version
```

### Issue: `No such file or directory: profiles.yml`

**Solution:**
```bash
# Create profiles.yml in the jaffle_shop directory (local to project)
cd tests/fixtures/sample_dbt_project/jaffle_shop

# Create profiles.yml as shown in Step 3
cat > profiles.yml << 'EOF'
jaffle_shop:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: 'jaffle_shop.duckdb'
      threads: 4
EOF
```

### Issue: `Tests failed to run`

**Solution:**
```bash
# Ensure you're in the jaffle_shop directory with profiles.yml
cd tests/fixtures/sample_dbt_project/jaffle_shop

# Check dbt logs
cat logs/dbt.log

# Run with debug flag
dbt test --debug

# Verify database connection
dbt debug
```

### Issue: `DuckDB adapter not found`

**Solution:**
```bash
# Activate virtual environment - use `make start` to create the virtual environment if it does not exist
source .venv/bin/activate

# Add dbt-duckdb with uv
uv add --dev dbt-duckdb

# Verify adapter installed
dbt --version | grep duckdb
```

### Issue: Virtual environment not activated

**Solution:**
```bash
# Navigate to project root
cd /Users/eka/Desktop/personal/correlator-io/correlator-dbt

# Activate virtual environment - use `make start` to create the virtual environment if it does not exist
source .venv/bin/activate

# You should see (.venv) prefix in your terminal prompt
```

---

## Regenerating Artifacts

If you need to regenerate artifacts (e.g., after dbt version upgrade):

```bash
# Ensure virtual environment is activated
cd /Users/eka/Desktop/personal/correlator-io/correlator-dbt
source .venv/bin/activate

# Navigate to jaffle shop
cd tests/fixtures/sample_dbt_project/jaffle_shop

# Clean previous runs
dbt clean

# Run complete workflow
dbt seed
dbt run
dbt test

# Copy new artifacts
cp target/run_results.json ../../run_results.json
cp target/manifest.json ../../manifest.json

# Commit the updated fixtures
cd /Users/eka/Desktop/personal/correlator-io/correlator-dbt
git add tests/fixtures/run_results.json tests/fixtures/manifest.json
git commit -m "chore: regenerate dbt test fixtures with updated dbt version"

# Update README with new generation date
```

---

## Next Steps

After completing this setup:

1. Write parser tests using these artifacts
2. Implement parser.py to parse these files
3. Use jaffle_shop for end-to-end integration testing

---

## References

- **Jaffle Shop Repository:** https://github.com/dbt-labs/jaffle-shop
- **dbt Documentation:** https://docs.getdbt.com/
- **dbt-duckdb Adapter:** https://github.com/duckdb/dbt-duckdb
- **DuckDB Documentation:** https://duckdb.org/docs/

---

**Setup Status:** Follow steps 1-6 above to complete the setup.
