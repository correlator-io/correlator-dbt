# Test Fixtures

This directory contains test data and sample dbt artifacts used for testing.

## Structure

```
fixtures/
├── README.md              # This file
├── dbt_test_results.json  # Output from `dbt test` ✅
├── dbt_run_results.json   # Output from `dbt run` ✅
├── manifest.json          # Sample dbt manifest.json ✅
└── sample_dbt_project/    # Sample dbt project (jaffle_shop, .gitignored)
```

## Artifact Metadata

**Generated:** December 9, 2025
**dbt Version:** 1.10.15
**dbt Adapter:** duckdb 1.10.0
**Source Project:** [dbt-labs/jaffle-shop](https://github.com/dbt-labs/jaffle-shop)
**Schema Version:**
- run_results: v6 (https://schemas.getdbt.com/dbt/run-results/v6.json)
- manifest: v12 (https://schemas.getdbt.com/dbt/manifest/v12.json)

## Files

### `dbt_test_results.json` ✅
Output from `dbt test` command - test execution results from jaffle shop project.

**Contains:**
- **27 test results** (all passed in this sample)
- Test execution timing information
- Invocation ID: `1e651364-45a1-4a76-9f21-4b69fa49a65f`
- Generated timestamp: `2025-12-09T18:34:51.064443Z`
- Adapter response metadata
- Compiled SQL for each test

**Test Types in Sample:**
- `accepted_values` - Column value validation
- `not_null` - NULL constraint validation
- `unique` - Uniqueness constraint validation
- `relationships` - Foreign key validation

### `dbt_run_results.json` ✅
Output from `dbt run` command - model execution results from jaffle shop project.

**Contains:**
- **13 model results** (all successful in this sample)
- Model execution timing information
- Invocation ID for correlation with lineage events
- Generated timestamp for eventTime
- Adapter response metadata (DuckDB - no row counts)

**Key Data for Parser Testing:**
- `model.jaffle_shop.customers` - mart model with multiple dependencies
- `model.jaffle_shop.orders` - mart model
- `model.jaffle_shop.stg_customers` - staging model with source dependency

**Note:** DuckDB adapter doesn't return `rows_affected` in adapter_response.
Postgres, Snowflake, and BigQuery adapters do provide this field.

### `manifest.json` ✅
Sample dbt manifest file with complete project metadata.

**Contains:**
- **13 models** (7 staging views + 6 mart tables)
- **27 data tests** (schema tests)
- **6 source definitions** (raw data tables)
- **19 metrics** (business metrics)
- **6 semantic models** (MetricFlow definitions)
- Database: `jaffle_shop` (DuckDB)
- Schema: `main`

**Key Nodes for Parser Testing:**
- `model.jaffle_shop.stg_products`
- `model.jaffle_shop.stg_customers`
- `model.jaffle_shop.stg_orders`
- `test.jaffle_shop.unique_customers_customer_id`
- `test.jaffle_shop.not_null_customers_customer_id`

### `sample_dbt_project/jaffle_shop/` (Not Committed)
The cloned jaffle shop project used to generate the artifacts above. This directory is in `.gitignore` and should not be committed.

**How to Regenerate:**
See `docs/JAFFLE_SHOP_SETUP.md` for complete instructions.
