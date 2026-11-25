# OpenFoodFacts Data Engineering Pipeline

## Overview

This repository implements a Databricks data engineering workflow for ingesting OpenFoodFacts product snapshots, maintaining a current product table, producing a historical Silver table, and building current-state Gold tables for analysis.

The deployed workflow is a Databricks multi-task Job managed as a Databricks Asset Bundle. It is not implemented as a Lakeflow Declarative Pipeline.

The pipeline follows these layers:

1. Staging stores the downloaded compressed source file in a Unity Catalog Volume.
2. Bronze appends each ingested source snapshot to a raw Delta table.
3. Bronze current state reconciles the newest snapshot against the existing current table and emits Delta Change Data Feed (CDF) changes.
4. Silver cleans and standardizes product records, tracks product history with custom SCD Type 2 logic, and stores a CDF checkpoint.
5. Gold derives product, brand, and product-entry tables from current Silver rows.
6. A separate data quality notebook checks Silver and Gold outputs, quarantines invalid rows, reconciles product keys, and fails its run when configured gates fail.

## Repository layout

| Path | Purpose |
| --- | --- |
| `databricks.yml` | Databricks Asset Bundle definition, variables, workspace targets, and per-target catalog/schema values. |
| `resources/openfoodfacts_pipeline_by_a_gulidov.job.yml` | Multi-task Job resource and task dependencies. |
| `resources/openfoodfacts_dashboard.dashboard.yml` | Bundle-managed AI/BI dashboard resource. |
| `config/config.yml` | Pipeline table names, source URLs, Silver mappings and validation reference data, Gold output columns, permissions, and job metadata. |
| `config/config.ipynb` | Notebook configuration loader. Reads YAML, defines Databricks widgets, constructs table and Volume paths, and grants selected permissions for service-principal runs. |
| `setup/setup.ipynb` | Creates schemas, the staging Volume, security functions, and grants. |
| `notebooks/Staging.ipynb` | Downloads the source snapshot. |
| `notebooks/Bronze.ipynb` | Reads the source file and appends it to raw Bronze. |
| `notebooks/Bronze_current.ipynb` | Builds and reconciles the current Bronze snapshot, hashes rows, and reads its CDF. |
| `notebooks/Silver.ipynb` | Bootstraps Silver, processes Bronze CDF versions, applies SCD Type 2, and advances the checkpoint. |
| `notebooks/Gold.ipynb` | Builds and overwrites the Gold product, brand, and fact tables. |
| `notebooks/dq_tests.ipynb` | Runs data quality rules and reconciliation gates. |
| `src/silver.py` | Reusable Silver cleansing and SCD Type 2 functions. |
| `src/gold.py` | Reusable Gold table construction. |
| `src/dq_tests.py` | Data quality rules, quarantine writes, and shared DQ runner. |
| `src/run_pipeline.py` | Python SDK entry point that starts and waits for the configured Databricks Job. |
| `src/run_dq.py` | Python SDK entry point that starts the separate DQ notebook run. |
| `src/create_cluster.py` | Helper that creates an all-purpose Databricks cluster. |
| `tests/conftest.py` | Session-scoped local PySpark fixture. |
| `tests/test_silver.py` | Unit tests for Silver mapping, preparation, and synthetic SCD2 behavior. |
| `tests/test_gold.py` | Unit test for current-row filtering, derived keys, and Gold relationships. |
| `.github/workflows/ci-cd.yml` | GitHub Actions validation and production deployment workflow. |
| `pyproject.toml` | Python package metadata, runtime dependencies, test extras, and pytest settings. |

## Architecture and data flow

### Job task graph

The bundle Job runs these tasks in dependency order:

```text
Setup
  |
Staging
  |
Bronze
  |
Bronze_CDC
  |
Silver
  |
Gold
```

The Job resource is named `Prod OpenFoodFacts Pipeline`. It limits concurrent runs to one and has a daily schedule configured for the Europe/Kiev timezone. The schedule is currently configured as paused. GitHub Actions can still start a run explicitly after deploying the production bundle.

The DQ notebook is not a task in this Job graph. It is a separate workflow that can be started with `src/run_dq.py` or through a direct notebook/job run.

### Staging

The source URL is configured in `config/config.yml` as the OpenFoodFacts product export. The staging notebook downloads the compressed file to the configured Unity Catalog Volume. The default file path is intended to be:

```text
/Volumes/<catalog>/<staging_schema>/<staging_volume>/products_raw.csv.gz
```

The setup notebook creates the schemas and Volume if they do not exist.

### Bronze raw snapshot

The Bronze notebook reads the compressed, tab-delimited source file with a header and multiline support. It adds an `ingested_at` timestamp and appends all columns into:

```text
<catalog>.<bronze_schema>.products_bronze
```

The append operation enables schema merging, so source schema changes can be represented in the Delta table.

### Bronze current state and change feed

The Bronze current notebook maintains:

```text
<catalog>.<bronze_schema>.products_bronze_current
```

On first creation it selects the most recently ingested row for each product code. For subsequent snapshots it:

1. Finds the newest `ingested_at` value.
2. Selects that snapshot and removes rows without a product code.
3. Deduplicates by `code`, preferring the most recently modified product record.
4. Computes a SHA-256 payload hash from the source fields, excluding ingestion metadata and the hash itself.
5. Merges the snapshot into the current table. Changed records are updated, new records are inserted, and products absent from the full snapshot are deleted.
6. Reads CDF records for the Delta versions written by the merge.

### Silver standardization and history

Silver uses `product_id` as the business key and writes:

```text
<catalog>.<silver_schema>.products_silver
<catalog>.<silver_schema>.cdf_checkpoint
```

The initial Silver load reads a fixed Bronze current-state Delta version. Later runs process Bronze CDF one version at a time, starting after the saved checkpoint. The checkpoint stores the pipeline name, source table, last processed Bronze version, and update timestamp.

The reusable transformation in `src/silver.py`:

- Selects the configured source columns and fails if required columns are missing.
- Deduplicates by product code using the latest modification time.
- Parses product timestamps.
- Removes locale prefixes such as language tags and removes month/year placeholder strings from selected text fields.
- Normalizes product names, packaging, brands, ingredients, and location fields.
- Maps configured country codes to country names.
- Replaces invalid or out-of-range values for selected numeric and classification fields.
- Converts product completeness to a percentage string.
- Derives energy in kilocalories from kilojoules.
- Sets saturated fat to zero when it exceeds total fat.
- Replaces configured null or unknown text fields and renames source columns into the Silver naming convention.

Custom SCD Type 2 logic computes a hash over the configured Silver business columns. New products are inserted as current. Unchanged products do not create a new version. Changed products close their current record by setting `valid_to` and `is_current = false`, then insert a new current version with a new `valid_from`, a null `valid_to`, and `is_current = true`.

### Gold analytical tables

Gold uses only rows where Silver `is_current = true`. It derives `entry_id` from `product_id` and `brand_id` from `brands_list`, selects the configured output columns, deduplicates the outputs, and overwrites the Gold Delta tables:

| Table | Grain and purpose |
| --- | --- |
| `dim_products` | One current product record with product, classification, location, and nutrition attributes. |
| `dim_brands` | Brand identifiers, brand names, and brand owners. For repeated brand rows, the transformation prefers a non-null, non-`N/A` owner. |
| `fact_product_entries` | Product entry identifiers, product and brand keys, creator, completeness, and product lifecycle timestamps. |

The Gold output schema and selected columns are defined in `config/config.yml`.

## Data quality and governance

### Data quality

The shared checks in `src/dq_tests.py` define:

- Silver product IDs as non-null and non-empty.
- Uniqueness of product IDs in the current Silver set.
- NOVA groups within the accepted range when present.
- Non-null `is_current` flags.
- Non-null and unique product IDs in Gold products.
- Non-null and unique brand IDs in Gold brands.
- Non-null and unique entry IDs in the Gold fact table.

The DQ notebook also checks for orphan product and brand references in the fact table, compares current Silver product keys with Gold product keys, writes invalid or reconciliation rows to configured quarantine tables, and raises an error if any gate has failures.

### Governance

The setup notebook creates two Gold schema functions:

- `filter_products` returns true for members of the configured account group when the product country includes Ukraine.
- `mask_entry_creator` returns the creator for group members and `REDACTED` otherwise.

It grants the configured group catalog and Gold schema access, including SELECT when Gold read access is enabled, and grants EXECUTE on the two functions. For service-principal execution, the config notebook grants the service principal schema privileges and read access to the source Volume.

These functions are created by setup; their presence alone does not apply them to a table or column. The repository also contains a separate governance notebook for table-level policies.

## Technology stack

| Component | Use |
| --- | --- |
| Python 3.12 | Project and automation runtime. |
| PySpark 4.0 | Distributed transformations and notebook processing. |
| Delta Lake | Bronze, Silver, and Gold storage, transactional merges, history, and CDF reads. |
| Databricks notebooks and Unity Catalog | Compute, governed tables, schemas, functions, and source Volume. |
| Databricks Asset Bundles | Declarative deployment of the Job and dashboard resources. |
| Databricks SDK for Python | Start and monitor Job runs from Python. |
| Databricks Labs DQX | Row-level and dataset-level quality checks. |
| PyYAML | Read pipeline configuration from YAML. |
| pytest and pytest-cov | Local unit tests and coverage reporting. |
| GitHub Actions | Automated testing, bundle validation, and production deployment. |

Python dependencies are declared in `pyproject.toml`. The project requires Python `>=3.12,<3.13`; PySpark is pinned to `4.0.0`. The test extra installs pytest, pytest-cov, and Delta Lake.

## Configuration

### Bundle targets

`databricks.yml` defines two targets:

| Target | Catalog | Staging schema | Bronze schema | Silver schema | Gold schema |
| --- | --- | --- | --- | --- | --- |
| `dev` | `dbr_dev_ua5816bd` | `gulidov_artem_staging` | `gulidov_artem_bronze` | `gulidov_artem_silver` | `gulidov_artem_gold` |
| `prod` | `dbr_dev_ua5816bd` | `prod_gulidov_artem_staging` | `prod_gulidov_artem_bronze` | `prod_gulidov_artem_silver` | `prod_gulidov_artem_gold` |

The production target runs as the configured service principal and sets a bundle-specific workspace root path. The development target is the default bundle target.

### Pipeline configuration

Use `config/config.yml` for source URLs, table names, the Silver business key and mappings, country mappings, allowed product categories, numeric columns, Gold output columns, quarantine names, permissions, and the configured Job ID.

Use `databricks.yml` for environment-specific resource placement. Avoid duplicating environment schema values in notebook code; notebooks obtain them from widgets defined in `config/config.ipynb`.

## Local setup and development

### Prerequisites

- Git and Python 3.12.
- Java 17 for local Spark execution.
- Databricks CLI compatible with Asset Bundles.
- Databricks workspace access and permissions on the target catalog, schemas, and Volume.
- A configured Databricks authentication profile for local bundle validation or deployment.

### Install dependencies

From the repository root, create and activate a virtual environment, then install the package with test dependencies.

PowerShell:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[test]"
```

Linux or macOS:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[test]"
```

### Configure workspace access

Configure the Databricks CLI and SDK to use an authorized workspace profile. The bundle target contains the workspace host and environment settings. Authentication credentials should be provided through the local CLI profile or an approved environment-based authentication method, not committed to the repository.

### Validate and deploy the bundle

From the repository root:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

Bundle deployment creates or updates the resources declared in the bundle, including the multi-task Job and the dashboard resource. To run the Job through the bundle:

```bash
databricks bundle run openfoodfacts_pipeline_by_a_gulidov -t dev
```

A production deployment uses `-t prod`. Production deployment is automated by GitHub Actions as described below.

### Run local unit tests

```bash
python -m pytest
```

The CI command also collects coverage:

```bash
python -m pytest --cov
```

These tests exercise pure transformation logic and synthetic Spark DataFrames. They do not deploy resources to Databricks. Tests that exercise Delta-backed SCD2 logic use a unique test table and should clean it up after execution.

### Run the pipeline through the SDK helper

`src/run_pipeline.py` reads `PIPELINE.JOB_ID` from `config/config.yml`, starts that Databricks Job, logs task state, waits up to two hours, and exits with an error if the run does not succeed.

```bash
python src/run_pipeline.py
```

Confirm that the configured Job ID and target environment are correct before using this helper. It does not select the bundle target; it starts the exact Job ID in YAML.

### Run data quality independently

`src/run_dq.py` starts the notebook path and existing cluster ID configured in `config/config.yml`. It waits up to two hours and exits nonzero if the run fails.

```bash
python src/run_dq.py
```

The configured notebook path and cluster must exist in the workspace. The DQ notebook is separate from the six-task ETL Job.

## GitHub Actions CI/CD

The workflow is `.github/workflows/ci-cd.yml`. It runs for pull requests and pushes targeting the `Lab11` branch, and for manual `workflow_dispatch` events.

### Unit test job

The `unit-tests` job runs on Ubuntu and:

1. Checks out the repository.
2. Installs Java 17.
3. Installs Python 3.12.
4. Installs the project and test extras with `python -m pip install -e ".[test]"`.
5. Runs `python -m pytest --cov`.

### Bundle validation job

The `bundle-validation` job validates the production bundle with `databricks bundle validate -t prod`. It uses the GitHub `prod` environment and Azure client-secret authentication supplied through these environment secrets:

- `ARM_TENANT_ID`
- `ARM_CLIENT_ID`
- `ARM_CLIENT_SECRET`

The job sets `DATABRICKS_AUTH_TYPE=azure-client-secret`.

### Production deployment job

The `deploy-prod` job requires both unit tests and bundle validation to succeed. It runs for pushes and manual dispatches, not for pull requests. It:

1. Deploys the production bundle with `databricks bundle deploy -t prod`.
2. Lists jobs as a basic deployment verification.
3. Installs Python 3.12 and the Databricks SDK and PyYAML.
4. Runs `src/run_pipeline.py` to start the configured Job and wait for its result.

The job uses the GitHub `prod` environment, the same Azure secret names, and a concurrency group named `openfoodfacts-prod-deploy` to serialize production deployments.

## Operational behavior and current caveats

- The Job resource has a paused schedule. A manual run or the CI deployment helper can still trigger it explicitly.
- Gold tables are written in overwrite mode. Gold is a current-state presentation layer and does not retain Silver's history.
- The raw Bronze table appends each full source snapshot. Bronze current state reconciles complete snapshots and deletes rows absent from the newest snapshot.
- Silver processes only CDF `insert` and `update_postimage` events. CDF delete events are not passed to the SCD2 function, so source deletions may remain marked current in Silver and therefore appear in Gold. Define and implement a deletion policy before relying on source deletions to propagate.
- The notebooks Staging and Bronze refer to `VOLUME_PATH`, while `config/config.ipynb` defines `STAGING_VOLUME`. Unless `VOLUME_PATH` is provided by another workspace mechanism, this name mismatch can prevent those notebook tasks from reading or writing the configured file.
- `config/config.yml` contains a fixed Job ID. `src/run_pipeline.py` starts that ID regardless of bundle target. Verify the ID resolves to the intended environment before invoking it.
- `src/run_dq.py` reads its YAML using a relative path, unlike `src/run_pipeline.py`, which resolves the path from the script location. Run it from a working directory where `../config/config.yml` resolves correctly or adjust the path handling before relying on it.
- The DQ path and cluster ID in the YAML are explicit workspace references. Keep them aligned with the deployed notebook and compute resource.
- The bundle-managed dashboard configuration is separate from the ETL transformations. The Job itself has six tasks; dashboard deployment is handled as another bundle resource.

## Troubleshooting guide

- If a notebook cannot find the source file, check the Volume path, the `raw_csv` Volume permission, and the mismatch between `VOLUME_PATH` and `STAGING_VOLUME`.
- If Bronze contains unexpected duplicates, inspect the `ingested_at` value and the source product `code` values for the latest snapshot.
- If Silver does not advance, inspect `cdf_checkpoint.last_processed_version`, the Bronze current Delta history, and whether new CDF versions exist.
- If Silver and Gold disagree, inspect the DQ reconciliation outputs and confirm that the DQ job is running against the intended target schemas.
- If bundle deployment points at an unexpected schema, confirm the target argument, such as `-t dev` or `-t prod`, and inspect the resolved bundle variables.
- If a GitHub deployment fails before resource deployment, review the unit-test result, bundle validation logs, GitHub environment configuration, and Azure authentication secrets.
- If CI deploys resources but the ETL run fails, inspect the Databricks Job run URL and task-level notebook logs.

## Change management

- Make transformations reusable in `src/` and keep orchestration in notebooks.
- Keep table names and transformation mappings in `config/config.yml`.
- Keep target-specific catalog and schema values in `databricks.yml`.
- Add or adjust unit tests for transformation behavior using synthetic local Spark DataFrames.
- Validate a bundle target before deployment.
- Use pull requests to the configured integration branch so unit tests and production bundle validation run before merging.
- Production deployment is performed by the GitHub Actions workflow after a push to `Lab11` or a manual workflow dispatch.

