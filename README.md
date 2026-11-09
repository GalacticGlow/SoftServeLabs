# OpenFoodFacts Pipeline — CI/CD: DEV → PROD Promotion

The OpenFoodFacts Data Engineering project uses **Databricks Asset Bundles (DAB)** to manage DEV and PROD deployments from a single codebase. GitHub Actions handles automated testing, bundle validation, and production deployment.

The pipeline follows a medallion architecture (Bronze → Silver → Gold), processing OpenFoodFacts product snapshots and maintaining historical product records.

## Environments

Both targets use the same Databricks workspace and Unity Catalog catalog, with environment isolation provided by separate schemas, bundle targets, and deployment paths.

| | **`dev`** | **`prod`** |
|---|---|---|
| Bundle mode | `development` | `production` |
| Workspace | Azure Databricks workspace | Same workspace |
| Catalog | `dbr_dev_ua5816bd` | `dbr_dev_ua5816bd` |
| Staging schema |`gulidov_artem_staging`|`prod_gulidov_artem_staging`|
| Bronze schema | `gulidov_artem_bronze` | `prod_gulidov_artem_bronze` |
| Silver schema | `gulidov_artem_silver` | `prod_gulidov_artem_silver` |
| Gold schema | `gulidov_artem_gold` | `prod_gulidov_artem_gold` |
| Landing volume | `raw_csv` | `raw_csv` |
| Job execution identity | Deploying user | Configured service principal |

All environment-specific values are defined as variables in `databricks.yml` and overridden by the corresponding target. The notebooks and transformations use these values through configuration and job parameters instead of hardcoding environment-specific catalog or schema names.

## Project structure

| **Path** | **Content** |
|---|---|
| `databricks.yml` | Main Asset Bundle configuration: variables, DEV/PROD targets, workspace settings, and deployment configuration. |
| `resources/` | Databricks resource definitions, including the OpenFoodFacts pipeline job. |
| `resources/openfoodfacts_job.yml` | Main ETL job configuration and task dependencies. |
| `src/` | Pipeline implementation: Bronze ingestion, Silver transformations, Gold transformations, and supporting code/notebooks. |
| `tests/` | Pytest unit tests for pipeline transformation logic. |
| `.github/workflows/` | GitHub Actions workflows for automated testing, validation, and deployment. |

## CI/CD workflow

The project uses GitHub Actions to automate validation and production deployment. The `Lab8` branch is used as the integration and deployment branch.

```text
Pull request to Lab8
        │
        ▼
  GitHub Actions
        │
        ├── Run unit tests (pytest)
        │
        └── Validate PROD bundle
                  │
                  ▼
             Merge to Lab8
                  │
                  ▼
          Push / workflow dispatch
                  │
                  ▼
          Deploy PROD bundle
                  │
                  ▼
       Update Databricks resources
```

- **Unit tests:** Run automatically using pytest. They verify the correctness of the pipeline's transformation logic.
- **Bundle validation:** Executes `databricks bundle validate -t prod` to check the production bundle configuration.
- **Production deployment:** After a push to `Lab8` (or a manual workflow dispatch from that branch), GitHub Actions deploys the production bundle using `databricks bundle deploy -t prod`.
- **Authentication:** The deployment workflow authenticates using Azure service principal credentials stored as GitHub Environment secrets (`ARM_TENANT_ID`, `ARM_CLIENT_ID`, and `ARM_CLIENT_SECRET`). The production deployment uses the `prod` GitHub Environment.
- **Deployment concurrency:** A GitHub Actions concurrency group prevents simultaneous production deployments.

The deployment process updates Databricks resources and bundle files; it does **not** automatically execute the ETL job. The job can be started separately through Databricks Workflows, its configured schedule, or `databricks bundle run`.

## Deployment management

The project uses Databricks Asset Bundles to keep resource configuration version-controlled and reproducible.

Useful commands:

```bash
# Validate the DEV bundle
databricks bundle validate -t dev

# Validate the PROD bundle
databricks bundle validate -t prod

# Deploy DEV resources
databricks bundle deploy -t dev

# Deploy PROD resources
databricks bundle deploy -t prod

# Run the PROD ETL job
databricks bundle run openfoodfacts_pipeline_by_a_gulidov -t prod
```

The production job is named **`Prod OpenFoodFacts Pipeline`**. Its daily schedule is configured for 12:32 (`Europe/Kiev`), but the schedule is currently paused.

Bundle deployments are designed to be repeatable: running `bundle deploy` again without configuration changes should not recreate or duplicate existing resources.

## Managed infrastructure and resources

The bundle manages the production ETL job, its configuration, deployment files, and associated permissions.

The Unity Catalog catalog, schemas, and staging volumes are provisioned separately rather than recreated on every bundle deployment. This avoids conflicts with existing resources and keeps infrastructure provisioning separate from regular pipeline deployments.
