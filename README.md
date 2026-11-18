## Incremental Data Processing with Change Data Feed

Implemented an incremental processing workflow using **Delta Lake Change Data Feed (CDF)** to avoid repeatedly processing the entire OpenFoodFacts dataset.

- **Bronze Current State:** Created a table maintaining the latest version of each product, using Delta MERGE operations against incoming snapshots.
- **Change Data Feed:** Enabled CDF on the current-state table to capture inserts, updates, and deletes.
- **Incremental Silver Processing:** Modified the Silver pipeline to consume CDF events, apply data cleaning and transformations, and maintain historical product records using SCD Type 2.
- **Checkpoint Management:** Implemented a Delta checkpoint table to track the last successfully processed Bronze version, allowing Silver processing to resume from the correct point during subsequent executions.
- **Job Orchestration:** Integrated the new processing steps into the Databricks Asset Bundle job, establishing dependencies between Bronze ingestion, Bronze CDC processing, and Silver transformations.

## Lakehouse Federation with Supabase PostgreSQL

Implemented **Databricks Lakehouse Federation** to integrate externally stored PostgreSQL data into the analytics layer.

- Migrated the `dim_brands` dimension table from the Databricks Lakehouse to a **Supabase PostgreSQL** instance.
- Configured a PostgreSQL connection and a foreign catalog in Databricks to access the external database.
- Used federated queries to combine the externally stored brand dimension with locally stored Gold tables.
- Integrated these joins into a Databricks SQL dashboard query, allowing dashboard visualizations to use data from both the Databricks Lakehouse and Supabase.
