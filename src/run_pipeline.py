import os
import sys
import logging
from datetime import timedelta
from databricks.sdk.service import compute

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

from pathlib import Path
import yaml

CONFIG_PATH = "../config/config.yml"

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

JOB_ID = config["PIPELINE"]["JOB_ID"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Cluster config
CLUSTER_NAME = "OpenFoodFacts Custom Compute"
NODE_TYPE_ID = "Standard_DS3_v2"

NUM_WORKERS = 1
AUTOTERMINATION_MINUTES = 30

def create_cluster():
    client = WorkspaceClient()

    logging.info("Selecting the latest Long-Term Support Databricks Runtime...")

    spark_version = client.clusters.select_spark_version(
        latest=True,
        long_term_support=True
    )

    logging.info("Selected Spark version: %s", spark_version)
    logging.info("Creating all-purpose compute: %s", CLUSTER_NAME)

    cluster = client.clusters.create_and_wait(
        cluster_name=CLUSTER_NAME,
        spark_version=spark_version,
        node_type_id=NODE_TYPE_ID,
        num_workers=NUM_WORKERS,
        autotermination_minutes=AUTOTERMINATION_MINUTES,
        data_security_mode=compute.DataSecurityMode.DATA_SECURITY_MODE_STANDARD
    )

    cluster_url = (
        f"{client.config.host}"
        f"#setting/clusters/{cluster.cluster_id}/configuration"
    )

    logging.info("Cluster successfully created!")
    logging.info("Cluster ID: %s", cluster.cluster_id)
    logging.info("Cluster state: %s", cluster.state)
    logging.info("Cluster URL: %s", cluster_url)

def run_pipeline():
    client = WorkspaceClient()

    logging.info("Starting pipeline...")

    waiter = client.jobs.run_now(job_id=JOB_ID)

    logging.info("Started job run: %s", waiter.run_id)

    def report_status(run: jobs.Run):
        for task in run.tasks or []:
            if task.state:
                logging.info(
                    "%s: %s",
                    task.task_key,
                    task.state.life_cycle_state
                )

    result = waiter.result(
        timeout=timedelta(hours=2),
        callback=report_status
    )

    result_state = result.state.result_state

    logging.info("Final job status: %s", result_state)
    logging.info("Run URL: %s", result.run_page_url)

    if result_state != jobs.RunResultState.SUCCESS:
        raise RuntimeError(
            f"Pipeline execution failed: {result_state}"
        )

    logging.info("Pipeline completed successfully!")

if __name__ == "__main__":
    try:
        #create_cluster() 
        run_pipeline()
    except Exception:
        logging.exception("Script failed")
        sys.exit(1)