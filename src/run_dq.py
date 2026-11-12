import os
import sys
import logging
from datetime import timedelta

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, compute

from databricks.sdk.errors import OperationFailed

from pathlib import Path
import yaml

CONFIG_PATH = "../config/config.yml"

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

NOTEBOOK_PATH = config["PIPELINE"]["DQ_PATH"]
CLUSTER_ID = config["PIPELINE"]["CLUSTER_ID"]

TIMEOUT_HOURS = 2

def run_dq():
    client = WorkspaceClient()

    waiter = client.jobs.submit(
        run_name="OpenFoodFacts Data Quality Check",
        tasks=[
            jobs.SubmitTask(
                task_key="data_quality_check",
                existing_cluster_id=CLUSTER_ID,
                notebook_task=jobs.NotebookTask(
                    notebook_path=NOTEBOOK_PATH,
                    source=jobs.Source.WORKSPACE
                )
            )
        ]
    )

    logging.info("Submitted run ID: %s", waiter.run_id)

    def report_status(run: jobs.Run):
        if run.state:
            logging.info(
                "Run status: %s | Result: %s",
                run.state.life_cycle_state,
                run.state.result_state
            )

    result = waiter.result(
        timeout=timedelta(hours=TIMEOUT_HOURS),
        callback=report_status
    )

    result_state = result.state.result_state

    logging.info("Final result: %s", result_state)
    logging.info("Run URL: %s", result.run_page_url)

    if result_state != jobs.RunResultState.SUCCESS:
        raise RuntimeError(
            f"Data quality notebook failed: {result_state}"
        )

    logging.info("Data quality completed successfully")

if __name__ == "__main__":
    try:
        run_dq()
    except Exception:
        logging.exception("Data quality failed")
        sys.exit(1)