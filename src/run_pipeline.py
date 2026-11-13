import os
import sys
import logging
from datetime import timedelta
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

from pathlib import Path
import yaml

CONFIG_PATH = "/Workspace/Users/art.gulidov@softserve.academy/Lab9/config/config.yml"

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

JOB_ID = config["PIPELINE"]["JOB_ID"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

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
        run_pipeline()
    except Exception:
        logging.exception("Script failed")
        sys.exit(1)