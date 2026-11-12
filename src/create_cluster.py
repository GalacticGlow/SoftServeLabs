from databricks.sdk.service import compute
from databricks.sdk import WorkspaceClient
import logging

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

if __name__ == "__main__":
    create_cluster()