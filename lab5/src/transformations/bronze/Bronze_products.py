from pyspark import pipelines as dp
from pyspark.sql import functions as F

VOLUME_PATH = spark.conf.get("lab5.batch.staging_volume")

BRONZE_TABLE = f"{spark.conf.get("lab5.catalog")}.{spark.conf.get("lab5.bronze_schema")}.{spark.conf.get("lab5.batch_bronze_table_name")}"

FILE_PATTERN = r"products_raw_(\d{8})T(\d{6})Z\.csv\.gz"

@dp.table(
    name=BRONZE_TABLE,
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true"
    }
)
def products_bronze():
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("delimiter", "\t")
        .option("multiLine", "true")
        .option("escape", '"')
        .option("inferSchema", "false")
        .load(VOLUME_PATH)
    )

    filename = F.col("_metadata.file_name")

    snapshot_date = F.regexp_extract(
        filename, FILE_PATTERN, 1
    )
    snapshot_time = F.regexp_extract(
        filename, FILE_PATTERN, 2
    )

    df = (
        df
        .withColumn("source_file", filename)
        .withColumn(
            "_snapshot_version",
            F.concat(snapshot_date, snapshot_time).cast("long")
        )
    )

    df = df.filter(
        F.col("_snapshot_version").isNotNull()
        & (F.col("_snapshot_version") > 0)
    )

    return df