from pyspark import pipelines as dp
from pyspark.sql import functions as F

BRONZE_STREAMING_TABLE = f"{spark.conf.get("lab5.catalog")}.{spark.conf.get("lab5.bronze_schema")}.{spark.conf.get("lab5.streaming_bronze_table_name")}"
SILVER_STREAMING_TABLE = f"{spark.conf.get("lab5.catalog")}.{spark.conf.get("lab5.silver_schema")}.{spark.conf.get("lab5.streaming_silver_table_name")}"

def silver_cleaning(df):
    df = df.filter(
        (F.trim(F.col("earthquake_id")) != "") | (F.col("earthquake_id").isNotNull())
    )

    df = df.withColumn(
        "place",
        F.when(
            F.col("place").isNull()
            | (F.trim(F.col("place")) == ""),
            "N/A"
        ).otherwise(F.trim(F.col("place")))
    )

    df = df.withColumn(
        "event_time",
        F.timestamp_millis(F.col("event_time"))
    )

    titude_cols = ["magnitude", "latitude", "longitude"]

    for c in titude_cols:
        df = df.withColumn(
            c,
            F.round(F.col(c), 2)
        )

    df = df.filter(
        (F.col("longitude") >= -180) &
        (F.col("longitude") <= 180) &
        (F.col("latitude") >= -90) &
        (F.col("latitude") <= 90)
    )

    return df
@dp.expect_all_or_drop({
    "valid_earthquake_id" : 
        "earthquake_id IS NOT NULL AND TRIM(earthquake_id) != ''",
    "valid_event_time" : 
        "event_time IS NOT NULL",
    "valid_longitude" : 
        "longitude >= -180 AND longitude <= 180",
    "valid_latitude":
        "latitude >= -90 AND latitude <= 90",
    "valid_magnitude":
        "magnitude IS NOT NULL AND magnitude >= -2.0"
})
@dp.materialized_view(
    name=SILVER_STREAMING_TABLE,
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true"
    }
)
def earthquakes_silver():
    df = spark.read.table(BRONZE_STREAMING_TABLE)

    df = silver_cleaning(df)

    return df