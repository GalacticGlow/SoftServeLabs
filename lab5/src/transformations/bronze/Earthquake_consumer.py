import pyspark.sql.functions as F
from pyspark import pipelines as dp
from pyspark.sql.types import *

BRONZE_STREAMING_TABLE = f"{spark.conf.get("lab5.catalog")}.{spark.conf.get("lab5.bronze_schema")}.{spark.conf.get("lab5.streaming_bronze_table_name")}"

secret_scope = spark.conf.get("lab5.eventhub_secret_scope")
consumer_connstr = spark.conf.get("lab5.eventhub_consumer_connstr_key")
eventhub_name = spark.conf.get("lab5.eventhub_name_key")
eventhub_namespace = spark.conf.get("lab5.eventhub_namespace_key")

EVENT_HUB_NAMESPACE = dbutils.secrets.get(
    scope=secret_scope,
    key=eventhub_namespace
)

EVENT_HUB_NAME = dbutils.secrets.get(
    scope=secret_scope,
    key=eventhub_name
)

EVENT_HUB_CONNECTION_STRING = dbutils.secrets.get(
    scope=secret_scope,
    key=consumer_connstr
)

schema = StructType([
    StructField("id", StringType()),
    StructField("properties", StructType([
        StructField("mag", DoubleType()),
        StructField("place", StringType()),
        StructField("time", LongType())
    ])),
    StructField("geometry", StructType([
        StructField("coordinates", ArrayType(DoubleType()))
    ]))
])

@dp.table(name=BRONZE_STREAMING_TABLE)
def earthquake_consumer():
    kafka_options = {
        "kafka.bootstrap.servers":
            f"{EVENT_HUB_NAMESPACE}.servicebus.windows.net:9093",
        "subscribe":
            EVENT_HUB_NAME,
        "kafka.security.protocol":
            "SASL_SSL",
        "kafka.sasl.mechanism":
            "PLAIN",
        "kafka.sasl.jaas.config":
            f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule '
            f'required username="$ConnectionString" '
            f'password="{EVENT_HUB_CONNECTION_STRING}";',
        "startingOffsets": "latest"
    }

    df = (
        spark.readStream
        .format("kafka")
        .options(**kafka_options)
        .load()
    )

    parsed_df = (
        df.select(
            F.from_json(
                F.col("value").cast("string"),
                schema
            ).alias("data")
        )
    )

    earthquakes_df = parsed_df.select(
        F.col("data.id").alias("earthquake_id"),
        F.col("data.properties.mag").alias("magnitude"),
        F.col("data.properties.place").alias("place"),
        F.col("data.properties.time").alias("event_time"),
        F.col("data.geometry.coordinates")[0].alias("longitude"),
        F.col("data.geometry.coordinates")[1].alias("latitude")
    )

    return earthquakes_df