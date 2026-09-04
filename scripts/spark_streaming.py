import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)


BOOTSTRAP_SERVERS = os.getenv(
    "REDPANDA_BOOTSTRAP_SERVERS",
    "redpanda-0:9092",
)

EXPORT_PATH = os.getenv(
    "SPARK_EXPORT_PATH",
    "/opt/spark/exports/tickets_by_type",
)

CHECKPOINT_PATH = os.getenv(
    "SPARK_CHECKPOINT_PATH",
    "/opt/spark/checkpoints/tickets_by_type_export",
)

spark = (
    SparkSession.builder
    .appName("Client Tickets Streaming")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "3")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


ticket_schema = StructType([
    StructField("ticket_id", StringType()),
    StructField("client_id", IntegerType()),
    StructField("created_at", StringType()),
    StructField("request", StringType()),
    StructField("request_type", StringType()),
    StructField("priority", StringType()),
])


df_raw = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        BOOTSTRAP_SERVERS,
    )
    .option(
        "subscribe",
        "client_tickets",
    )
    .option(
        "startingOffsets",
        "earliest",
    )
    .load()
)


tickets = (
    df_raw
    .select(
        from_json(
            col("value").cast("string"),
            ticket_schema,
        ).alias("ticket")
    )
    .select("ticket.*")
)


tickets_enriched = tickets.withColumn(
    "support_team",
    when(
        col("request_type") == "facturation",
        "Equipe Facturation",
    )
    .when(
        col("request_type") == "incident_technique",
        "Equipe Technique",
    )
    .when(
        col("request_type") == "demande_information",
        "Service Client",
    )
    .when(
        col("request_type") == "modification_compte",
        "Gestion des Comptes",
    )
    .when(
        col("request_type") == "resiliation",
        "Equipe Retention",
    )
    .otherwise("Support General"),
)


tickets_by_type = (
    tickets_enriched
    .groupBy("request_type")
    .count()
)


def export_analysis(batch_df, batch_id):
    print(f"Export du batch {batch_id}")

    (
        batch_df.write
        .mode("overwrite")
        .parquet(EXPORT_PATH)
    )


query = (
    tickets_by_type.writeStream
    .outputMode("complete")
    .foreachBatch(export_analysis)
    .option(
        "checkpointLocation",
        CHECKPOINT_PATH,
    )
    .start()
)

query.awaitTermination()