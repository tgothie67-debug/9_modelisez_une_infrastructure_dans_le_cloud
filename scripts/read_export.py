from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("Read Export")
    .getOrCreate()
)

df = spark.read.parquet(
    "/opt/spark/work-dir/exports/tickets_by_type"
)

df.show()

spark.stop()