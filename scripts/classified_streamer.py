from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("Classified Streamer")
    .master("local[*]")
    .getOrCreate()
)

df = (
    spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "redpanda-0:9092")
    .option("subscribe", "classifieds")
    .load()
)

print("Connexion au topic configurée")
print(df.isStreaming)