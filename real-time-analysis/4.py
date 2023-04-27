# 4. Alarm when at least 3 occurrences of anti-social behaviour

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from os import environ
import os

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "crime-data"
OUTPUT_PATH = "/home/housing-big-data/transformation-zone/alarms"
HDFS_NAMENODE_PATH = environ.get("CORE_CONF_fs_defaultFS", "hdfs://namenode:9000")

def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

def write_batch_to_csv(df, epoch_id):
    file_path = os.path.join(HDFS_NAMENODE_PATH + OUTPUT_PATH, f"batch_{epoch_id}.csv")
    df.coalesce(1).write.mode("append").option("header", "true").csv(file_path)

spark = SparkSession \
    .builder \
    .appName("4. Alarm when at least 3 occurrences of anti-social behaviour") \
    .getOrCreate()

quiet_logs(spark)

schema = StructType([
    StructField("key", StringType(), True),
    StructField("month", StringType(), True),
    StructField("reported_by", StringType(), True),
    StructField("falls_within", StringType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("location", StringType(), True),
    StructField("lsoa_code", StringType(), True),
    StructField("lsoa_name", StringType(), True),
    StructField("crime_type", StringType(), True),
    StructField("last_outcome_category", StringType(), True),
    StructField("context", StringType(), True),
])
schema_str = ",".join([f"{field.name} {field.dataType.typeName()}" for field in schema.fields])

crimes_stream = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", KAFKA_BROKER) \
  .option("subscribe", KAFKA_TOPIC) \
  .load()

crimes = crimes_stream.selectExpr("timestamp", "CAST(key AS STRING)", "CAST(value AS STRING)") \
                        .select("timestamp", from_csv(col("value"), schema_str).alias("data")) \
                        .select("timestamp", "data.*") \
                        .drop('key', 'falls_within')

query = crimes \
    .filter(col("crime_type") == "Anti-social behaviour") \
    .withWatermark("timestamp", "1 seconds") \
    .groupBy(window("timestamp", "10 seconds", "3 seconds")) \
    .agg(count("crime_type").alias("crime_count")) \
    .orderBy(desc('window')) \
    
def alarm(row):
    if row["crime_count"] > 2:
        print("--- Anti-social behaviour occurrences over threshold (2)! ---")

query.writeStream \
    .outputMode("complete") \
    .foreach(alarm) \
    .start()

query.select('window.start', 'window.end', 'crime_count') \
    .writeStream \
    .foreachBatch(write_batch_to_csv) \
    .outputMode("complete") \
    .start()

spark.streams.awaitAnyTermination()