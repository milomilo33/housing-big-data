from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from os import environ

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

spark = SparkSession \
    .builder \
    .appName("1. Hello world") \
    .getOrCreate()

quiet_logs(spark)

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "crime-data"

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

crimes = crimes_stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
                        .select(from_csv(col("value"), schema_str).alias("data")) \
                        .select("data.*") \
                        .drop('key', 'falls_within')
# crimes = crimes_stream.selectExpr("CAST(value AS STRING)")

query = crimes \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option('truncate', 'false') \
    .start()

# query = wordCounts \
#     .writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .trigger(processingTime='2 seconds') \
#     .start()

query.awaitTermination()