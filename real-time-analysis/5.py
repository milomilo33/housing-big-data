# 5. Show average price of house and number of crimes by postcode in timeframe (join with primary dataset)

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from os import environ
import os
import requests
import xml.etree.ElementTree as ET
import uuid

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "crime-data"
OUTPUT_PATH = "/home/housing-big-data/transformation-zone/postcode-join"
HDFS_NAMENODE_PATH = environ.get("CORE_CONF_fs_defaultFS", "hdfs://namenode:9000")
DATASET_PATH = "/home/housing-big-data/raw-zone/primary-dataset/uk-housing-official-1995-to-2023.csv"

def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

def write_batch_to_csv(df, epoch_id):
    file_path = os.path.join(HDFS_NAMENODE_PATH + OUTPUT_PATH, f"batch_{epoch_id}.csv")
    df.coalesce(1).write.mode("append").option("header", "true").csv(file_path)

spark = SparkSession \
    .builder \
    .appName("5. Show average price of house and number of crimes by postcode in timeframe (join with primary dataset)") \
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

primary_dataset_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("date", TimestampType(), True),
    StructField("postcode", StringType(), True),
    StructField("property_type", StringType(), True),
    StructField("new_property", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("PAON", StringType(), True),
    StructField("SAON", StringType(), True),
    StructField("street", StringType(), True),
    StructField("locality", StringType(), True),
    StructField("town", StringType(), True),
    StructField("district", StringType(), True),
    StructField("county", StringType(), True),
    StructField("ppd", StringType(), True),
    StructField("record_status", StringType(), True)
])

df = spark.read.csv(
    path=HDFS_NAMENODE_PATH + DATASET_PATH,
    schema=primary_dataset_schema
).limit(1000)

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

def get_postcode(lat, lon):
    url = f"https://nominatim.openstreetmap.org/reverse?lat={lat}&lon={lon}&format=xml"
    response = requests.get(url)
    if response.status_code == 200:
        root = ET.fromstring(response.content)
        for address in root.iter('addressparts'):
            postcode = address.find('postcode').text
            return postcode
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return None
    
get_postcode_udf = udf(get_postcode)
uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())

crimes = crimes.withColumn("postcode", get_postcode_udf(crimes["latitude"], crimes["longitude"]))
crimes = crimes.withColumn("crime_id", uuid_udf())
joined_stream = crimes.join(df, "postcode")

query = joined_stream \
    .withWatermark("timestamp", "1 seconds") \
    .groupBy(window("timestamp", "45 seconds", "15 seconds"), "postcode") \
    .agg(avg("price").alias("average_price"), approx_count_distinct("crime_id").alias("crime_count")) \
    .orderBy(desc('window'), asc("postcode"))

query.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option('truncate', 'false') \
    .start()

query.select('window.start', 'window.end', 'postcode', 'average_price', 'crime_count') \
    .writeStream \
    .foreachBatch(write_batch_to_csv) \
    .outputMode("complete") \
    .start()

spark.streams.awaitAnyTermination()