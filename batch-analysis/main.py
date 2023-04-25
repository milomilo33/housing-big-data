#!/usr/bin/python

from os import environ
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark = SparkSession \
    .builder \
    .appName("Batch analysis app") \
    .getOrCreate()

HDFS_NAMENODE_PATH = environ.get("CORE_CONF_fs_defaultFS", "hdfs://namenode:9000")
DATASET_PATH = "/home/housing-big-data/raw-zone/primary-dataset/uk-housing-official-1995-to-2023.csv"

schema = StructType([
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
    # schema=['transaction_id', 'price', 'date', 'postcode', 'property_type', 'new_property', 'duration', \
    #         'PAON', 'SAON', 'street', 'locality', 'town', 'district', 'county', 'ppd', 'record_status']
    schema=schema
)

# group by city, calculate average price, and order by descending price
top_cities = df.groupBy("town") \
                .agg(avg("price") \
                .alias("avg_price")) \
                .orderBy(desc("avg_price"))

# show top 10 cities by average price
top_cities.show(10)