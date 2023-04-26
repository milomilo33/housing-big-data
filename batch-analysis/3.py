#!/usr/bin/python

# 3. Price difference between least and most expensive property of given type in each county

from os import environ
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

spark = SparkSession \
    .builder \
    .appName("Batch analysis app -- 3. Price difference between least and most expensive property of given type in each county") \
    .getOrCreate()

HDFS_NAMENODE_PATH = environ.get("CORE_CONF_fs_defaultFS", "hdfs://namenode:9000")
DATASET_PATH = "/home/housing-big-data/raw-zone/primary-dataset/uk-housing-official-1995-to-2023.csv"
MONGO_URI = environ.get("MONGO_URI", "mongodb://root:example@mongodb-housing:27017/")
MONGO_DB = "housing_data"
MONGO_COLLECTION = "price_difference_by_county"

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
    schema=schema
)

price_diff_df = df.groupBy("county", "property_type") \
                 .agg((col("price").max() - col("price").min()).alias("price_difference"))

price_diff_df.show()

# save to MongoDB
price_diff_df.write.format("com.mongodb.spark.sql.DefaultSource") \
                .option("uri", f"{MONGO_URI}{MONGO_DB}.{MONGO_COLLECTION}") \
                .mode("overwrite").save()
