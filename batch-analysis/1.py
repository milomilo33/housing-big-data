#!/usr/bin/python

# 1. Top 10 towns by average price

from os import environ
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pymongo import MongoClient
import json

spark = SparkSession \
    .builder \
    .appName("Batch analysis app -- 1. Top 10 towns by average price") \
    .getOrCreate()
    # .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:10.1.1') \

HDFS_NAMENODE_PATH = environ.get("CORE_CONF_fs_defaultFS", "hdfs://namenode:9000")
DATASET_PATH = "/home/housing-big-data/raw-zone/primary-dataset/uk-housing-official-1995-to-2023.csv"
MONGO_URI = environ.get("MONGO_URI", "mongodb://root:example@mongodb-housing:27017/")
MONGO_DB = "housing_data"
MONGO_COLLECTION = "top_towns_by_price"

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

# group by town, calculate average price, and order by descending price
top_towns = df.groupBy("town") \
                .agg(avg("price") \
                .alias("avg_price")) \
                .orderBy(desc("avg_price"))

# show top 10 towns by average price
top_towns.show(10)


# # save to MongoDB
# mongo_db = "housing_data"
# mongo_collection = "top_towns_by_price"
# mongo_client = MongoClient(MONGO_URI)
# mongo_db = mongo_client[mongo_db]
# mongo_db[mongo_collection].drop()
# top_towns_json = json.loads(top_towns.toJSON().collect())
# mongo_db[mongo_collection].insert_many(top_towns_json)

top_towns.limit(10).write.format("com.mongodb.spark.sql.DefaultSource") \
                .option("uri", f"{MONGO_URI}{MONGO_DB}.{MONGO_COLLECTION}") \
                .mode("overwrite").save()
