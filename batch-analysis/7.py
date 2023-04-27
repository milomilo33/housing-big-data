#!/usr/bin/python

# 7. Percentage of property types by county

from os import environ
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import sys

spark = SparkSession \
    .builder \
    .appName("Batch analysis app -- 7. Percentage of property types by county") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

HDFS_NAMENODE_PATH = environ.get("CORE_CONF_fs_defaultFS", "hdfs://namenode:9000")
DATASET_PATH = "/home/housing-big-data/raw-zone/primary-dataset/uk-housing-official-1995-to-2023.csv"
MONGO_URI = environ.get("MONGO_URI", "mongodb://root:example@mongodb-housing:27017/")
MONGO_DB = "housing_data"
MONGO_COLLECTION = "percentage_of_property_types"

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

county_type_window = Window.partitionBy('county', 'property_type')
county_window = Window.partitionBy('county')

df = df.withColumn('num_of_type', count('*').over(county_type_window))
df = df.withColumn('total_num', count('*').over(county_window))
df = df.withColumn('row_num', row_number().over(county_type_window.orderBy('price')))

df = df.filter(col('row_num') == 1)

df = df.withColumn('percentage_of_type', round(col('num_of_type') / col('total_num') * 100, 2))

df = df.select('county', 'property_type', 'num_of_type', 'total_num', 'percentage_of_type')

# show the results
df.show()

# save to MongoDB
df.write.format("com.mongodb.spark.sql.DefaultSource") \
                .option("uri", f"{MONGO_URI}{MONGO_DB}.{MONGO_COLLECTION}") \
                .mode("overwrite").save()
