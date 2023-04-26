#!/usr/bin/python

# 2. Number of properties sold each month in specified year

from os import environ
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

spark = SparkSession \
    .builder \
    .appName("Batch analysis app -- 2. Number of properties sold each month in specified year") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

HDFS_NAMENODE_PATH = environ.get("CORE_CONF_fs_defaultFS", "hdfs://namenode:9000")
DATASET_PATH = "/home/housing-big-data/raw-zone/primary-dataset/uk-housing-official-1995-to-2023.csv"
MONGO_URI = environ.get("MONGO_URI", "mongodb://root:example@mongodb-housing:27017/")
MONGO_DB = "housing_data"
MONGO_COLLECTION = "num_properties_by_month"

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

if len(sys.argv) > 1:
    specified_year = int(sys.argv[1])
else:
    specified_year = 2011

sales_by_month = df.filter(year("date") == specified_year) \
                    .groupBy(month("date").alias("month")) \
                    .count()

sales_by_month.show()

sales_by_month = sales_by_month.withColumn("month", sales_by_month["month"].cast(StringType()))
sales_by_month = sales_by_month.withColumn("month_name", date_format(to_date("month", "MM"), "MMMM"))

sales_by_month.show()

# save to MongoDB
sales_by_month.write.format("com.mongodb.spark.sql.DefaultSource") \
                .option("uri", f"{MONGO_URI}{MONGO_DB}.{MONGO_COLLECTION}") \
                .mode("overwrite").save()
