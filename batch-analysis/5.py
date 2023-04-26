#!/usr/bin/python

# 5. Month with the highest number of transactions in specified year for each county, with average price

from os import environ
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import sys

spark = SparkSession \
    .builder \
    .appName("Batch analysis app -- 5. Month with the highest number of transactions in specified year for each county, with average price") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

HDFS_NAMENODE_PATH = environ.get("CORE_CONF_fs_defaultFS", "hdfs://namenode:9000")
DATASET_PATH = "/home/housing-big-data/raw-zone/primary-dataset/uk-housing-official-1995-to-2023.csv"
MONGO_URI = environ.get("MONGO_URI", "mongodb://root:example@mongodb-housing:27017/")
MONGO_DB = "housing_data"
MONGO_COLLECTION = "month_with_most_transactions_with_avg_price"

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

df = df.filter(year('date') == specified_year)
df = df.withColumn('month', month('date'))

county_month_window = Window.partitionBy('county', 'month')

df = df.withColumn('num_transactions', count('transaction_id').over(county_month_window))
df = df.withColumn('avg_price', avg('price').over(county_month_window))

county_window = Window.partitionBy("county").orderBy(desc("num_transactions"), "month")

df = df.withColumn('rank', row_number().over(county_window))

# filter only the rows with rank 1, i.e. the month with the highest number of transactions for each county
df = df.filter(col('rank') == 1)

df = df.withColumn("month", df["month"].cast(StringType()))
df = df.withColumn("month_name", date_format(to_date("month", "MM"), "MMMM"))
df = df.select('county', 'month', 'month_name', 'num_transactions', 'avg_price')

# show the results
df.show()

# save to MongoDB
df.write.format("com.mongodb.spark.sql.DefaultSource") \
                .option("uri", f"{MONGO_URI}{MONGO_DB}.{MONGO_COLLECTION}") \
                .mode("overwrite").save()
