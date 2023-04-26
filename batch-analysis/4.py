#!/usr/bin/python

# 4. Average price change per year for each county for the given year range

from os import environ
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import sys

spark = SparkSession \
    .builder \
    .appName("Batch analysis app -- 4. Average price change per year for each county for the given year range") \
    .getOrCreate()

HDFS_NAMENODE_PATH = environ.get("CORE_CONF_fs_defaultFS", "hdfs://namenode:9000")
DATASET_PATH = "/home/housing-big-data/raw-zone/primary-dataset/uk-housing-official-1995-to-2023.csv"
MONGO_URI = environ.get("MONGO_URI", "mongodb://root:example@mongodb-housing:27017/")
MONGO_DB = "housing_data"
MONGO_COLLECTION = "average_price_change_per_year"

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
    start_year = int(sys.argv[1])
else:
    start_year = 2011

if len(sys.argv) > 2:
    end_year = int(sys.argv[2])
else:
    end_year = 2020

df = df.withColumn('year', year('date'))
df = df.filter((col('year') >= start_year) & (col('year') <= end_year))

county_year_window = Window.partitionBy('county', 'year')
year_window = Window.partitionBy('county').orderBy(asc('year'))

df = df.withColumn('avg_price', avg('price').over(county_year_window))
df = df.withColumn('previous_year', col('year') - 1)
df = df.dropDuplicates(['county', 'year'])
df = df.withColumn('prev_year_avg_price', lag('avg_price'))
df = df.withColumn('price_change', col('avg_price') - col('prev_year_avg_price'))
df = df.withColumn('percentage_change', round(((col('avg_price') - col('prev_year_avg_price')) / col('prev_year_avg_price')) * 100, 2)) \
        .orderBy(asc('county'), asc('year'))

df.show()

# # Use the lag function to get the previous year's average price for each county
# df = df.withColumn('prev_avg_price', lag(avg('price').over(county_window)).over(county_window))

# # Calculate the price change from the previous year
# df_with_change = df_with_lag.withColumn('price_change', round(avg('price') - col('prev_avg_price'), 2))

# # Group by county and year to get the average price change per year for each county
# result = df_filtered.groupBy('county', 'year').agg(avg('price_change'))

# save to MongoDB
df.write.format("com.mongodb.spark.sql.DefaultSource") \
                .option("uri", f"{MONGO_URI}{MONGO_DB}.{MONGO_COLLECTION}") \
                .mode("overwrite").save()
