from pyspark.sql import SparkSession
from pyspark.sql.functions import *
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

messages = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", KAFKA_BROKER) \
  .option("subscribe", KAFKA_TOPIC) \
  .load()

msg = messages.selectExpr("CAST(value AS STRING)")

query = msg \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# query = wordCounts \
#     .writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .trigger(processingTime='2 seconds') \
#     .start()

query.awaitTermination()