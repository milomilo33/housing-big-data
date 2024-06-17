# Batch and Real-Time Analysis of Housing Prices and Crime Data
This project aims to perform batch analysis of a large dataset consisting of historical housing price data, and real-time analysis of streamed crime data in the UK (as well as a combination of the two). This is achieved by simulating a big data architecture on a single machine by means of containerization.

**Apache Spark** is used for data analysis. **Apache Kafka** is used for streaming real-time data.

Housing prices dataset can be downloaded [here](https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads). It should be located in _/primary-dataset_.

Crime dataset can be downloaded [here](https://data.police.uk/data/archive/2013-12.zip).

## Setup
To run this project and run an example batch analysis job:

```
$ docker compose up -d

$ docker exec -it namenode /home/housing-big-data/primary-dataset/initialize.sh

$ docker exec -it spark-master /bin/bash

$ ./spark/bin/spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1  /home/housing-big-data/batch-analysis/${JOB_NAME}.py
```

For data visualization, open _localhost:3000_.

For real-time analysis, extract the downloaded crime dataset in _/util_ and run _python combine_shuffle_csv.py_ in _/util_. Then:

```
$ ./spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 /home/housing-big-data/real-time-analysis/${JOB_NAME}.py
```
