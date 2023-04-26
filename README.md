# housing-big-data

docker compose up -d

docker exec -it namenode /home/housing-big-data/primary-dataset/initialize.sh

docker exec -it spark-master /bin/bash
->
./spark/bin/spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1  /home/housing-big-data/batch-analysis/${JOB_NAME}.py

localhost:3000 for data visualization; might have to manually sync

download real-time dataset and extract to /util and run python combine_shuffle_csv.py in /util

./spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 /home/housing-big-data/real-time-analysis/${JOB_NAME}.py