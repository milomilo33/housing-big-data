# housing-big-data

docker compose up -d

docker exec -it namenode /home/housing-big-data/primary-dataset/initialize.sh

docker exec -it spark-master /bin/bash
->
./spark/bin/spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1  /home/housing-big-data/batch-analysis/${JOB_NAME}.py