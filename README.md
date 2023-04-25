# housing-big-data

docker compose up -d

docker exec -it namenode /home/housing-big-data/primary-dataset/initialize.sh

docker exec -it spark-master /bin/bash
->
./spark/bin/spark-submit /home/housing-big-data/batch-analysis/main.py