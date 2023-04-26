#!/usr/bin/python3

import os
import time
from kafka import KafkaProducer
import kafka.errors
from os import environ
import csv

KAFKA_BROKER = environ.get("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = environ.get("KAFKA_TOPIC", "crime-data")

while True:
    try:
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER.split(","))
        print("Connected to Kafka!")
        break
    except kafka.errors.NoBrokersAvailable as e:
        print(e)
        time.sleep(3)

with open('/dataset/crime-data.csv', 'r') as csv_file:
    csv_reader = csv.reader(csv_file)

    for row in csv_reader:
        print(f'Sending a row to kafka topic {KAFKA_TOPIC}')

        message = ','.join(row).encode('utf-8')
        producer.send(KAFKA_TOPIC, message)

        time.sleep(2)
