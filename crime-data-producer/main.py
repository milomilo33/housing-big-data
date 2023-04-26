#!/usr/bin/python3

import os
import time
from kafka import KafkaProducer
import kafka.errors
from os import environ

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

while True:
    print(f'sending msg to kafka topic {KAFKA_TOPIC}')
    producer.send(KAFKA_TOPIC, key=bytes('key', 'utf-8'), value=bytes('hello world', 'utf-8'))