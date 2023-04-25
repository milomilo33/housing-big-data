#!/bin/bash

echo "Creating directories for raw, transformation and curated zones on HDFS"
hdfs dfs -mkdir -p "/home/housing-big-data/raw-zone"
hdfs dfs -mkdir -p "/home/housing-big-data/transformation-zone"
hdfs dfs -mkdir -p "/home/housing-big-data/curated-zone"

primary_dataset_path_hdfs="/home/housing-big-data/raw-zone/primary-dataset"
echo "Creating directory for primary dataset at $primary_dataset_path_hdfs on HDFS"
hdfs dfs -mkdir -p $primary_dataset_path_hdfs

primary_dataset_path_namenode="/home/housing-big-data/primary-dataset/uk-housing-official-1995-to-2023.csv"
echo "Copying primary dataset from namenode's fs to HDFS"
hdfs dfs -copyFromLocal $primary_dataset_path_namenode $primary_dataset_path_hdfs