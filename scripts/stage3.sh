#!/bin/bash
set -e  # Stop script on any error

# 1. Create local data directory (if not exists)
mkdir -p data

export YARN_CONF_DIR=/etc/hadoop/conf
echo "Set YARN_CONF_DIR to $YARN_CONF_DIR"

export PYSPARK_PYTHON=python3.11
export PYSPARK_DRIVER_PYTHON=python3.11


# 2. Clean HDFS directory
echo "Cleaning HDFS /project/data..."
hdfs dfs -rm -r project/data >/dev/null 2>&1 || true  # Ignore errors if directory doesn't exist

# 3. Recreate HDFS directory structure
echo "Recreating HDFS structure..."
hdfs dfs -mkdir -p project/data/train
hdfs dfs -mkdir -p project/data/test

echo "Data preparation..."
spark-submit \
    --master yarn \
    --deploy-mode client \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=python3.11 \
    --conf spark.executorEnv.PYSPARK_PYTHON=python3.11 \
    scripts/data_predprocessing.py

# 4. Copy files from HDFS to local (with merging)
echo "Downloading training data..."
hdfs dfs -getmerge project/data/train/*.json data/train.json

echo "Downloading test data..."
hdfs dfs -getmerge project/data/test/*.json data/test.json

echo "Operation completed successfully!"