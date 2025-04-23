#!/bin/bash
set -e  # Stop script on any error

# 1. Create local data directory (if not exists)
mkdir -p data

# 2. Clean HDFS directory
echo "Cleaning HDFS /project/data..."
hdfs dfs -rm -r project/data >/dev/null 2>&1 || true  # Ignore errors if directory doesn't exist

# 3. Recreate HDFS directory structure
echo "Recreating HDFS structure..."
hdfs dfs -mkdir -p project/data/train
hdfs dfs -mkdir -p project/data/test

# 4. Data preparation...
# echo "Data preparation..."
# spark-submit \
#     --master yarn \
#     --deploy-mode client \
#     scripts/run_hive_queries.py


# # 4. Copy files from HDFS to local (with merging)
# echo "Downloading training data..."
# hdfs dfs -getmerge project/data/train/*.json data/train.json

# echo "Downloading test data..."
# hdfs dfs -getmerge project/data/test/*.json data/test.json

# echo "Operation completed successfully!"