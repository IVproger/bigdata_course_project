#!/bin/bash

# Exit on error, undefined variables, and propagate pipe failures
set -euo pipefail

# --- Environment Setup ---
export YARN_CONF_DIR=/etc/hadoop/conf
echo "Set YARN_CONF_DIR to $YARN_CONF_DIR"
export PYSPARK_PYTHON=python3.11
export PYSPARK_DRIVER_PYTHON=python3.11

# --- Load Hive Password ---
echo "Loading Hive credentials..."
hive_password=$(head -n 1 secrets/.hive.pass)
if [ -z "$hive_password" ]; then
  echo "Error: Failed to load Hive password from secrets/.hive.pass" >&2
  exit 1
fi

# --- Local Directory Setup ---
# Ensure output directory exists locally
mkdir -p output

# --- HDFS Cleanup (for KL divergence output) ---
echo "Cleaning HDFS KL divergence output directory..."
hdfs dfs -rm -r -f project/output/kl_divergence.csv >/dev/null 2>&1 || true

# --- Stage 4: Part 1 - Calculate KL Divergence ---
echo "Running KL Divergence calculation script..."

spark-submit \
    --master yarn \
    --deploy-mode client \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=$PYSPARK_PYTHON \
    --conf spark.executorEnv.PYSPARK_PYTHON=$PYSPARK_PYTHON \
    scripts/calculate_kl.py

# --- Stage 4: Part 2 - Create Hive Tables ---
echo "Creating Hive tables for Stage 3 results and KL divergence..."

# Ensure the Hive script exists
if [ ! -f sql/stage4_hive_tables.hql ]; then
    echo "ERROR: Hive script sql/stage4_hive_tables.hql not found!"
    exit 1
fi

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001/team14_projectdb \
    -n team14 \
    -p "$hive_password" \
    -f sql/stage4_hive_tables.hql > output/stage4_hive_results.txt 2>&1

# Check beeline exit code
if [ $? -ne 0 ]; then
    echo "ERROR: Hive script execution failed. Check output/stage4_hive_results.txt for details."
    exit 1
else
    echo "Hive tables created successfully. See output/stage4_hive_results.txt"
fi

# --- Download KL Divergence Results from HDFS ---
echo "Downloading KL divergence results..."
# Clean local file before downloading
rm -f output/kl_divergence.csv
hdfs dfs -getmerge project/output/kl_divergence.csv/*.csv output/kl_divergence.csv

# --- Pylint Check ---
echo "Running pylint on Stage 4 Python scripts..."
pylint --rcfile=.pylintrc scripts/calculate_kl.py || echo "Pylint found issues in calculate_kl.py (non-blocking)"

echo "Stage 4 automated tasks completed successfully!"
echo "Next steps: Use Apache Superset to connect to Hive tables (evaluation_results, model1_predictions, model2_predictions, kl_divergence) and build the dashboard."
