#!/bin/bash

echo "Running Stage 3: Predictive Data Analytics..."

# Ensure script stops if any command fails
set -e

# Define Team Number and Script Path
TEAM_NAME="team14"
PYTHON_SCRIPT="scripts/stage3.py"

# Set YARN configuration directory if needed (usually set in environment)
# export YARN_CONF_DIR=/etc/hadoop/conf

echo "Submitting Spark job: ${PYTHON_SCRIPT}..."

# Submit the Spark ML script to YARN
spark-submit \
    --master yarn \
    --deploy-mode client \
    --name "${TEAM_NAME}_Stage3_ML" \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/bin/python \
    --conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python \
    --archives .venv.tar.gz#environment \
    "${PYTHON_SCRIPT}"

echo "Spark job completed."

# Run pylint check on the Python script
echo "Running pylint check on ${PYTHON_SCRIPT}..."
pylint --fail-under=8.0 "${PYTHON_SCRIPT}"

echo "Stage 3 completed successfully."