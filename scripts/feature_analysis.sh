#!/bin/bash

# Set environment variables for PySpark
export YARN_CONF_DIR=/etc/hadoop/conf
export PYSPARK_PYTHON=python3.11
export PYSPARK_DRIVER_PYTHON=python3.11


source .venv/bin/activate

# Define HDFS output directory
HDFS_OUTPUT_DIR="project/data_insights"

# Remove the existing HDFS directory if it exists
echo "Attempting to remove existing HDFS directory: $HDFS_OUTPUT_DIR"
hdfs dfs -rm -r -f "$HDFS_OUTPUT_DIR"
if [ $? -eq 0 ]; then
    echo "Successfully removed existing HDFS directory."
else
    echo "Warning: Failed to remove HDFS directory (it might not exist)."
fi

# Create the HDFS directory
echo "Creating HDFS directory: $HDFS_OUTPUT_DIR"
hdfs dfs -mkdir -p "$HDFS_OUTPUT_DIR"
if [ $? -ne 0 ]; then
    echo "Error: Failed to create HDFS directory $HDFS_OUTPUT_DIR. Exiting."
    exit 1
fi
echo "Successfully created HDFS directory."

# Submit the Spark job
echo "Submitting Spark job: scripts/feature_analysis.py"
spark-submit \
    --master yarn \
    --deploy-mode client \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=$PYSPARK_PYTHON \
    --conf spark.executorEnv.PYSPARK_PYTHON=$PYSPARK_PYTHON \
    --jars /shared/postgresql-42.6.1.jar \
    scripts/general_feature_analysis.py

# Check the exit status of spark-submit
if [ $? -eq 0 ]; then
    echo "Spark job completed successfully."
else
    echo "Error: Spark job failed."
    exit 1
fi

echo "Script finished."