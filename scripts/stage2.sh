#!/bin/bash
# Stage 2 - Data Storage/Preparation & EDA
# Exit on error, undefined variables, and propagate pipe failures
set -euo pipefail

echo "Starting Stage 2: Data Storage/Preparation & EDA"

# Load Hive password from secrets file
echo "Loading Hive password from secrets file..."
HIVE_PASS=$(head -n 1 secrets/.hive.pass)
if [ -z "$HIVE_PASS" ]; then
    echo "Error: Hive password is empty"
    exit 1
fi

# Create output directory if it doesn't exist
mkdir -p output

# Clean and create HDFS warehouse directory
echo "Cleaning and creating HDFS warehouse directory..."
hdfs dfs -rm -r -f project/hive/warehouse
hdfs dfs -mkdir -p project/hive/warehouse
echo "HDFS warehouse directory created successfully."


# Run the PySpark script to create Hive database and tables
echo "Creating Hive database and tables"

# Activate virtual environment if it exists
if [ -d ".venv" ]; then
    echo "Activating virtual environment..."
    source .venv/bin/activate
fi

# Run the Python script with proper Spark configuration
bash scripts/create_hive_tables.sh

echo "Perfome the EDA analysis using job_descriptions_part HIVE table"



echo "Stage 2 completed successfully!"
echo "Note: Datasets and charts will be created in Apache Superset manually."
