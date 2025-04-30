#!/bin/bash
# Stage 2 - Data Storage/Preparation & EDA
# Exit on error, undefined variables, and propagate pipe failures
set -euo pipefail

echo "Starting Stage 2: Data Storage/Preparation & EDA"

# Create output directory if it doesn't exist
mkdir -p output

# Activate virtual environment if it exists
# This might be needed if run_hive_queries.py has specific dependencies
if [ -d ".venv" ]; then
    echo "Activating virtual environment..."
    source .venv/bin/activate
fi

# Set YARN configuration directory
export YARN_CONF_DIR=/etc/hadoop/conf
echo "Set YARN_CONF_DIR to $YARN_CONF_DIR"

# Create tables in Hive for EDA 
echo "Create tables in Hive for EDA"
bash scripts/create_hive_tables.sh

# Run the EDA PySpark script
echo "Performing EDA analysis using job_descriptions_part HIVE table..."
echo "Executing Hive queries, storing results in PostgreSQL, and exporting to CSV (via HDFS)..."

# Perform general EDA for stats collecting
bash scripts/feature_analysis.sh 

# Note: Adjust master, driver memory, executor memory/cores as needed for your cluster
# Extract specific data insights 
spark-submit \
    --master yarn \
    --deploy-mode client \
    --jars /shared/postgresql-42.6.1.jar \
    scripts/run_hive_queries.py

echo "EDA analysis script finished."

# Download results from HDFS to local CSV files
echo "Downloading query results from HDFS to local output directory..."
for i in {1..6}; do
    hdfs_tmp_path="output/q${i}.csv.tmp"
    local_csv="output/q${i}.csv"
    echo "Processing results for q${i}..."
    # Check if the HDFS temp directory exists
    if hdfs dfs -test -d "$hdfs_tmp_path"; then
        echo "Merging HDFS path $hdfs_tmp_path to local file $local_csv"
        # Remove existing local file if it exists
        rm -f "$local_csv"
        # Merge HDFS part-files (created by Spark) into a single local CSV file
        # This should preserve the header since Spark wrote it with .option("header", "true")
        hdfs dfs -getmerge "$hdfs_tmp_path" "$local_csv"
        echo "Removing temporary HDFS directory $hdfs_tmp_path"
        hdfs dfs -rm -r "$hdfs_tmp_path"
    else
        echo "Warning: HDFS temporary directory $hdfs_tmp_path not found. Skipping download for q${i}."
    fi
done
echo "Finished downloading results."

# Run pylint with specific configuration
echo "Running pylint on Python scripts..."
pylint --rcfile=.pylintrc scripts/run_hive_queries.py || {
    echo "Warning: pylint found some issues. Please review the output above."
    # Continue execution even if pylint finds issues
}

echo "Stage 2 completed successfully!"
echo "Note: Datasets and charts need to be created manually in Apache Superset using the PostgreSQL tables (q1_results, q2_results, etc.)."
