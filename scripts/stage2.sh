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
echo "Executing Hive queries, storing results in PostgreSQL, and exporting to CSV..."

# Note: Adjust master, driver memory, executor memory/cores as needed for your cluster
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --jars /shared/postgresql-42.6.1.jar \
    scripts/run_hive_queries.py

echo "EDA analysis script finished."

# Run pylint validation
echo "Running pylint validation..."
if ! command -v pylint &> /dev/null; then
    echo "Installing pylint..."
    pip install pylint
fi

# Run pylint with specific configuration
echo "Running pylint on Python scripts..."
pylint --rcfile=.pylintrc scripts/run_hive_queries.py || {
    echo "Warning: pylint found some issues. Please review the output above."
    # Continue execution even if pylint finds issues
}

echo "Stage 2 completed successfully!"
echo "Note: Datasets and charts need to be created manually in Apache Superset using the PostgreSQL tables (q1_results, q2_results, etc.)."
