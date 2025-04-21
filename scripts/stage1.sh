#!/bin/bash
# Data import script to collect data and load it into HDFS
# Exit on error, undefined variables, and propagate pipe failures
set -euo pipefail

echo "Starting data import process..."

# Load PostgreSQL password from secrets file
echo "Loading credentials..."
password=$(head -n 1 secrets/.psql.pass)
if [ -z "$password" ]; then
  echo "Error: Failed to load database password" >&2
  exit 1
fi

source .venv/bin/activate

# Run data collection script
echo "Collecting data..."
bash scripts/data_collection.sh

# Clear project database
echo "Clear project database..."
python3.11 scripts/clear_postgres_db.py 

# Build project database
echo "Building project database..."
python3.11 scripts/build_projectdb.py

# First clean existing output directory (optional but safer approach)
echo "Cleaning existing output directory..."
hdfs dfs -rm -r -f project/*

# Import all tables from PostgreSQL to HDFS as Parquet files
echo "Importing data to HDFS..."
sqoop import-all-tables \
  --connect jdbc:postgresql://hadoop-04.uni.innopolis.ru/team14_projectdb \
  --username team14 \
  --password "$password" \
  --compression-codec=snappy \
  --compress \
  --as-avrodatafile \
  --warehouse-dir=project/warehouse \
  --m 3 

hdfs dfs -rm -r -f project/warehouse/avsc
hdfs dfs -mkdir -p project/warehouse/avsc

mv *.avsc output/
mv *.java output/

hdfs dfs -put output/*.avsc project/warehouse/avsc

echo "Data import completed successfully!"