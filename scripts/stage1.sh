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

# Run data collection script
echo "Collecting data..."
bash scripts/data_collection.sh

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
  --as-parquetfile \
  --warehouse-dir=project/warehouse \
  --m 3

echo "Data import completed successfully!"