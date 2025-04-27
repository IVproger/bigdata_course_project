#!/bin/bash

# --- Environment Setup ---
export YARN_CONF_DIR=/etc/hadoop/conf
echo "Set YARN_CONF_DIR to $YARN_CONF_DIR"
export PYSPARK_PYTHON=python3.11
export PYSPARK_DRIVER_PYTHON=python3.11

# --- Local Directory Setup ---
mkdir -p data
mkdir -p models
mkdir -p output

# --- HDFS Cleanup ---
echo "Cleaning HDFS data, models, and output directories..."
hdfs dfs -rm -r -f project/data >/dev/null 2>&1 || true
hdfs dfs -rm -r -f project/models >/dev/null 2>&1 || true
hdfs dfs -rm -r -f project/output >/dev/null 2>&1 || true

# --- HDFS Directory Creation ---
echo "Recreating HDFS structure..."
hdfs dfs -mkdir -p project/data/train
hdfs dfs -mkdir -p project/data/test
hdfs dfs -mkdir -p project/models
hdfs dfs -mkdir -p project/output

# --- Stage 3: Part 1 - Data Preprocessing ---
echo "Running data preprocessing script..."
# Add check to see if the script file exists before submitting
# echo "Checking if preprocessing script exists:"
# ls -l scripts/data_predprocessing.py

spark-submit \
    --master yarn \
    --deploy-mode client \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=$PYSPARK_PYTHON \
    --conf spark.executorEnv.PYSPARK_PYTHON=$PYSPARK_PYTHON \
    scripts/data_predprocessing.py

# --- Stage 3: Part 2 - ML Modeling ---
echo "Running ML modeling script..."
# Add check to see if the modeling script exists before submitting
# echo "Checking if modeling script exists:"
# ls -l scripts/ml_modeling.py

spark-submit \
    --master yarn \
    --deploy-mode client \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=$PYSPARK_PYTHON \
    --conf spark.executorEnv.PYSPARK_PYTHON=$PYSPARK_PYTHON \
    scripts/ml_modeling.py

# --- Download Results from HDFS ---
echo "Downloading processed data..."
hdfs dfs -getmerge project/data/train/*.json data/train.json
hdfs dfs -getmerge project/data/test/*.json data/test.json

echo "Downloading trained models..."
# Clean local model directories before downloading
rm -rf models/model1 models/model2
hdfs dfs -get project/models/model1 models/model1
hdfs dfs -get project/models/model2 models/model2

echo "Downloading model predictions..."
hdfs dfs -getmerge project/output/model1_predictions.csv/*.csv output/model1_predictions.csv
hdfs dfs -getmerge project/output/model2_predictions.csv/*.csv output/model2_predictions.csv

echo "Downloading model evaluation comparison..."
hdfs dfs -getmerge project/output/evaluation.csv/*.csv output/evaluation.csv

# --- Pylint Check ---
echo "Running pylint on Stage 3 Python scripts..."
pylint --rcfile=.pylintrc scripts/data_predprocessing.py || echo "Pylint found issues in data_predprocessing.py (non-blocking)"
pylint --rcfile=.pylintrc scripts/ml_modeling.py || echo "Pylint found issues in ml_modeling.py (non-blocking)"

echo "Stage 3 completed successfully!"
