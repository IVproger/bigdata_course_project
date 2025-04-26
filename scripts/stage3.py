#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Stage 3: Predictive Data Analytics for Job Descriptions Salary Prediction.

This script loads preprocessed data, trains two regression models
(Linear Regression and Random Forest), tunes their hyperparameters using
Cross-Validation, evaluates the best models, compares them, and saves
the models and results.
"""

import os
import time
import numpy as np
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.ml import Pipeline
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

# --- Configuration ---
TEAM_NAME = "team14"
WAREHOUSE_LOCATION = "project/hive/warehouse"
HDFS_TRAIN_DATA_PATH = "project/data/train"
HDFS_TEST_DATA_PATH = "project/data/test"
HDFS_MODEL1_PATH = "project/models/model1"  # Linear Regression
HDFS_MODEL2_PATH = "project/models/model2"  # Random Forest
HDFS_PRED1_PATH = "project/output/model1_predictions.csv"
HDFS_PRED2_PATH = "project/output/model2_predictions.csv"
HDFS_EVAL_PATH = "project/output/evaluation.csv"

LOCAL_MODEL1_DIR = "models/model1"
LOCAL_MODEL2_DIR = "models/model2"
LOCAL_PRED1_FILE = "output/model1_predictions.csv"
LOCAL_PRED2_FILE = "output/model2_predictions.csv"
LOCAL_EVAL_FILE = "output/evaluation.csv"

# --- Helper Function ---
def run(command):
    """Executes a shell command and returns its output."""
    print(f"Executing command: {command}")
    output = os.popen(command).read()
    print(output)
    return output

def hdfs_to_local(hdfs_path, local_path, is_dir=False):
    """Copies files/dirs from HDFS to local, handling overwrite."""
    if is_dir:
        run(f"rm -rf {local_path}") # Remove local dir first
        run(f"hdfs dfs -get {hdfs_path} {local_path}")
    else:
        run(f"rm -f {local_path}") # Remove local file first
        run(f"hdfs dfs -cat {hdfs_path}/*.csv > {local_path}") # Assumes CSV for files

# --- Main Execution ---
if __name__ == "__main__":
    start_time = time.time()

    # 1. Initialize Spark Session
    print("Initializing Spark Session...")
    spark = SparkSession.builder \
        .appName(f"{TEAM_NAME} - Spark ML Salary Prediction") \
        .master("yarn") \
        .config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883") \
        .config("spark.sql.warehouse.dir", WAREHOUSE_LOCATION) \
        .config("spark.sql.avro.compression.codec", "snappy") \
        .enableHiveSupport() \
        .getOrCreate()
    print("Spark Session initialized.")

    # 2. Load Data
    print(f"Loading training data from {HDFS_TRAIN_DATA_PATH}...")
    train_data = spark.read.format("json").load(HDFS_TRAIN_DATA_PATH)
    print(f"Loading test data from {HDFS_TEST_DATA_PATH}...")
    test_data = spark.read.format("json").load(HDFS_TEST_DATA_PATH)

    # Cache data for better performance during iteration
    train_data.cache()
    test_data.cache()
    print(f"Training data count: {train_data.count()}")
    print(f"Test data count: {test_data.count()}")
    train_data.printSchema()

    # --- Model Training, Tuning, and Evaluation ---

    # Define evaluators (RMSE and R2)
    evaluator_rmse = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
    evaluator_r2 = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="r2")

    # Dictionary to store results
    results = {}

    # --- Model 1: Linear Regression ---
    print("\n--- Training Model 1: Linear Regression ---")
    lr = LinearRegression(featuresCol="features", labelCol="label")

    # Hyperparameter Grid
    lr_param_grid = ParamGridBuilder() \
        .addGrid(lr.regParam, [0.01, 0.1, 0.5]) \
        .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0]) \
        .build()

    # Cross-Validator
    lr_cv = CrossValidator(estimator=lr,
                           estimatorParamMaps=lr_param_grid,
                           evaluator=evaluator_rmse, # Use RMSE for tuning
                           numFolds=3,
                           parallelism=4, # Adjust based on cluster resources
                           seed=42)

    # Train Cross-Validator
    print("Running Cross-Validation for Linear Regression...")
    lr_cv_model = lr_cv.fit(train_data)
    print("Cross-Validation finished.")

    # Get Best Model
    lr_best_model = lr_cv_model.bestModel
    print(f"Best LR Model Params: regParam={lr_best_model._java_obj.getRegParam()}, elasticNetParam={lr_best_model._java_obj.getElasticNetParam()}")

    # Save Best Model
    print(f"Saving best Linear Regression model to {HDFS_MODEL1_PATH}...")
    lr_best_model.write().overwrite().save(HDFS_MODEL1_PATH)
    print("Model saved.")

    # Predict on Test Data
    print("Predicting on test data using best Linear Regression model...")
    lr_predictions = lr_best_model.transform(test_data)
    lr_predictions.select("label", "prediction").show(5)

    # Evaluate Best Model
    lr_rmse = evaluator_rmse.evaluate(lr_predictions)
    lr_r2 = evaluator_r2.evaluate(lr_predictions)
    results['LinearRegression'] = {'RMSE': lr_rmse, 'R2': lr_r2}
    print(f"Linear Regression - Test RMSE: {lr_rmse}, Test R2: {lr_r2}")

    # Save Predictions
    print(f"Saving Linear Regression predictions to {HDFS_PRED1_PATH}...")
    lr_predictions.select("label", "prediction") \
        .coalesce(1) \
        .write \
        .mode("overwrite") \
        .format("csv") \
        .option("sep", ",") \
        .option("header", "true") \
        .save(HDFS_PRED1_PATH)
    print("Predictions saved.")

    # --- Model 2: Random Forest Regressor ---
    print("\n--- Training Model 2: Random Forest Regressor ---")
    rf = RandomForestRegressor(featuresCol="features", labelCol="label", seed=42)

    # Hyperparameter Grid
    rf_param_grid = ParamGridBuilder() \
        .addGrid(rf.numTrees, [10, 20]) \
        .addGrid(rf.maxDepth, [5, 10]) \
        .addGrid(rf.maxBins, [32, 64]) \
        .build()
        # Note: Reduced grid size for potentially faster execution example.
        # Expand grid for better tuning: .addGrid(rf.numTrees, [10, 20, 50]) .addGrid(rf.maxDepth, [5, 10, 15])

    # Cross-Validator
    rf_cv = CrossValidator(estimator=rf,
                           estimatorParamMaps=rf_param_grid,
                           evaluator=evaluator_rmse, # Use RMSE for tuning
                           numFolds=3,
                           parallelism=4, # Adjust based on cluster resources
                           seed=42)

    # Train Cross-Validator
    print("Running Cross-Validation for Random Forest...")
    rf_cv_model = rf_cv.fit(train_data)
    print("Cross-Validation finished.")

    # Get Best Model
    rf_best_model = rf_cv_model.bestModel
    print(f"Best RF Model Params: numTrees={rf_best_model.getNumTrees}, maxDepth={rf_best_model.getMaxDepth()}, maxBins={rf_best_model.getMaxBins()}")


    # Save Best Model
    print(f"Saving best Random Forest model to {HDFS_MODEL2_PATH}...")
    rf_best_model.write().overwrite().save(HDFS_MODEL2_PATH)
    print("Model saved.")

    # Predict on Test Data
    print("Predicting on test data using best Random Forest model...")
    rf_predictions = rf_best_model.transform(test_data)
    rf_predictions.select("label", "prediction").show(5)

    # Evaluate Best Model
    rf_rmse = evaluator_rmse.evaluate(rf_predictions)
    rf_r2 = evaluator_r2.evaluate(rf_predictions)
    results['RandomForest'] = {'RMSE': rf_rmse, 'R2': rf_r2}
    print(f"Random Forest - Test RMSE: {rf_rmse}, Test R2: {rf_r2}")

    # Save Predictions
    print(f"Saving Random Forest predictions to {HDFS_PRED2_PATH}...")
    rf_predictions.select("label", "prediction") \
        .coalesce(1) \
        .write \
        .mode("overwrite") \
        .format("csv") \
        .option("sep", ",") \
        .option("header", "true") \
        .save(HDFS_PRED2_PATH)
    print("Predictions saved.")

    # --- Model Comparison ---
    print("\n--- Comparing Models ---")
    comparison_data = [
        (model_name, metrics['RMSE'], metrics['R2'])
        for model_name, metrics in results.items()
    ]
    comparison_df = spark.createDataFrame(comparison_data, ["model", "RMSE", "R2"])
    comparison_df.show(truncate=False)

    # Save Comparison Results
    print(f"Saving comparison results to {HDFS_EVAL_PATH}...")
    comparison_df.coalesce(1) \
        .write \
        .mode("overwrite") \
        .format("csv") \
        .option("sep", ",") \
        .option("header", "true") \
        .save(HDFS_EVAL_PATH)
    print("Comparison results saved.")

    # --- Copy Results from HDFS to Local ---
    print("\n--- Copying results from HDFS to Local ---")
    hdfs_to_local(HDFS_MODEL1_PATH, LOCAL_MODEL1_DIR, is_dir=True)
    hdfs_to_local(HDFS_MODEL2_PATH, LOCAL_MODEL2_DIR, is_dir=True)
    hdfs_to_local(HDFS_PRED1_PATH, LOCAL_PRED1_FILE)
    hdfs_to_local(HDFS_PRED2_PATH, LOCAL_PRED2_FILE)
    hdfs_to_local(HDFS_EVAL_PATH, LOCAL_EVAL_FILE)
    print("Results copied locally.")

    # --- Cleanup ---
    train_data.unpersist()
    test_data.unpersist()

    print("\nStopping Spark Session...")
    spark.stop()
    print("Spark Session stopped.")

    end_time = time.time()
    print(f"Stage 3 finished in {end_time - start_time:.2f} seconds.") 