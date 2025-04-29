#!/usr/bin/env python
# ml_modeling.py
import os
import math
import numpy as np
from pprint import pprint

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
# Import necessary types for schema definition
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.ml.linalg import VectorUDT # Import VectorUDT for schema

from pyspark.ml import PipelineModel
from pyspark.ml.regression import LinearRegression, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
# Import CrossValidator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

# Function to run HDFS commands (used for cleanup and checking existence)
def run_hdfs_command(command):
    # In a real cluster environment, you might use subprocess or os.system
    # For simplicity here, we'll just print the command.
    # Replace with actual execution logic if needed.
    print(f"Executing HDFS command (simulation): {command}")
    # Example using os.popen (uncomment and adapt if needed):
    # return os.popen(command).read()
    return "" # Return empty string for simulation


def main():
    # --- Spark Session Setup ---
    team = 14 # Make sure this matches your team number
    warehouse = "project/hive/warehouse" # Standard warehouse location

    spark = SparkSession.builder \
        .appName(f"Team {team} - Spark ML Modeling") \
        .master("yarn") \
        .config("spark.submit.deployMode", "client") \
        .config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883") \
        .config("spark.sql.warehouse.dir", warehouse) \
        .enableHiveSupport() \
        .getOrCreate()

    sc = spark.sparkContext
    print("Spark Session Created.")

    # --- Load Preprocessed Data ---
    train_data_path = "project/data/train"
    test_data_path = "project/data/test"

    # Define the schema for the input JSON data
    input_schema = StructType([
        StructField("features", VectorUDT(), True), # Features are stored as vectors
        StructField("label", DoubleType(), True)    # Label is a double (salary_avg)
    ])

    print(f"Loading training data from HDFS: {train_data_path}")
    # Apply the explicit schema when reading
    train_data = spark.read.format("json").schema(input_schema).load(train_data_path)
    print(f"Loading test data from HDFS: {test_data_path}")
    # Apply the explicit schema when reading
    test_data = spark.read.format("json").schema(input_schema).load(test_data_path)

    # Cache data for better performance during iterative ML tasks
    train_data.cache()
    test_data.cache()
    print(f"Training data count: {train_data.count()}")
    print(f"Test data count: {test_data.count()}")

    # --- Model Training & Evaluation ---
    # Define evaluators (RMSE and R2) - use the same for both models
    evaluator_rmse = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
    evaluator_r2 = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="r2")

    results = [] # To store evaluation metrics for comparison

    # --- Model 1: Linear Regression ---
    print("\n--- Starting Model 1: Linear Regression ---")
    lr = LinearRegression(featuresCol="features", labelCol="label")

    # Hyperparameter Tuning Setup
    print("Setting up ParamGridBuilder for Linear Regression...")
    lr_grid = ParamGridBuilder() \
        .addGrid(lr.regParam, [0.01, 0.1, 0.5]) \
        .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0]) \
        .addGrid(lr.aggregationDepth, [2, 3]) \
        .build()

    # Use CrossValidator
    print("Setting up CrossValidator for Linear Regression...")
    lr_cv = CrossValidator(estimator=lr,
                         estimatorParamMaps=lr_grid,
                         evaluator=evaluator_rmse, # Use RMSE for tuning
                         numFolds=3, # Use 3 folds for cross-validation
                         parallelism=4, # Number of parallel jobs
                         seed=42)

    print("Starting CrossValidator fitting for Linear Regression...")
    lr_cv_model = lr_cv.fit(train_data) # Fit on the main training data
    model1 = lr_cv_model.bestModel
    print("CrossValidator fitting finished. Best Linear Regression model selected.")
    print("Best Linear Regression Params:")
    pprint(model1.extractParamMap())

    # --- ADDED: Save LR Tuning Results --- #
    print("Extracting and saving Linear Regression tuning results...")
    lr_param_maps = lr_cv.getEstimatorParamMaps()
    lr_avg_metrics = lr_cv_model.avgMetrics
    lr_tuning_results = []
    for params, metric in zip(lr_param_maps, lr_avg_metrics):
        param_dict = {param.name: value for param, value in params.items()}
        param_dict['avgRMSE'] = metric
        lr_tuning_results.append(param_dict)

    if lr_tuning_results:
        lr_tuning_df = spark.createDataFrame(lr_tuning_results)
        lr_tuning_path_hdfs = "project/output/lr_tuning_results.csv"
        print(f"Saving LR tuning results to HDFS: {lr_tuning_path_hdfs}")
        run_hdfs_command(f"hdfs dfs -rm -r -f {lr_tuning_path_hdfs}")
        lr_tuning_df.coalesce(1).write.mode("overwrite").format("csv") \
            .option("header", "true").save(lr_tuning_path_hdfs)
        print("LR tuning results saved.")
    else:
        print("No LR tuning results to save.")
    # --- END ADDED --- #

    # Save Model 1
    model1_path_hdfs = "project/models/model1"
    print(f"Saving Best Linear Regression model to HDFS: {model1_path_hdfs}")
    run_hdfs_command(f"hdfs dfs -rm -r -f {model1_path_hdfs}") # Clean up previous model
    model1.write().overwrite().save(model1_path_hdfs)
    print("Model 1 saved.")

    # Predict and Evaluate Model 1 on the *test* data
    print("Predicting on test data using Model 1...")
    predictions1 = model1.transform(test_data)

    print("Evaluating Model 1...")
    rmse1 = evaluator_rmse.evaluate(predictions1)
    r21 = evaluator_r2.evaluate(predictions1)
    print(f"Model 1 - Test RMSE: {rmse1}")
    print(f"Model 1 - Test R2: {r21}")
    results.append(("LinearRegression", str(model1), rmse1, r21))

    # Save Predictions 1
    predictions1_path_hdfs = "project/output/model1_predictions.csv"
    print(f"Saving Model 1 predictions to HDFS: {predictions1_path_hdfs}")
    run_hdfs_command(f"hdfs dfs -rm -r -f {predictions1_path_hdfs}") # Clean up previous predictions
    predictions1.select("label", "prediction") \
        .coalesce(1) \
        .write \
        .mode("overwrite") \
        .format("csv") \
        .option("sep", ",") \
        .option("header", "true") \
        .save(predictions1_path_hdfs)
    print("Model 1 predictions saved.")


    # --- Model 2: Gradient-Boosted Trees (GBT) Regressor ---
    print("\n--- Starting Model 2: GBT Regressor ---")
    gbt = GBTRegressor(featuresCol="features", labelCol="label", seed=42)

    # Hyperparameter Tuning Setup
    print("Setting up ParamGridBuilder for GBT...")
    gbt_grid = ParamGridBuilder() \
        .addGrid(gbt.maxDepth, [3, 5, 7]) \
        .addGrid(gbt.maxIter, [10, 20]) \
        .addGrid(gbt.stepSize, [0.1, 0.05]) \
        .build()

    # Use CrossValidator
    print("Setting up CrossValidator for GBT...")
    gbt_cv = CrossValidator(estimator=gbt,
                          estimatorParamMaps=gbt_grid,
                          evaluator=evaluator_rmse, # Use RMSE for tuning
                          numFolds=3, # Use 3 folds for cross-validation
                          parallelism=4,
                          seed=42)

    print("Starting CrossValidator fitting for GBT...")
    gbt_cv_model = gbt_cv.fit(train_data) # Fit on the main training data
    model2 = gbt_cv_model.bestModel
    print("CrossValidator fitting finished. Best GBT model selected.")
    print("Best GBT Params:")
    pprint(model2.extractParamMap())

    # --- ADDED: Save GBT Tuning Results --- #
    print("Extracting and saving GBT tuning results...")
    gbt_param_maps = gbt_cv.getEstimatorParamMaps()
    gbt_avg_metrics = gbt_cv_model.avgMetrics
    gbt_tuning_results = []
    for params, metric in zip(gbt_param_maps, gbt_avg_metrics):
        param_dict = {param.name: value for param, value in params.items()}
        param_dict['avgRMSE'] = metric
        gbt_tuning_results.append(param_dict)

    if gbt_tuning_results:
        gbt_tuning_df = spark.createDataFrame(gbt_tuning_results)
        gbt_tuning_path_hdfs = "project/output/gbt_tuning_results.csv"
        print(f"Saving GBT tuning results to HDFS: {gbt_tuning_path_hdfs}")
        run_hdfs_command(f"hdfs dfs -rm -r -f {gbt_tuning_path_hdfs}")
        gbt_tuning_df.coalesce(1).write.mode("overwrite").format("csv") \
            .option("header", "true").save(gbt_tuning_path_hdfs)
        print("GBT tuning results saved.")
    else:
        print("No GBT tuning results to save.")
    # --- END ADDED --- #

    # Save Model 2
    model2_path_hdfs = "project/models/model2"
    print(f"Saving Best GBT model to HDFS: {model2_path_hdfs}")
    run_hdfs_command(f"hdfs dfs -rm -r -f {model2_path_hdfs}") # Clean up previous model
    model2.write().overwrite().save(model2_path_hdfs)
    print("Model 2 saved.")

    # Predict and Evaluate Model 2 on the *test* data
    print("Predicting on test data using Model 2...")
    predictions2 = model2.transform(test_data)

    print("Evaluating Model 2...")
    rmse2 = evaluator_rmse.evaluate(predictions2)
    r22 = evaluator_r2.evaluate(predictions2)
    print(f"Model 2 - Test RMSE: {rmse2}")
    print(f"Model 2 - Test R2: {r22}")
    results.append(("GBTRegressor", str(model2), rmse2, r22))

    # Save Predictions 2
    predictions2_path_hdfs = "project/output/model2_predictions.csv"
    print(f"Saving Model 2 predictions to HDFS: {predictions2_path_hdfs}")
    run_hdfs_command(f"hdfs dfs -rm -r -f {predictions2_path_hdfs}") # Clean up previous predictions
    predictions2.select("label", "prediction") \
        .coalesce(1) \
        .write \
        .mode("overwrite") \
        .format("csv") \
        .option("sep", ",") \
        .option("header", "true") \
        .save(predictions2_path_hdfs)
    print("Model 2 predictions saved.")


    # --- Compare Models ---
    print("\n--- Comparing Models ---")
    comparison_df = spark.createDataFrame(results, ["Model_Type", "Model_UID", "RMSE", "R2"])
    print("Model Comparison Results:")
    comparison_df.select("Model_Type", "RMSE", "R2").show(truncate=False)

    # Save Comparison Results
    evaluation_path_hdfs = "project/output/evaluation.csv"
    print(f"Saving comparison results to HDFS: {evaluation_path_hdfs}")
    run_hdfs_command(f"hdfs dfs -rm -r -f {evaluation_path_hdfs}") # Clean up previous evaluation
    comparison_df.select("Model_Type", "RMSE", "R2") \
        .coalesce(1) \
        .write \
        .mode("overwrite") \
        .format("csv") \
        .option("sep", ",") \
        .option("header", "true") \
        .save(evaluation_path_hdfs)
    print("Comparison results saved.")

    # --- Cleanup and Stop Spark ---
    train_data.unpersist()
    test_data.unpersist()
    print("\nStopping Spark Session...")
    spark.stop()
    print("Script finished successfully!")

if __name__ == "__main__":
    main() 