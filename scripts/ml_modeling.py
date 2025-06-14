#!/usr/bin/env python
# ml_modeling.py
import os
import math
import numpy as np
from pprint import pprint

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
from pyspark.ml.linalg import VectorUDT # Import VectorUDT for schema

from pyspark.ml import PipelineModel
from pyspark.ml.regression import LinearRegression, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

# Function to run HDFS commands (used for cleanup and checking existence)
def run_hdfs_command(command):
    print(f"Executing HDFS command (simulation): {command}")
    return "" # Return empty string for simulation


def main():
    # --- Spark Session Setup ---
    team = 14
    warehouse = "project/hive/warehouse"

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

    # Define the schema for the input JSON data (including job_id)
    input_schema = StructType([
        StructField("job_id", StringType(), True),    # Added job_id
        StructField("features", VectorUDT(), True),
        StructField("label", DoubleType(), True)
    ])

    print(f"Loading training data from HDFS: {train_data_path}")
    # Apply the explicit schema when reading
    train_data = spark.read.format("json").schema(input_schema).load(train_data_path)
    print(f"Loading test data from HDFS: {test_data_path}")
    # Apply the explicit schema when reading
    test_data = spark.read.format("json").schema(input_schema).load(test_data_path)

    # --- Load Original Data ---
    print("Loading original job descriptions data from Hive...")
    db = 'team14_projectdb'
    original_table_name = 'job_descriptions_part'
    original_df = spark.read.format("avro").table(f'{db}.{original_table_name}')
    # Select necessary columns and cache
    original_df = original_df.select("job_id", "*").filter(F.col("job_id").isNotNull()).cache()
    print(f"Original data count: {original_df.count()}")

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

    print("Extracting and saving Linear Regression tuning results...")
    lr_param_maps = lr_cv.getEstimatorParamMaps()
    lr_avg_metrics = lr_cv_model.avgMetrics
    lr_tuning_results = []
    for params, metric in zip(lr_param_maps, lr_avg_metrics):
        param_dict = {param.name: value for param, value in params.items()}
        # Cast NumPy float64 to Python float
        param_dict['avgRMSE'] = float(metric)
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

    # Save Model 1
    model1_path_hdfs = "project/models/model1"
    print(f"Saving Best Linear Regression model to HDFS: {model1_path_hdfs}")
    run_hdfs_command(f"hdfs dfs -rm -r -f {model1_path_hdfs}") # Clean up previous model
    model1.write().overwrite().save(model1_path_hdfs)
    print("Model 1 saved.")

    # Predict and Evaluate Model 1 on the *test* data
    print("Predicting on test data using Model 1...")
    predictions1 = model1.transform(test_data)

    print("Evaluating Model 1 (on log-scale)...")
    rmse1 = evaluator_rmse.evaluate(predictions1)
    r21 = evaluator_r2.evaluate(predictions1)
    # Store log-scale results for comparison table
    results.append(("LinearRegression", rmse1, r21))

    print("Joining Model 1 predictions with original data...")
    # Select necessary columns from predictions (job_id, log label, log prediction)
    predictions1_to_join = predictions1.select("job_id", F.col("label").alias("log_label"), F.col("prediction").alias("log_prediction"))

    # Join with original data
    enriched_predictions1 = original_df.join(predictions1_to_join, "job_id", "inner")

    print("Converting Model 1 label and prediction back to original scale...")
    enriched_predictions1 = enriched_predictions1 \
        .withColumn("original_salary", F.expm1(F.col("log_label"))) \
        .withColumn("predicted_salary", F.expm1(F.col("log_prediction")))

    # Select all original columns + original_salary + predicted_salary
    # Drop the temporary log columns and potentially duplicate job_id if necessary
    # Let's dynamically get original columns except job_id to avoid duplication
    original_cols = [col for col in original_df.columns if col != 'job_id']
    final_output_cols1 = ['job_id'] + original_cols + ['original_salary', 'predicted_salary']
    final_predictions1 = enriched_predictions1.select(final_output_cols1)

    # Save Enriched Predictions 1
    predictions1_path_hdfs = "project/output/model1_predictions.csv"
    print(f"Saving enriched Model 1 predictions to HDFS: {predictions1_path_hdfs}")
    run_hdfs_command(f"hdfs dfs -rm -r -f {predictions1_path_hdfs}")
    final_predictions1 \
        .coalesce(1) \
        .write \
        .mode("overwrite") \
        .format("csv") \
        .option("sep", ",") \
        .option("header", "true") \
        .save(predictions1_path_hdfs)
    print("Enriched Model 1 predictions saved.")


    # --- Model 2: Gradient-Boosted Trees (GBT) Regressor ---
    print("\n--- Starting Model 2: GBT Regressor ---")
    gbt = GBTRegressor(featuresCol="features", labelCol="label", seed=42)

    # Hyperparameter Tuning Setup
    print("Setting up ParamGridBuilder for GBT...")
    gbt_grid = ParamGridBuilder() \
        .addGrid(gbt.maxDepth, [3, 5]) \
        .addGrid(gbt.maxIter, [10]) \
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

    print("Extracting and saving GBT tuning results...")
    gbt_param_maps = gbt_cv.getEstimatorParamMaps()
    gbt_avg_metrics = gbt_cv_model.avgMetrics
    gbt_tuning_results = []
    for params, metric in zip(gbt_param_maps, gbt_avg_metrics):
        param_dict = {param.name: value for param, value in params.items()}
        # Cast NumPy float64 to Python float
        param_dict['avgRMSE'] = float(metric)
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

    # Save Model 2
    model2_path_hdfs = "project/models/model2"
    print(f"Saving Best GBT model to HDFS: {model2_path_hdfs}")
    run_hdfs_command(f"hdfs dfs -rm -r -f {model2_path_hdfs}") # Clean up previous model
    model2.write().overwrite().save(model2_path_hdfs)
    print("Model 2 saved.")

    # Predict and Evaluate Model 2 on the *test* data
    print("Predicting on test data using Model 2...")
    predictions2 = model2.transform(test_data)

    print("Evaluating Model 2 (on log-scale)...")
    rmse2 = evaluator_rmse.evaluate(predictions2)
    r22 = evaluator_r2.evaluate(predictions2)
    # Store log-scale results for comparison table
    results.append(("GBTRegressor", rmse2, r22))

    print("Joining Model 2 predictions with original data...")
    # Select necessary columns from predictions (job_id, log label, log prediction)
    predictions2_to_join = predictions2.select("job_id", F.col("label").alias("log_label"), F.col("prediction").alias("log_prediction"))

    # Join with original data
    enriched_predictions2 = original_df.join(predictions2_to_join, "job_id", "inner")

    print("Converting Model 2 label and prediction back to original scale...")
    enriched_predictions2 = enriched_predictions2 \
        .withColumn("original_salary", F.expm1(F.col("log_label"))) \
        .withColumn("predicted_salary", F.expm1(F.col("log_prediction")))

    # Select all original columns + original_salary + predicted_salary
    # Use the same column list logic as before
    final_output_cols2 = ['job_id'] + original_cols + ['original_salary', 'predicted_salary']
    final_predictions2 = enriched_predictions2.select(final_output_cols2)

    # Save Enriched Predictions 2
    predictions2_path_hdfs = "project/output/model2_predictions.csv"
    print(f"Saving enriched Model 2 predictions to HDFS: {predictions2_path_hdfs}")
    run_hdfs_command(f"hdfs dfs -rm -r -f {predictions2_path_hdfs}")
    final_predictions2 \
        .coalesce(1) \
        .write \
        .mode("overwrite") \
        .format("csv") \
        .option("sep", ",") \
        .option("header", "true") \
        .save(predictions2_path_hdfs)
    print("Enriched Model 2 predictions saved.")


    # --- Compare Models ---
    print("\n--- Comparing Models ---")
    comparison_df = spark.createDataFrame(results, ["Model_Type", "RMSE", "R2"])
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
    original_df.unpersist() # Unpersist original data
    print("\nStopping Spark Session...")
    spark.stop()
    print("Script finished successfully!")

if __name__ == "__main__":
    main() 