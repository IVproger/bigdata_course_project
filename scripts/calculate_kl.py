#!/usr/bin/env python
# calculate_kl.py

import math
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

# --- Constants ---
EPSILON = 1e-10  # Small value to avoid log(0) or division by zero
NUM_BINS = 50    # Number of bins for discretizing salary values

# --- Helper Functions ---
def get_probability_distribution(df, col_name, min_val, max_val, num_bins):
    """Calculates the probability distribution of a column after binning."""
    bin_width = (max_val - min_val) / num_bins
    
    # Assign bin number to each value
    binned_df = df.withColumn(
        f"{col_name}_bin",
        F.floor((F.col(col_name) - min_val) / bin_width).cast("int")
    )
    
    # Handle edge case for the max value
    binned_df = binned_df.withColumn(
        f"{col_name}_bin",
        F.when(F.col(f"{col_name}_bin") >= num_bins, num_bins - 1)
         .when(F.col(f"{col_name}_bin") < 0, 0) # Handle values below min
         .otherwise(F.col(f"{col_name}_bin"))
    )
    
    # Count occurrences in each bin
    bin_counts = binned_df.groupBy(f"{col_name}_bin").count()
    
    # Calculate total count
    total_count = binned_df.count()
    
    # Calculate probability for each bin
    prob_df = bin_counts.withColumn(
        f"{col_name}_prob",
        (F.col("count") / total_count)
    ).select(f"{col_name}_bin", f"{col_name}_prob")
    
    # Ensure all bins from 0 to num_bins-1 are present, filling missing with epsilon
    all_bins_df = spark.range(num_bins).withColumnRenamed("id", f"{col_name}_bin")
    prob_df_full = all_bins_df.join(prob_df, f"{col_name}_bin", "left_outer") \
                            .fillna({f"{col_name}_prob": EPSILON})
                            
    return prob_df_full.orderBy(f"{col_name}_bin")

def calculate_kl(p_df, q_df, p_col="label_prob", q_col="prediction_prob", bin_col="label_bin"):
    """Calculates KL Divergence D_KL(P || Q)."""
    # Join the two distributions on the bin number
    # Ensure the bin column names match for joining
    q_df_renamed = q_df.withColumnRenamed(q_df.columns[0], bin_col) # Rename Q's bin column
    q_col_renamed = q_df.columns[1]
    
    joined_df = p_df.join(q_df_renamed, bin_col, "inner")
    
    # Calculate KL divergence component for each bin: P(i) * log(P(i) / Q(i))
    kl_components = joined_df.withColumn(
        "kl_comp",
        F.col(p_col) * F.log(F.col(p_col) / F.col(q_col_renamed))
    )
    
    # Sum the components
    kl_divergence = kl_components.agg(F.sum("kl_comp")).first()[0]
    return kl_divergence if kl_divergence is not None else float('inf')

# --- Main Execution ---
if __name__ == "__main__":
    # --- Spark Session Setup ---
    team = 14
    warehouse = "project/hive/warehouse"
    
    spark = SparkSession.builder \
        .appName(f"Team {team} - KL Divergence Calculation") \
        .master("yarn") \
        .config("spark.submit.deployMode", "client") \
        .config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883") \
        .config("spark.sql.warehouse.dir", warehouse) \
        .enableHiveSupport() \
        .getOrCreate()
    
    print("Spark Session Created.")

    # --- Define Paths ---
    model1_pred_path = "project/output/model1_predictions.csv"
    model2_pred_path = "project/output/model2_predictions.csv"
    output_path_hdfs = "project/output/kl_divergence.csv"

    # --- Load Predictions --- 
    # Define schema ONLY for the columns needed for KL divergence
    pred_schema = StructType([
        StructField("original_salary", DoubleType(), True), # Updated column name
        StructField("predicted_salary", DoubleType(), True) # Updated column name
    ])

    print(f"Loading Model 1 predictions from HDFS: {model1_pred_path}")
    # Read only necessary columns using the specific schema
    pred1_df = spark.read.format("csv") \
                    .schema(pred_schema) \
                    .option("header", "true") \
                    .load(model1_pred_path)
    pred1_df.cache()
    print(f"Model 1 predictions count: {pred1_df.count()}")

    print(f"Loading Model 2 predictions from HDFS: {model2_pred_path}")
    # Read only necessary columns using the specific schema
    pred2_df = spark.read.format("csv") \
                    .schema(pred_schema) \
                    .option("header", "true") \
                    .load(model2_pred_path)
    pred2_df.cache()
    print(f"Model 2 predictions count: {pred2_df.count()}")

    # --- Determine Bin Range --- 
    # Use the renamed columns for binning
    min_max_label = pred1_df.agg(F.min("original_salary"), F.max("original_salary")).first()
    min_label = min_max_label["min(original_salary)"] # Updated column name
    max_label = min_max_label["max(original_salary)"] # Updated column name

    min_max_pred1 = pred1_df.agg(F.min("predicted_salary"), F.max("predicted_salary")).first() # Updated column name
    min_max_pred2 = pred2_df.agg(F.min("predicted_salary"), F.max("predicted_salary")).first() # Updated column name

    global_min = min(min_label, min_max_pred1["min(predicted_salary)"], min_max_pred2["min(predicted_salary)"]) # Updated column names
    global_max = max(max_label, min_max_pred1["max(predicted_salary)"], min_max_pred2["max(predicted_salary)"]) # Updated column names

    # Add a small buffer to avoid edge issues
    global_min -= EPSILON
    global_max += EPSILON

    print(f"Determined global range for binning: [{global_min}, {global_max}] with {NUM_BINS} bins.")

    # --- Calculate Distributions ---
    print("Calculating probability distributions...")
    # P (Actual Labels - Original Scale) - Use updated column name
    p_dist = get_probability_distribution(pred1_df, "original_salary", global_min, global_max, NUM_BINS)
    p_dist.cache()
    # print("P (Actual Labels) Distribution:")
    # p_dist.show(5, truncate=False)

    # Q1 (Model 1 Predictions - Original Scale) - Use updated column name
    q1_dist = get_probability_distribution(pred1_df, "predicted_salary", global_min, global_max, NUM_BINS)
    q1_dist.cache()
    # print("Q1 (Model 1 Predictions) Distribution:")
    # q1_dist.show(5, truncate=False)
    
    # Q2 (Model 2 Predictions - Original Scale) - Use updated column name
    q2_dist = get_probability_distribution(pred2_df, "predicted_salary", global_min, global_max, NUM_BINS)
    q2_dist.cache()
    # print("Q2 (Model 2 Predictions) Distribution:")
    # q2_dist.show(5, truncate=False)

    # --- Calculate KL Divergence ---
    print("Calculating KL Divergence...")
    # The p_col and q_col names are generated inside the helper function based on input col_name, so they adapt automatically.
    # Need to ensure the bin column name used for joining is correct.
    # Assuming P distribution uses 'original_salary_bin' and Q distributions use 'predicted_salary_bin'
    kl1 = calculate_kl(p_dist, q1_dist, p_col="original_salary_prob", q_col="predicted_salary_prob", bin_col="original_salary_bin")
    kl2 = calculate_kl(p_dist, q2_dist, p_col="original_salary_prob", q_col="predicted_salary_prob", bin_col="original_salary_bin")

    print(f"KL Divergence (Model 1 vs Actual): {kl1}")
    print(f"KL Divergence (Model 2 vs Actual): {kl2}")

    # --- Save Results ---
    kl_results = [
        ("LinearRegression", kl1),
        ("GBTRegressor", kl2)
    ]
    kl_schema = StructType([
        StructField("model_type", StringType(), False),
        StructField("kl_divergence", DoubleType(), False)
    ])
    kl_df = spark.createDataFrame(kl_results, schema=kl_schema)

    print(f"Saving KL Divergence results to HDFS: {output_path_hdfs}")
    # Clean up previous results first using HDFS commands via spark (less direct but avoids os calls)
    try:
        dbutils = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        dbutils.delete(spark._jvm.org.apache.hadoop.fs.Path(output_path_hdfs), True) 
        print(f"Successfully cleaned HDFS path: {output_path_hdfs}")
    except Exception as e:
        print(f"Warning: Could not clean HDFS path {output_path_hdfs}. It might not exist yet. Error: {e}")
        
    kl_df.coalesce(1) \
        .write \
        .mode("overwrite") \
        .format("csv") \
        .option("sep", ",") \
        .option("header", "true") \
        .save(output_path_hdfs)
    print("KL Divergence results saved.")

    # --- Cleanup ---
    pred1_df.unpersist()
    pred2_df.unpersist()
    p_dist.unpersist()
    q1_dist.unpersist()
    q2_dist.unpersist()

    print("Stopping Spark Session...")
    spark.stop()
    print("Script finished successfully!") 