#!/usr/bin/env python
# feature_analysis.py

import math
import re
import json
import sys
import os
import pandas as pd # For creating correlation matrix DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
    ArrayType, BooleanType
)
from pyspark.ml.feature import StringIndexer
from pyspark.ml.stat import Correlation
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.feature import VectorAssembler

# --- Configuration ---
TEAM_NUMBER = 14
HIVE_DB = f'team{TEAM_NUMBER}_projectdb'
HIVE_TABLE = 'job_descriptions_part'
HDFS_OUTPUT_DIR = f"project/data_insights" # HDFS base path for CSV results
POSTGRES_JDBC_URL = f"jdbc:postgresql://hadoop-04.uni.innopolis.ru/team{TEAM_NUMBER}_projectdb"
POSTGRES_USER = f"team{TEAM_NUMBER}"
POSTGRES_PASSWORD_FILE = "secrets/.psql.pass"
JDBC_DRIVER_PATH = "/shared/postgresql-42.6.1.jar"
LOG_FILENAME = "feature_analysis_output.txt" # Local log file

# --- Utility Functions ---

def read_password(file_path):
    """Read a password from a file."""
    try:
        with open(file_path, "r", encoding='utf-8') as file:
            return file.read().rstrip()
    except FileNotFoundError:
        print(f"Error: Password file not found at {file_path}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error reading password file {file_path}: {e}", file=sys.stderr)
        sys.exit(1)

def save_results(spark_df, base_filename, pg_table_name, pg_url, pg_properties):
    """Saves a Spark DataFrame to HDFS (CSV) and PostgreSQL."""
    hdfs_path = f"{HDFS_OUTPUT_DIR}/{base_filename}.csv"
    print(f"Saving results to HDFS: {hdfs_path}")
    try:
        spark_df.coalesce(1).write \
            .option("header", "true") \
            .mode("overwrite") \
            .csv(hdfs_path)
        print(f"Successfully saved to HDFS.")
    except Exception as e:
        print(f"Error saving to HDFS {hdfs_path}: {e}", file=sys.stderr)
        # Decide if you want to stop or continue if HDFS save fails

    print(f"Saving results to PostgreSQL table: {pg_table_name}")
    try:
        spark_df.write \
            .format("jdbc") \
            .option("url", pg_url) \
            .option("dbtable", pg_table_name) \
            .option("user", pg_properties["user"]) \
            .option("password", pg_properties["password"]) \
            .option("driver", pg_properties["driver"]) \
            .mode("overwrite") \
            .save()
        print(f"Successfully saved to PostgreSQL.")
    except Exception as e:
        print(f"Error saving to PostgreSQL table {pg_table_name}: {e}", file=sys.stderr)
        # Decide if you want to stop or continue if PG save fails


# Initialize Spark Session
def create_spark_session(team_number):
    warehouse = "project/hive/warehouse"
    print("Creating Spark session...")
    spark = SparkSession.builder \
        .appName(f"Team {team_number} - Spark Feature Analysis") \
        .master("yarn") \
        .config("spark.submit.deployMode", "client") \
        .config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883") \
        .config("spark.sql.warehouse.dir", warehouse) \
        .config("spark.sql.avro.compression.codec", "snappy") \
        .config("spark.driver.extraClassPath", JDBC_DRIVER_PATH) \
        .config("spark.jars", JDBC_DRIVER_PATH) \
        .enableHiveSupport() \
        .getOrCreate()
    print("Spark session created.")
    return spark

# --- UDFs (Copied from original) ---
def extract_salary_range(salary):
    try:
        if not salary: return (None, None)
        salary = salary.replace('$', '')
        parts = salary.split('-')
        min_salary = float(parts[0].replace('K', '')) * 1000 if 'K' in parts[0] else float(parts[0])
        max_salary = float(parts[1].replace('K', '')) * 1000 if 'K' in parts[1] else float(parts[1])
        return (min_salary, max_salary)
    except: return (None, None)

def extract_experience(exp_str):
    try:
        if exp_str is None: return (None, None)
        numbers = re.findall(r'\d+', exp_str)
        if len(numbers) >= 2: return (int(numbers[0]), int(numbers[1]))
        elif len(numbers) == 1: return (int(numbers[0]), int(numbers[0]))
        else: return (None, None)
    except: return (None, None)

def is_valid_json(s):
    try: json.loads(s); return True
    except: return False

def infer_gender(name):
    if not name or not name.strip(): return "unknown"
    MALE_NAMES = {"john", "david", "michael", "robert", "james", "william", "mark", "richard", "thomas", "charles", "steven", "kevin", "joseph", "brian", "jeff", "scott", "mike", "paul", "dan", "chris", "tim", "greg"}
    FEMALE_NAMES = {"mary", "patricia", "jennifer", "linda", "elizabeth", "barbara", "susan", "jessica", "sarah", "karen", "lisa", "nancy", "betty", "margaret", "sandra", "ashley", "kimberly", "emily", "donna", "michelle", "carol", "amanda", "melissa", "deborah", "stephanie"}
    try:
        first_name = name.strip().split()[0].lower()
        if first_name in MALE_NAMES: return "male"
        elif first_name in FEMALE_NAMES: return "female"
        return "unknown"
    except (AttributeError, IndexError): return "unknown"

def lat_lon_to_ecef(lat, lon, alt=0):
    a = 6378137.0; b = 6356752.314245; f = (a - b) / a; e_sq = f * (2 - f)
    try:
        lat_rad = math.radians(float(lat)); lon_rad = math.radians(float(lon))
        N = a / math.sqrt(1 - e_sq * math.sin(lat_rad)**2)
        x = (N + alt) * math.cos(lat_rad) * math.cos(lon_rad)
        y = (N + alt) * math.cos(lat_rad) * math.sin(lon_rad)
        z = (N * (1 - e_sq) + alt) * math.sin(lat_rad)
        return (x, y, z)
    except: return None

# --- Main Analysis Function ---
def main():
    original_stdout = sys.stdout # Save original stdout
    spark = None # Initialize spark to None for finally block

    try:
        with open(LOG_FILENAME, 'w') as f_out:
            sys.stdout = f_out # Redirect stdout to log file

            # --- Setup ---
            print("--- Starting Feature Analysis Script ---")
            spark = create_spark_session(TEAM_NUMBER)
            postgres_password = read_password(POSTGRES_PASSWORD_FILE)
            pg_properties = {
                "user": POSTGRES_USER,
                "password": postgres_password,
                "driver": "org.postgresql.Driver"
            }

            print(f"Reading raw data from Hive: {HIVE_DB}.{HIVE_TABLE}")
            df = spark.read.format("avro").table(f'{HIVE_DB}.{HIVE_TABLE}')

            # --- Apply Preprocessing Steps (similar to data_preprocessing.py, but keep columns separate) ---
            print("\n--- Applying Preprocessing Steps ---")

            # Parse salary_range and filter
            print("Processing Salary...")
            salary_schema = StructType([StructField("min", DoubleType(), True), StructField("max", DoubleType(), True)])
            extract_salary_udf = F.udf(extract_salary_range, salary_schema)
            df = df.withColumn("salary_parsed", extract_salary_udf("salary_range")) \
                .withColumn("salary_min", F.col("salary_parsed.min")) \
                .withColumn("salary_max", F.col("salary_parsed.max")) \
                .withColumn("salary_avg", (F.col("salary_min") + F.col("salary_max")) / 2)
            initial_count = df.count()
            df = df.filter(F.col("salary_avg").isNotNull())
            print(f"Rows after salary filtering: {df.count()} (removed {initial_count - df.count()})")

            # Parse experience
            print("Processing Experience...")
            experience_schema = StructType([StructField("min", IntegerType(), True), StructField("max", IntegerType(), True)])
            extract_experience_udf = F.udf(extract_experience, experience_schema)
            df = df.withColumn("experience_parsed", extract_experience_udf("experience")) \
                .withColumn("experience_min", F.col("experience_parsed.min")) \
                .withColumn("experience_max", F.col("experience_parsed.max")) \
                .withColumn("experience_avg", (F.col("experience_min") + F.col("experience_max")) / 2)
            df = df.fillna({'experience_min': 0, 'experience_max': 0, 'experience_avg': 0})

            # Process company_profile and filter
            print("Processing Company Profile...")
            is_valid_json_udf = F.udf(is_valid_json, BooleanType())
            df = df.withColumn("is_valid_json", is_valid_json_udf("company_profile"))
            initial_count = df.count()
            df = df.filter(F.col("is_valid_json"))
            print(f"Rows after company profile JSON filtering: {df.count()} (removed {initial_count - df.count()})")

            df = df.withColumn("company_profile_cleaned", F.regexp_replace("company_profile", "'", '"'))
            company_profile_schema = StructType([
                StructField("Sector", StringType(), True), StructField("Industry", StringType(), True),
                StructField("City", StringType(), True), StructField("State", StringType(), True),
                StructField("Zip", StringType(), True), StructField("Website", StringType(), True),
                StructField("Ticker", StringType(), True), StructField("CEO", StringType(), True)
            ])
            df = df.withColumn("company_profile_parsed", F.from_json(F.col("company_profile_cleaned"), company_profile_schema))
            df = df.withColumn("sector", F.col("company_profile_parsed.Sector")) \
                   .withColumn("industry", F.col("company_profile_parsed.Industry")) \
                   .withColumn("company_city", F.col("company_profile_parsed.City")) \
                   .withColumn("company_state", F.col("company_profile_parsed.State")) \
                   .withColumn("company_zip", F.col("company_profile_parsed.Zip"))
            df = df.withColumn("is_public", F.when(F.col("company_profile_parsed.Ticker").isNotNull() & (F.length(F.col("company_profile_parsed.Ticker")) > 0), 1).otherwise(0))
            infer_gender_udf = F.udf(infer_gender, StringType())
            df = df.withColumn("ceo_gender", infer_gender_udf(F.col("company_profile_parsed.CEO")))

            # Process benefits
            print("Processing Benefits...")
            df = df.withColumn("benefits_cleaned", F.regexp_replace(F.regexp_replace(F.col("benefits"), r"\{\\'|\\'\\'\\\\'", ""), r",\s", ","))
            df = df.withColumn("benefits_count", F.size(F.split(F.col("benefits_cleaned"), ",")))
            common_benefits = ["health insurance", "dental", "vision", "401k", "retirement", "pto", "paid time off", "flexible", "remote", "bonus", "education", "training", "insurance", "life insurance"]
            for benefit in common_benefits:
                df = df.withColumn(f"has_{benefit.replace(' ', '_')}", F.when(F.lower(F.col("benefits")).contains(benefit), 1).otherwise(0))

            # Skills and Responsibilities counts
            print("Processing Skills/Responsibilities...")
            df = df.withColumn("skills_count", F.size(F.split(F.col("skills"), " ")))
            df = df.withColumn("responsibilities_count", F.size(F.split(F.col("responsibilities"), "\\.")))

            # Process job_posting_date
            print("Processing Job Posting Date...")
            df = df.withColumn("job_posting_year", F.year("job_posting_date")) \
                   .withColumn("job_posting_month", F.month("job_posting_date")) \
                   .withColumn("job_posting_day", F.dayofmonth("job_posting_date")) \
                   .withColumn("month_sin", F.sin(2 * math.pi * F.col("job_posting_month") / 12)) \
                   .withColumn("month_cos", F.cos(2 * math.pi * F.col("job_posting_month") / 12)) \
                   .withColumn("day_sin", F.sin(2 * math.pi * F.col("job_posting_day") / 31)) \
                   .withColumn("day_cos", F.cos(2 * math.pi * F.col("job_posting_day") / 31))

            # Geospatial processing (ECEF)
            print("Processing Geospatial Data...")
            ecef_schema = StructType([StructField("x", DoubleType(), False), StructField("y", DoubleType(), False), StructField("z", DoubleType(), False)])
            ecef_udf = F.udf(lat_lon_to_ecef, ecef_schema)
            df = df.withColumn("ecef", ecef_udf("latitude", "longitude")) \
                .withColumn("ecef_x", F.col("ecef.x")) \
                .withColumn("ecef_y", F.col("ecef.y")) \
                .withColumn("ecef_z", F.col("ecef.z"))
            df = df.fillna({'ecef_x': 0, 'ecef_y': 0, 'ecef_z': 0})

            # --- Feature Selection ---
            numerical_features = [
                'company_size', 'benefits_count', 'skills_count', 'responsibilities_count',
                'job_posting_year', 'experience_min', 'experience_max', 'experience_avg',
                'ecef_x', 'ecef_y', 'ecef_z', 'is_public'
            ]
            categorical_features = [
                'qualifications', 'work_type', 'preference', 'job_portal', 'sector',
                'industry', 'company_state', 'ceo_gender'
            ]
            cyclical_features = ['month_sin', 'month_cos', 'day_sin', 'day_cos']
            binary_features = [col for col in df.columns if col.startswith('has_')]
            target = 'salary_avg'

            analysis_cols = numerical_features + cyclical_features + binary_features + [target]
            df_analysis = df.select(analysis_cols + categorical_features).persist()
            print(f"\nFinal DataFrame for Analysis has {df_analysis.count()} rows.")

            # --- Analysis & Saving Results ---

            # 1. Target Variable Analysis
            print("\n--- Target Variable Analysis (salary_avg) ---")
            target_dist_df = df_analysis.select(target).describe()
            target_dist_df.show()
            save_results(target_dist_df, "target_distribution", "target_distribution", POSTGRES_JDBC_URL, pg_properties)

            # 2. Correlation Analysis (Features vs Target)
            print("\n--- Correlation Analysis (Numerical, Cyclical, Binary vs Target) ---")
            correlations = {}
            for col_name in analysis_cols:
                if col_name != target:
                    try:
                        corr_df = df_analysis.select(col_name, target).na.drop()
                        if corr_df.count() > 1:
                            corr_value = corr_df.corr(col_name, target)
                            correlations[col_name] = corr_value
                        else: correlations[col_name] = None
                    except Exception as e:
                        print(f"Could not calculate correlation for {col_name}: {e}")
                        correlations[col_name] = None

            print("Correlation with Target (salary_avg):")
            sorted_correlations = sorted(correlations.items(), key=lambda item: abs(item[1]) if item[1] is not None else -1, reverse=True)
            corr_list_for_df = []
            for col_name, corr_value in sorted_correlations:
                print(f"- {col_name}: {corr_value:.4f}" if corr_value is not None else f"- {col_name}: N/A")
                corr_list_for_df.append((col_name, corr_value))

            # Create DataFrame for correlations
            corr_schema = StructType([
                StructField("feature", StringType(), True),
                StructField("correlation_with_target", DoubleType(), True)
            ])
            feature_corr_df = spark.createDataFrame(corr_list_for_df, schema=corr_schema)
            save_results(feature_corr_df, "feature_correlations", "feature_correlations", POSTGRES_JDBC_URL, pg_properties)


            # 3. Multicollinearity Check (Numerical Features)
            print("\n--- Multicollinearity Check (Numerical Features) ---")
            vec_assembler_num = VectorAssembler(inputCols=numerical_features, outputCol="num_features_vec", handleInvalid="skip")
            df_vector_num = vec_assembler_num.transform(df_analysis).select("num_features_vec")

            try:
                correlation_matrix_result = Correlation.corr(df_vector_num, "num_features_vec").head()
                if correlation_matrix_result:
                    corr_matrix_dense = correlation_matrix_result[0].toArray()
                    print("Correlation Matrix (Numerical Features):")
                    print("Features:", numerical_features)
                    print(corr_matrix_dense)

                    # Convert matrix to DataFrame for saving
                    pandas_df = pd.DataFrame(corr_matrix_dense, index=numerical_features, columns=numerical_features)
                    pandas_df = pandas_df.stack().reset_index() # Unpivot
                    pandas_df.columns = ['feature1', 'feature2', 'correlation']
                    multicollinearity_df = spark.createDataFrame(pandas_df)
                    save_results(multicollinearity_df, "numerical_feature_multicollinearity", "numerical_feature_multicollinearity", POSTGRES_JDBC_URL, pg_properties)


                    print("\nHighly Correlated Numerical Feature Pairs (|corr| > 0.8):")
                    highly_correlated = False
                    for i in range(len(numerical_features)):
                        for j in range(i + 1, len(numerical_features)):
                            if abs(corr_matrix_dense[i, j]) > 0.8:
                                print(f"- ({numerical_features[i]}, {numerical_features[j]}): {corr_matrix_dense[i, j]:.4f}")
                                highly_correlated = True
                    if not highly_correlated:
                        print("No pairs found with |correlation| > 0.8.")
                else:
                    print("Could not compute correlation matrix (maybe insufficient data).")
            except Exception as e:
                print(f"Error computing or saving correlation matrix: {e}")


            # 4. Categorical Feature Analysis
            print("\n--- Categorical Feature Analysis ---")
            for cat_col in categorical_features:
                print(f"\nAnalysis for Category: {cat_col}")
                try:
                    # Category Counts
                    print(f"Top 10 Categories by Count for '{cat_col}':")
                    cat_counts_df = df_analysis.groupBy(cat_col).count().orderBy(F.desc("count"))
                    cat_counts_df.show(10, truncate=False)
                    save_results(cat_counts_df, f"categorical_counts_{cat_col}", f"categorical_counts_{cat_col}", POSTGRES_JDBC_URL, pg_properties)

                    # Average Target per Category
                    print(f"Average {target} per {cat_col} (Top 10 by count):")
                    avg_target_per_cat_df = df_analysis.groupBy(cat_col) \
                        .agg(F.avg(target).alias(f"avg_{target}"), F.count("*").alias("count")) \
                        .orderBy(F.desc("count"))
                    avg_target_per_cat_df.show(10, truncate=False)
                    save_results(avg_target_per_cat_df, f"categorical_avg_target_{cat_col}", f"categorical_avg_target_{cat_col}", POSTGRES_JDBC_URL, pg_properties)

                except Exception as e:
                    print(f"Could not analyze or save results for category {cat_col}: {e}")


            # --- Assessment & Recommendations (Logging Only) ---
            print("\n--- ML Applicability Assessment ---")
            meaningful_correlations = [name for name, corr in sorted_correlations if corr is not None and abs(corr) > 0.1]
            if meaningful_correlations:
                print(f"Found features with |correlation| > 0.1 with target: {meaningful_correlations}")
                print("This suggests that there are *some* linear relationships to exploit.")
            else:
                print("No features found with |correlation| > 0.1 with target. Linear relationships might be weak.")
            print("Categorical analysis shows variations in average salary across different categories (see above).")
            print("Overall Assessment: The data shows some predictive signals. Building an ML model is feasible, but improvements are likely needed.")

            print("\n--- Potential Preprocessing Modifications ---")
            print("1.  **Handle Missing Values:** Use imputation (mean, median, mode, 'Missing' category).")
            print("2.  **Target Transformation:** Check skewness of `salary_avg`. Consider log transformation.")
            print("3.  **Categorical Feature Encoding:** Group rare categories, consider Target Encoding (carefully).")
            print("4.  **Text Feature Engineering:** Tune `CountVectorizer`, try n-grams, Word2Vec.")
            print("5.  **Feature Scaling:** Apply `StandardScaler` or `MinMaxScaler`.")
            print("6.  **Multicollinearity:** Remove redundant features, use PCA, or regularization.")
            print("7.  **Feature Selection:** Use automated methods.")
            print("8.  **Interaction Features:** Create interaction terms.")

            df_analysis.unpersist()
            print("\nFeature analysis script finished processing.")

    except Exception as e:
        # Log any top-level errors to the log file
        print(f"\n--- SCRIPT FAILED ---", file=sys.stderr)
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        # Also print to the redirected stdout (log file)
        print(f"\n--- SCRIPT FAILED ---")
        print(f"An unexpected error occurred: {e}")
        traceback.print_exc()


    finally:
        sys.stdout = original_stdout # Restore stdout BEFORE stopping Spark
        if spark:
            print("Stopping Spark session...")
            spark.stop()
            print("Spark session stopped.")
        else:
            print("Spark session was not initialized.")

    # Print completion message to the actual console
    print(f"\nFeature analysis script execution attempted.")
    print(f"Check the log file '{LOG_FILENAME}' for detailed output.")
    print(f"Check HDFS directory '{HDFS_OUTPUT_DIR}' for CSV results.")
    print(f"Check PostgreSQL database '{POSTGRES_USER}' for table results.")


if __name__ == "__main__":
    main()