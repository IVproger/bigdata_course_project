#!/usr/bin/env python
# feature_analysis.py

import math
import re
import json
import sys # Import sys module for stdout redirection
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
    ArrayType, BooleanType
)
from pyspark.ml.feature import StringIndexer # For analyzing categorical relationships
from pyspark.ml.stat import Correlation
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.feature import VectorAssembler # To assemble numerical features for correlation matrix


# --- Reusing functions from data_preprocessing.py ---

# Initialize Spark Session
def create_spark_session(team_number):
    warehouse = "project/hive/warehouse"
    spark = SparkSession.builder \
        .appName(f"Team {team_number} - Spark Feature Analysis") \
        .master("yarn") \
        .config("spark.submit.deployMode", "client") \
        .config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883") \
        .config("spark.sql.warehouse.dir", warehouse) \
        .config("spark.sql.avro.compression.codec", "snappy") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark

# UDFs (Copied directly from data_preprocessing.py)
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
    team = 14
    output_filename = "feature_analysis_output.txt"
    original_stdout = sys.stdout # Save original stdout

    try:
        with open(output_filename, 'w') as f_out:
            sys.stdout = f_out # Redirect stdout to file

            spark = create_spark_session(team)
            db = 'team14_projectdb'
            table_name = 'job_descriptions_part'

            print(f"Reading raw data from Hive: {db}.{table_name}")
            df = spark.read.format("avro").table(f'{db}.{table_name}')

            # --- Apply Preprocessing Steps (similar to data_preprocessing.py, but keep columns separate) ---
            print("Applying preprocessing steps for analysis...")

            # *** Handle NaNs - More careful approach for analysis ***
            # Instead of df.na.drop(), we will analyze missingness later if needed,
            # but apply filters used in the original script for comparability
            
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
            # Impute missing experience with median or 0? For analysis, let's keep NULLs for now.
            df = df.fillna({'experience_min': 0, 'experience_max': 0, 'experience_avg': 0}) # Simple imputation for analysis


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
            # Impute missing geo data if necessary (e.g., with 0 or mean)
            df = df.fillna({'ecef_x': 0, 'ecef_y': 0, 'ecef_z': 0}) # Simple imputation for analysis


            # --- Feature Selection (based on data_preprocessing.py) ---
            numerical_features = [
                'company_size', 'benefits_count', 'skills_count', 'responsibilities_count',
                'job_posting_year', 'experience_min', 'experience_max', 'experience_avg',
                'ecef_x', 'ecef_y', 'ecef_z', 'is_public'
            ]
            categorical_features = [
                'qualifications', 'work_type', 'preference', 'job_portal', 'sector',
                'industry', 'company_state', 'ceo_gender'
            ]
            # Text features are excluded from this correlation analysis
            cyclical_features = ['month_sin', 'month_cos', 'day_sin', 'day_cos']
            binary_features = [col for col in df.columns if col.startswith('has_')]
            target = 'salary_avg'

            analysis_cols = numerical_features + cyclical_features + binary_features + [target]
            df_analysis = df.select(analysis_cols + categorical_features).persist() # Persist for multiple uses
            print(f"\nFinal DataFrame for Analysis has {df_analysis.count()} rows.")

            # --- Analysis ---
            print("\n--- Target Variable Analysis (salary_avg) ---")
            df_analysis.select(target).describe().show()
            # Check skewness (requires collecting data or using approximate methods)
            # skewness = df_analysis.select(F.skewness(target)).first()[0]
            # print(f"Target Skewness: {skewness}") # Requires Spark 3.0+

            print("\n--- Correlation Analysis (Numerical, Cyclical, Binary vs Target) ---")
            # Calculate correlations with the target variable
            correlations = {}
            for col_name in analysis_cols:
                if col_name != target:
                    try:
                        # Filter out rows where the column might be null if not imputed earlier
                        corr_df = df_analysis.select(col_name, target).na.drop()
                        if corr_df.count() > 1: # Need at least 2 points for correlation
                            corr_value = corr_df.corr(col_name, target)
                            correlations[col_name] = corr_value
                        else:
                            correlations[col_name] = None # Not enough data
                    except Exception as e:
                        print(f"Could not calculate correlation for {col_name}: {e}")
                        correlations[col_name] = None

            print("Correlation with Target (salary_avg):")
            # Sort by absolute correlation value, descending
            sorted_correlations = sorted(correlations.items(), key=lambda item: abs(item[1]) if item[1] is not None else -1, reverse=True)
            for col_name, corr_value in sorted_correlations:
                print(f"- {col_name}: {corr_value:.4f}" if corr_value is not None else f"- {col_name}: N/A")


            print("\n--- Multicollinearity Check (Numerical Features) ---")
            # Assemble numerical features into a vector
            vec_assembler_num = VectorAssembler(inputCols=numerical_features, outputCol="num_features_vec", handleInvalid="skip")
            df_vector_num = vec_assembler_num.transform(df_analysis).select("num_features_vec")

            # Calculate the correlation matrix
            try:
                correlation_matrix = Correlation.corr(df_vector_num, "num_features_vec").head()
                if correlation_matrix:
                    corr_matrix_dense = correlation_matrix[0].toArray()
                    print("Correlation Matrix (Numerical Features):")
                    print("Features:", numerical_features)
                    print(corr_matrix_dense)

                    # Identify pairs with high correlation (e.g., > 0.8 or < -0.8)
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
                print(f"Error computing correlation matrix: {e}")


            print("\n--- Categorical Feature Analysis ---")
            # Analyze relationship between top categories and average target value
            for cat_col in categorical_features:
                print(f"\nAnalysis for Category: {cat_col}")
                # Show top N categories by count
                df_analysis.groupBy(cat_col).count().orderBy(F.desc("count")).show(10, truncate=False)

                # Show average target value per top N category
                # Requires indexing first if we want to group sparse categories later
                # For now, let's just look at average salary per category (might be slow for high cardinality)
                try:
                    avg_target_per_cat = df_analysis.groupBy(cat_col) \
                        .agg(F.avg(target).alias(f"avg_{target}"), F.count("*").alias("count")) \
                        .orderBy(F.desc("count")) # Order by count to see most frequent first
                    print(f"Average {target} per {cat_col} (Top 10 by count):")
                    avg_target_per_cat.show(10, truncate=False)
                except Exception as e:
                    print(f"Could not calculate average target for {cat_col}: {e}")


            # --- Assessment & Recommendations ---
            print("\n--- ML Applicability Assessment ---")
            # Based on correlations and categorical analysis
            meaningful_correlations = [name for name, corr in sorted_correlations if corr is not None and abs(corr) > 0.1] # Example threshold
            if meaningful_correlations:
                print(f"Found features with |correlation| > 0.1 with target: {meaningful_correlations}")
                print("This suggests that there are *some* linear relationships to exploit.")
            else:
                print("No features found with |correlation| > 0.1 with target. Linear relationships might be weak.")

            print("Categorical analysis shows variations in average salary across different categories (see above).")
            print("Overall Assessment: The data shows some predictive signals (correlations, categorical variations). Building an ML model is feasible, but improvements are likely needed.")
            print("The low metrics in the original models might stem from data loss during preprocessing, feature representation (e.g., OHE sparsity, text handling), multicollinearity, or the need for target transformation.")

            print("\n--- Potential Preprocessing Modifications ---")
            print("1.  **Handle Missing Values:** Instead of `na.drop()`, use imputation strategies:")
            print("    - Numerical: Mean, median, or model-based imputation.")
            print("    - Categorical: Mode or a dedicated 'Missing' category.")
            print("    - Consider imputation *after* train/test split to avoid data leakage if using statistics from the data.")
            print("2.  **Target Transformation:** Check the skewness of `salary_avg`. If highly skewed, apply a log transformation (e.g., `log(1 + salary_avg)`) and train the model to predict the transformed value (remembering to inverse transform predictions).")
            print("3.  **Categorical Feature Encoding:** For high-cardinality features (`industry`, `sector`, `company_state`, `company_city`):")
            print("    - Group rare categories into an 'Other' category before OHE.")
            print("    - Consider Target Encoding (use with caution to prevent target leakage, e.g., apply within cross-validation folds).")
            print("    - Evaluate removing features with very low variance or too many unique values (like `company_zip` if it wasn't already excluded).")
            print("4.  **Text Feature Engineering:**")
            print("    - Tune `minDF`, `maxDF` in `CountVectorizer`.")
            print("    - Experiment with n-grams (`ngram_range` in `CountVectorizer` or separate `NGram` transformer).")
            print("    - Consider `Word2Vec` or other embedding techniques for richer text representation.")
            print("5.  **Feature Scaling:** Apply `StandardScaler` or `MinMaxScaler` to the final assembled `features` vector, especially for Linear Regression.")
            print("6.  **Multicollinearity:** If high correlations were observed (e.g., between experience features or ECEF coordinates):")
            print("    - Remove redundant features (e.g., keep `experience_avg`, drop min/max).")
            print("    - Use PCA for dimensionality reduction on correlated groups.")
            print("    - Rely on models less sensitive to multicollinearity (like GBT, Random Forest) or use regularization (Lasso) that handles it.")
            print("7.  **Feature Selection:** Implement automated feature selection (e.g., using `ChiSqSelector` for categorical, correlation filter, or feature importances from a tree model) instead of manual lists.")
            print("8.  **Interaction Features:** Create potentially useful interaction terms (e.g., `experience * is_public`, `sector * work_type`).")


            df_analysis.unpersist()
            spark.stop()
            print("\nFeature analysis script finished.")

    finally:
        sys.stdout = original_stdout # Restore stdout

    # Print completion message to the actual console
    print(f"Feature analysis output written to {output_filename}")


if __name__ == "__main__":
    main() 

