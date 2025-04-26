# ml_job_descriptions.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
    ArrayType, BooleanType
)
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer, OneHotEncoder, VectorAssembler,
    Tokenizer, StopWordsRemover, CountVectorizer, IDF,
    StandardScaler
)
import json
import re
import math

# Initialize Spark Session
def create_spark_session(team_number):
    warehouse = "project/hive/warehouse"
    spark = SparkSession.builder \
        .appName(f"Team {team_number} - Spark ML Job Descriptions") \
        .master("yarn") \
        .config("spark.submit.deployMode", "client") \
        .config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883") \
        .config("spark.sql.warehouse.dir", warehouse) \
        .config("spark.sql.avro.compression.codec", "snappy") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark

# UDFs for data processing
def extract_salary_range(salary):
    try:
        if not salary:
            return (None, None)
        salary = salary.replace('$', '')
        parts = salary.split('-')
        min_salary = float(parts[0].replace('K', '')) * 1000 if 'K' in parts[0] else float(parts[0])
        max_salary = float(parts[1].replace('K', '')) * 1000 if 'K' in parts[1] else float(parts[1])
        return (min_salary, max_salary)
    except:
        return (None, None)

def extract_experience(exp_str):
    try:
        if exp_str is None:
            return (None, None)
        
        # Find all numbers in the string
        numbers = re.findall(r'\d+', exp_str)
        
        if len(numbers) >= 2:
            min_exp = int(numbers[0])
            max_exp = int(numbers[1])
            return (min_exp, max_exp)
        elif len(numbers) == 1:
            # If only one number, use it for both min and max
            exp = int(numbers[0])
            return (exp, exp)
        else:
            return (None, None)
    except:
        return (None, None)

def is_valid_json(s):
    try:
        json.loads(s)
        return True
    except:
        return False

def infer_gender(name):
    """
    Infer gender from first name using a predefined list of common names.
    
    Args:
        name (str): Full name to analyze (only first name is used)
        
    Returns:
        str: 'male', 'female', or 'unknown' if name is empty or not in lists
    """
    # Handle empty/None input
    if not name or not name.strip():
        return "unknown"

    # Predefined name sets (frozen for immutability)
    MALE_NAMES = {
        "john", "david", "michael", "robert", "james", "william", 
        "mark", "richard", "thomas", "charles", "steven", "kevin",
        "joseph", "brian", "jeff", "scott", "mike", "paul", "dan",
        "chris", "tim", "greg"
    }
    
    FEMALE_NAMES = {
        "mary", "patricia", "jennifer", "linda", "elizabeth", 
        "barbara", "susan", "jessica", "sarah", "karen", "lisa",
        "nancy", "betty", "margaret", "sandra", "ashley", 
        "kimberly", "emily", "donna", "michelle", "carol", 
        "amanda", "melissa", "deborah", "stephanie"
    }

    try:
        first_name = name.strip().split()[0].lower()
        if first_name in MALE_NAMES:
            return "male"
        elif first_name in FEMALE_NAMES:
            return "female"
        return "unknown"
    except (AttributeError, IndexError):
        return "unknown"

def lat_lon_to_ecef(lat, lon, alt=0):
    a = 6378137.0
    b = 6356752.314245
    f = (a - b) / a
    e_sq = f * (2 - f)
    try:
        lat_rad = math.radians(float(lat))
        lon_rad = math.radians(float(lon))
        N = a / math.sqrt(1 - e_sq * math.sin(lat_rad)**2)
        x = (N + alt) * math.cos(lat_rad) * math.cos(lon_rad)
        y = (N + alt) * math.cos(lat_rad) * math.sin(lon_rad)
        z = (N * (1 - e_sq) + alt) * math.sin(lat_rad)
        return (x, y, z)
    except:
        return None

# Main function
def main():
    # Spark session setup
    team = 14  # Update team number if needed
    spark = create_spark_session(team)
    
    # Read Hive table
    print("Read Hive table")
    db = 'team14_projectdb'
    table_name = 'job_descriptions_part'
    df = spark.read.format("avro").table(f'{db}.{table_name}')
    
    # Data preprocessing
    print("Drop Nan values")
    df = df.na.drop()
    
    # Parse salary_range
    print("Parse salary_range")
    salary_schema = StructType([
        StructField("min", DoubleType(), True),
        StructField("max", DoubleType(), True)
    ])
    extract_salary_udf = F.udf(extract_salary_range, salary_schema)
    df = df.withColumn("salary_parsed", extract_salary_udf("salary_range")) \
        .withColumn("salary_min", F.col("salary_parsed.min")) \
        .withColumn("salary_max", F.col("salary_parsed.max")) \
        .withColumn("salary_avg", (F.col("salary_min") + F.col("salary_max")) / 2)
    df = df.filter(F.col("salary_min").isNotNull() & F.col("salary_max").isNotNull())
    
    # Parse experience
    print("Parse experience")
    experience_schema = StructType([
        StructField("min", IntegerType(), True),
        StructField("max", IntegerType(), True)
    ])
    extract_experience_udf = F.udf(extract_experience, experience_schema)
    df = df.withColumn("experience_parsed", extract_experience_udf("experience")) \
        .withColumn("experience_min", F.col("experience_parsed.min")) \
        .withColumn("experience_max", F.col("experience_parsed.max")) \
        .withColumn("experience_avg", (F.col("experience_min") + F.col("experience_max")) / 2)
    
    # Process company_profile
    print("Process company_profile")
    is_valid_json_udf = F.udf(is_valid_json, BooleanType())
    df = df.withColumn("is_valid_json", is_valid_json_udf("company_profile")) \
        .filter(F.col("is_valid_json")) \
        .withColumn("company_profile_cleaned", F.regexp_replace("company_profile", "'", '"'))
    
    company_profile_schema = StructType([
        StructField("Sector", StringType(), True),
        StructField("Industry", StringType(), True),
        StructField("City", StringType(), True),
        StructField("State", StringType(), True),
        StructField("Zip", StringType(), True),
        StructField("Website", StringType(), True),
        StructField("Ticker", StringType(), True),
        StructField("CEO", StringType(), True)
    ])
    # Parse the JSON into separate columns
    df = df.withColumn(
        "company_profile_parsed", 
        F.from_json(F.col("company_profile_cleaned"), company_profile_schema)
    )

    # Extract individual fields from the parsed JSON
    df = df.withColumn("sector", F.col("company_profile_parsed.Sector")) \
           .withColumn("industry", F.col("company_profile_parsed.Industry")) \
           .withColumn("company_city", F.col("company_profile_parsed.City")) \
           .withColumn("company_state", F.col("company_profile_parsed.State")) \
           .withColumn("company_zip", F.col("company_profile_parsed.Zip"))

    # Create is_public flag based on Ticker field
    df = df.withColumn(
        "is_public",
        F.when(
            F.col("company_profile_parsed.Ticker").isNotNull() & 
            (F.length(F.col("company_profile_parsed.Ticker")) > 0), 
            1
        ).otherwise(0)
    )
    
    # CEO gender inference
    infer_gender_udf = F.udf(infer_gender, StringType())
    df = df.withColumn("ceo_gender", infer_gender_udf(F.col("company_profile_parsed.CEO")))
    
    # Process benefits
    df = df.withColumn(
        "benefits_cleaned",
        F.regexp_replace(
            F.regexp_replace(
                F.col("benefits"),
                # Remove opening curly braces and single quotes
                r"\{\\'|\\'\\'\\\\'",  
                ""
            ),
            # Normalize commas by removing spaces after them
            r",\s",  
            ","
        )
    )
    # Count the number of benefits listed
    df = df.withColumn("benefits_count", F.size(F.split(F.col("benefits_cleaned"), ",")))
    
    # Extract common benefits as binary flags
    common_benefits = ["health insurance", 
                       "dental", "vision", "401k", "retirement", 
                       "pto", "paid time off", 
                   "flexible", "remote", "bonus", 
                       "education", "training", "insurance", "life insurance"]

    # Create binary flags for each common benefit
    for benefit in common_benefits:
        df = df.withColumn(
            f"has_{benefit.replace(' ', '_')}",
            F.when(
                F.lower(F.col("benefits")).contains(benefit),
                1
            ).otherwise(0)
        )
    # Count the number of skills and responsibilities listed
    # Count the number of skills and responsibilities listed
    df = df.withColumn("skills_count", F.size(F.split(F.col("skills"), " ")))
    df = df.withColumn("responsibilities_count", F.size(F.split(F.col("responsibilities"), "\\.")))

    # Process job_posting_date
    print("Process job_posting_date")
    df = (df
        .withColumn("job_posting_year", F.year("job_posting_date"))
        .withColumn("job_posting_month", F.month("job_posting_date"))
        .withColumn("job_posting_day", F.dayofmonth("job_posting_date"))
        .withColumn("month_sin", F.sin(2 * math.pi * F.col("job_posting_month") / 12))
        .withColumn("month_cos", F.cos(2 * math.pi * F.col("job_posting_month") / 12))
        .withColumn("day_sin", F.sin(2 * math.pi * F.col("job_posting_day") / 31))
        .withColumn("day_cos", F.cos(2 * math.pi * F.col("job_posting_day") / 31))
    )
    
    # Geospatial processing (ECEF)
    ecef_schema = StructType([
        StructField("x", DoubleType(), False),
        StructField("y", DoubleType(), False),
        StructField("z", DoubleType(), False)
    ])
    
    ecef_udf = F.udf(lat_lon_to_ecef, ecef_schema)
    df = df.withColumn("ecef", ecef_udf("latitude", "longitude")) \
        .withColumn("ecef_x", F.col("ecef.x")) \
        .withColumn("ecef_y", F.col("ecef.y")) \
        .withColumn("ecef_z", F.col("ecef.z"))
    
    # Feature selection
    print("Feature selection")
    numerical_features = [
    'company_size', 'benefits_count', 'skills_count', 'responsibilities_count',
    'job_posting_year', 'experience_min', 'experience_max', 'experience_avg',
    'ecef_x', 'ecef_y', 'ecef_z', 'is_public' ]
    
    categorical_features = [
    'qualifications', 'work_type', 'preference', 'job_portal', 'sector', 
    'industry', 'company_state', 'ceo_gender']
    
    text_features = [
    'job_title', 'role', 'job_description'
    ]

    cyclical_features = [
        'month_sin', 'month_cos', 'day_sin', 'day_cos'
    ]

    binary_features = [col for col in df.columns if col.startswith('has_')]
    
    # Target is salary_avg (average of min and max salary)
    target = 'salary_avg'
        
    # All features to use in the model
    all_features = numerical_features + categorical_features + text_features + cyclical_features + binary_features
    selected_columns = all_features + [target]
    df_selected = df.select(selected_columns)
    
    # Build ML pipeline
    print("Build predprocessing pipeline")
    categorical_indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_indexed", handleInvalid="skip") 
                       for c in categorical_features]
    categorical_encoders = [OneHotEncoder(inputCol=indexer.getOutputCol(), outputCol=f"{indexer.getOutputCol()}_encoded") 
                       for indexer in categorical_indexers]
    
   # Process text features
    tokenizers = [Tokenizer(inputCol=c, outputCol=f"{c}_tokens") for c in text_features]
    remover = [StopWordsRemover(inputCol=tokenizer.getOutputCol(), outputCol=f"{c}_filtered") 
          for tokenizer, c in zip(tokenizers, text_features)]
    count_vectorizers = [CountVectorizer(inputCol=remover[i].getOutputCol(), outputCol=f"{c}_counted", minDF=5.0) 
                    for i, c in enumerate(text_features)]
    idfs = [IDF(inputCol=count_vectorizer.getOutputCol(), outputCol=f"{c}_tfidf") 
       for count_vectorizer, c in  zip(count_vectorizers, text_features)]

    # Get all feature columns after transformations
    transformed_categorical = [encoder.getOutputCol() for encoder in categorical_encoders]
    transformed_text = [idf.getOutputCol() for idf in idfs]
    all_features = numerical_features + cyclical_features + binary_features + transformed_categorical + transformed_text
    
    # First assemble just the numerical features for scaling
    numerical_assembler = VectorAssembler(inputCols=numerical_features, outputCol="numerical_features")
    numerical_scaler = StandardScaler(inputCol="numerical_features", outputCol="scaled_numerical_features", withStd=True, withMean=True)

    # Then assemble all features with the scaled numerical ones
    all_features_for_assembly = ["scaled_numerical_features"] + cyclical_features + transformed_categorical + transformed_text + binary_features
    assembler = VectorAssembler(inputCols=all_features_for_assembly, outputCol="features")
    
    # Create the pipeline stages
    stages = categorical_indexers + categorical_encoders + tokenizers + remover + count_vectorizers + idfs
    stages.append(numerical_assembler)
    stages.append(numerical_scaler)
    stages.append(assembler)
    
    pipeline = Pipeline(stages=stages)
    print(f"Created pipeline with {len(stages)} stages")
    
    # Fit pipeline and transform data
    print("Fit pipeline and transform data")
    pipeline_model = pipeline.fit(df_selected)
    transformed_data = pipeline_model.transform(df_selected)
    ml_data = transformed_data.select("features", F.col(target).alias("label"))
    
    # Split and save data
    print("Split and save data")
    (train_data, test_data) = ml_data.randomSplit([0.7, 0.3], seed=42)
    train_data.repartition(3) \
    .write \
    .mode("overwrite") \
    .format("json") \
    .save("project/data/train")
        
    # Save test data to HDFS in JSON format
    test_data.repartition(2) \
        .write \
        .mode("overwrite") \
        .format("json") \
        .save("project/data/test")
    
    spark.stop()

if __name__ == "__main__":
    main()