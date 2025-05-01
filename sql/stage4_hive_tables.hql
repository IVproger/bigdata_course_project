-- HiveQL script to create external tables for Stage III results and KL divergence

-- Use the project database
USE team14_projectdb;

-- Drop tables if they exist (for idempotency)
DROP TABLE IF EXISTS evaluation_results;
DROP TABLE IF EXISTS model1_predictions;
DROP TABLE IF EXISTS model2_predictions;
DROP TABLE IF EXISTS kl_divergence;
DROP TABLE IF EXISTS lr_tuning_results;
DROP TABLE IF EXISTS gbt_tuning_results;

-- Create external table for model evaluation results
CREATE EXTERNAL TABLE evaluation_results (
    Model_Type STRING,
    RMSE DOUBLE,
    R2 DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',')
STORED AS TEXTFILE
LOCATION 'project/output/evaluation.csv'
TBLPROPERTIES ('skip.header.line.count'='1'); -- Skip the header row

-- UPDATED: Create external table for Model 1 (Linear Regression) enriched predictions
CREATE EXTERNAL TABLE model1_predictions (
    job_id STRING, -- Assuming BIGINT originally, cast to STRING in Spark output
    job_title STRING,
    company_name STRING,
    company_profile STRING, -- Contains JSON-like structure
    job_description STRING,
    requirements STRING,
    benefits STRING,
    telecommuting BOOLEAN,
    has_company_logo BOOLEAN,
    has_questions BOOLEAN,
    employment_type STRING,
    required_experience STRING,
    required_education STRING,
    industry STRING,
    `function` STRING,
    fraudulent BOOLEAN,
    country STRING,
    city STRING,
    zip_code STRING,
    department STRING,
    salary_range STRING,
    company_size STRING, -- e.g., "10000+ employees"
    job_board STRING,
    geo STRING,
    job_posting_date DATE,
    last_processed_time STRING, -- Assuming this is a timestamp string
    employment_status STRING,
    experience STRING,
    job_functions STRING,
    job_industries STRING,
    locations STRING,
    salary_currency STRING,
    salary_period STRING,
    skills STRING,
    work_type STRING, -- Partition key in original table
    preference STRING, -- Bucketing key in original table
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    -- Added prediction columns
    original_salary DOUBLE,
    predicted_salary DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',', 'serialization.null.format' = '') -- Handle potential nulls
STORED AS TEXTFILE
LOCATION 'project/output/model1_predictions.csv'
TBLPROPERTIES ('skip.header.line.count'='1'); -- Skip the header row

-- UPDATED: Create external table for Model 2 (GBT Regressor) enriched predictions
CREATE EXTERNAL TABLE model2_predictions (
    job_id STRING, -- Assuming BIGINT originally, cast to STRING in Spark output
    job_title STRING,
    company_name STRING,
    company_profile STRING, -- Contains JSON-like structure
    job_description STRING,
    requirements STRING,
    benefits STRING,
    telecommuting BOOLEAN,
    has_company_logo BOOLEAN,
    has_questions BOOLEAN,
    employment_type STRING,
    required_experience STRING,
    required_education STRING,
    industry STRING,
    `function` STRING,
    fraudulent BOOLEAN,
    country STRING,
    city STRING,
    zip_code STRING,
    department STRING,
    salary_range STRING,
    company_size STRING, -- e.g., "10000+ employees"
    job_board STRING,
    geo STRING,
    job_posting_date DATE,
    last_processed_time STRING, -- Assuming this is a timestamp string
    employment_status STRING,
    experience STRING,
    job_functions STRING,
    job_industries STRING,
    locations STRING,
    salary_currency STRING,
    salary_period STRING,
    skills STRING,
    work_type STRING, -- Partition key in original table
    preference STRING, -- Bucketing key in original table
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    -- Added prediction columns
    original_salary DOUBLE,
    predicted_salary DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',', 'serialization.null.format' = '') -- Handle potential nulls
STORED AS TEXTFILE
LOCATION 'project/output/model2_predictions.csv'
TBLPROPERTIES ('skip.header.line.count'='1'); -- Skip the header row

-- Create external table for KL Divergence results (to be created by PySpark script)
CREATE EXTERNAL TABLE kl_divergence (
    model_type STRING,
    kl_divergence DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',')
STORED AS TEXTFILE
LOCATION 'project/output/kl_divergence.csv'
TBLPROPERTIES ('skip.header.line.count'='1'); -- Skip the header row

-- ADDED: Table for Linear Regression Tuning Results
CREATE EXTERNAL TABLE lr_tuning_results (
    aggregationDepth INT, -- Assuming Spark saves this as INT
    avgRMSE DOUBLE,
    elasticNetParam DOUBLE,
    regParam DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'project/output/lr_tuning_results.csv'
TBLPROPERTIES ('skip.header.line.count'='1');

-- ADDED: Table for GBT Regressor Tuning Results
CREATE EXTERNAL TABLE gbt_tuning_results (
    avgRMSE DOUBLE,
    maxDepth INT, -- Assuming Spark saves this as INT
    maxIter INT, -- Assuming Spark saves this as INT
    stepSize DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'project/output/gbt_tuning_results.csv'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Verify table creation
SHOW TABLES; 