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

CREATE EXTERNAL TABLE model1_predictions (
    job_id STRING,
    id STRING, -- Assuming this is an ID
    experience STRING,
    qualifications STRING,
    salary_range STRING,
    location STRING, -- Might be city or a combined field
    country STRING,
    latitude DECIMAL(9,6), -- Adjust precision/scale if needed
    longitude DECIMAL(9,6), -- Adjust precision/scale if needed
    company_size STRING,
    job_posting_date DATE, -- Assuming YYYY-MM-DD format
    contact_person STRING,
    preference STRING,
    contact STRING,
    job_title STRING,
    role STRING,
    job_portal STRING,
    job_description STRING,
    benefits STRING,
    skills STRING,
    responsibilities STRING,
    company_name STRING,
    company_profile STRING,
    work_type STRING,
    original_salary DOUBLE, -- Changed back to DOUBLE
    predicted_salary DOUBLE  -- Changed back to DOUBLE
)

ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar'     = '"',
    'escapeChar'    = '\\'
)
STORED AS TEXTFILE
LOCATION 'project/output/model1_predictions.csv'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE model2_predictions (
    job_id STRING,
    id STRING,
    experience STRING,
    qualifications STRING,
    salary_range STRING,
    location STRING,
    country STRING,
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    company_size STRING,
    job_posting_date DATE,
    contact_person STRING,
    preference STRING,
    contact STRING,
    job_title STRING,
    role STRING,
    job_portal STRING,
    job_description STRING,
    benefits STRING,
    skills STRING,
    responsibilities STRING,
    company_name STRING,
    company_profile STRING,
    work_type STRING,
    original_salary DOUBLE,
    predicted_salary DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar'     = '"',
    'escapeChar'    = '\\'
)
STORED AS TEXTFILE
LOCATION 'project/output/model2_predictions.csv'
TBLPROPERTIES ('skip.header.line.count'='1');

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