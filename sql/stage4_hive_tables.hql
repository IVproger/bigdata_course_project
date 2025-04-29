-- HiveQL script to create external tables for Stage III results and KL divergence

-- Use the project database
USE team14_projectdb;

-- Drop tables if they exist (for idempotency)
DROP TABLE IF EXISTS evaluation_results;
DROP TABLE IF EXISTS model1_predictions;
DROP TABLE IF EXISTS model2_predictions;
DROP TABLE IF EXISTS kl_divergence;

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

-- Create external table for Model 1 (Linear Regression) predictions
CREATE EXTERNAL TABLE model1_predictions (
    label DOUBLE,
    prediction DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',')
STORED AS TEXTFILE
LOCATION 'project/output/model1_predictions.csv'
TBLPROPERTIES ('skip.header.line.count'='1'); -- Skip the header row

-- Create external table for Model 2 (GBT Regressor) predictions
CREATE EXTERNAL TABLE model2_predictions (
    label DOUBLE,
    prediction DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',')
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

-- Verify table creation
SHOW TABLES; 