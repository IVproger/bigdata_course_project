-- Drop the database if it exists
DROP DATABASE IF EXISTS team14_projectdb CASCADE;

-- Create a database and access it
CREATE DATABASE team14_projectdb LOCATION "project/hive/warehouse";
USE team14_projectdb;

-- Create a single table with partitioning and bucketing
CREATE EXTERNAL TABLE job_descriptions 
STORED AS AVRO 
LOCATION 'project/warehouse/job_descriptions' 
TBLPROPERTIES ('avro.schema.url'='project/warehouse/avsc/job_descriptions.avsc');

-- Enable dynamic partitioning
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

SET hive.enforce.bucketing = true;

CREATE EXTERNAL TABLE job_descriptions_part (
    id INT,
    job_id BIGINT,
    experience STRING,
    qualifications STRING,
    salary_range STRING,
    location STRING,
    country STRING,
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    company_size INT,
    job_posting_date date,
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
    company_profile STRING
)
PARTITIONED BY (work_type STRING)
CLUSTERED BY (preference) INTO 3 BUCKETS
STORED AS AVRO 
LOCATION 'project/hive/warehouse/job_descriptions_part' 
TBLPROPERTIES ('AVRO.COMPRESS'='SNAPPY');

INSERT INTO job_descriptions_part PARTITION (work_type)
SELECT
    id,
    job_id,
    experience,
    qualifications,
    salary_range,
    location,
    country,
    CAST(latitude AS DECIMAL(9,6)) AS latitude,
    CAST(longitude AS DECIMAL(9,6)) AS longitude,
    company_size,
    CAST(
        FROM_UNIXTIME(
            CAST(job_posting_date / 1000 AS BIGINT)  -- Convert milliseconds to seconds as BIGINT
        ) AS DATE
    ) AS job_posting_date,
    contact_person,
    preference,
    contact,
    job_title,
    role,
    job_portal,
    job_description,
    benefits,
    skills,
    responsibilities,
    company_name,
    company_profile,
    work_type  -- Must be last (partition column)
FROM job_descriptions;

-- Delete the unpartitioned Hive TABLE job_descriptions
DROP TABLE job_descriptions;

-- Verify the table was created correctly
SHOW TABLES;

-- Check the partitions in the table
SHOW PARTITIONS job_descriptions_part;

-- Check the bucketing in the table 
SELECT 
  preference,
  INPUT__FILE__NAME AS file,
  COUNT(*) AS count
FROM job_descriptions_part
WHERE work_type = 'Contract'
GROUP BY preference, INPUT__FILE__NAME;

-- Sample query to verify data
SELECT COUNT(*) FROM job_descriptions_part;

-- Sample query to check data distribution by work type
SELECT work_type, COUNT(*) as job_count 
FROM job_descriptions_part 
GROUP BY work_type 
ORDER BY job_count DESC; 