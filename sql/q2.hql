USE team14_projectdb;

-- Drop the results table if it exists
DROP TABLE IF EXISTS q2_results;

-- Create a table to store the results
CREATE EXTERNAL TABLE q2_results(
    work_type STRING,
    job_count BIGINT,
    percentage DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q2';

-- Set to not display table names with column names
SET hive.resultset.use.unique.column.names = false;

-- Insert the results of the query
INSERT INTO q2_results
SELECT 
    work_type, 
    COUNT(*) AS job_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM job_descriptions
WHERE work_type IS NOT NULL AND work_type != ''
GROUP BY work_type
ORDER BY job_count DESC;

-- Display the results
SELECT * FROM q2_results;

-- Export the results to a file
INSERT OVERWRITE DIRECTORY 'project/output/q2' 
ROW FORMAT DELIMITED FIELDS 
TERMINATED BY ',' 
SELECT * FROM q2_results; 