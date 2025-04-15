USE team14_projectdb;

-- Drop the results table if it exists
DROP TABLE IF EXISTS q4_results;

-- Create a table to store the results
CREATE EXTERNAL TABLE q4_results(
    job_title STRING,
    job_count BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q4';

-- Set to not display table names with column names
SET hive.resultset.use.unique.column.names = false;

-- Insert the results of the query
INSERT INTO q4_results
SELECT 
    job_title, 
    COUNT(*) AS job_count
FROM job_descriptions
WHERE job_title IS NOT NULL AND job_title != ''
GROUP BY job_title
ORDER BY job_count DESC
LIMIT 20;

-- Display the results
SELECT * FROM q4_results;

-- Export the results to a file
INSERT OVERWRITE DIRECTORY 'project/output/q4' 
ROW FORMAT DELIMITED FIELDS 
TERMINATED BY ',' 
SELECT * FROM q4_results; 