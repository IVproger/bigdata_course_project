USE team14_projectdb;

-- Drop the results table if it exists
DROP TABLE IF EXISTS q1_results;

-- Create a table to store the results
CREATE EXTERNAL TABLE q1_results(
    country STRING,
    job_count BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q1';

-- Set to not display table names with column names
SET hive.resultset.use.unique.column.names = false;

-- Insert the results of the query
INSERT INTO q1_results
SELECT 
    country, 
    COUNT(*) AS job_count
FROM job_descriptions
GROUP BY country
ORDER BY job_count DESC
LIMIT 10;

-- Display the results
SELECT * FROM q1_results;

-- Export the results to a file
INSERT OVERWRITE DIRECTORY 'project/output/q1' 
ROW FORMAT DELIMITED FIELDS 
TERMINATED BY ',' 
SELECT * FROM q1_results; 