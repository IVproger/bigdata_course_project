USE team14_projectdb;

-- Drop the results table if it exists
DROP TABLE IF EXISTS q3_results;

-- Create a table to store the results
CREATE EXTERNAL TABLE q3_results(
    company_size_range STRING,
    job_count BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q3';

-- Set to not display table names with column names
SET hive.resultset.use.unique.column.names = false;

-- Insert the results of the query
INSERT INTO q3_results
SELECT 
    CASE 
        WHEN company_size <= 50 THEN '1-50'
        WHEN company_size <= 200 THEN '51-200'
        WHEN company_size <= 1000 THEN '201-1000'
        WHEN company_size <= 5000 THEN '1001-5000'
        ELSE '5000+'
    END AS company_size_range,
    COUNT(*) AS job_count
FROM job_descriptions
WHERE company_size > 0
GROUP BY 
    CASE 
        WHEN company_size <= 50 THEN '1-50'
        WHEN company_size <= 200 THEN '51-200'
        WHEN company_size <= 1000 THEN '201-1000'
        WHEN company_size <= 5000 THEN '1001-5000'
        ELSE '5000+'
    END
ORDER BY 
    CASE company_size_range
        WHEN '1-50' THEN 1
        WHEN '51-200' THEN 2
        WHEN '201-1000' THEN 3
        WHEN '1001-5000' THEN 4
        WHEN '5000+' THEN 5
    END;

-- Display the results
SELECT * FROM q3_results;

-- Export the results to a file
INSERT OVERWRITE DIRECTORY 'project/output/q3' 
ROW FORMAT DELIMITED FIELDS 
TERMINATED BY ',' 
SELECT * FROM q3_results; 