USE team14_projectdb;

-- Drop the results table if it exists
DROP TABLE IF EXISTS q5_results;

-- Create a table to store the results
CREATE EXTERNAL TABLE q5_results(
    skill STRING,
    job_count BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q5';

-- Set to not display table names with column names
SET hive.resultset.use.unique.column.names = false;

-- Insert the results of the query
-- This query extracts skills from the skills column and counts their occurrences
INSERT INTO q5_results
SELECT 
    skill, 
    COUNT(*) AS job_count
FROM (
    SELECT 
        explode(split(regexp_replace(regexp_replace(skills, '\\[|\\]', ''), '\\'', ''), ',')) AS skill
    FROM job_descriptions
    WHERE skills IS NOT NULL AND skills != ''
) skills_exploded
WHERE skill != ''
GROUP BY skill
ORDER BY job_count DESC
LIMIT 30;

-- Display the results
SELECT * FROM q5_results;

-- Export the results to a file
INSERT OVERWRITE DIRECTORY 'project/output/q5' 
ROW FORMAT DELIMITED FIELDS 
TERMINATED BY ',' 
SELECT * FROM q5_results; 