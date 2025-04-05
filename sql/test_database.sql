-- Check the number of rows in the table
SELECT COUNT(*) AS total_rows FROM job_descriptions;

-- View a few sample rows
SELECT * 
FROM job_descriptions 
ORDER BY id 
LIMIT 5;
