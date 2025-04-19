-- Analyze job distribution and average salary ranges by country and work type

SELECT 
    country,
    work_type,
    COUNT(*) as job_count,
    AVG(
        CASE 
            WHEN salary_range RLIKE '\\$?\\d+(K|k)?-\\$?\\d+(K|k)?' THEN (
                CAST(
                    REGEXP_EXTRACT(salary_range, '\\$?(\\d+)(K|k)?-', 1) AS INT
                ) * 
                CASE 
                    WHEN REGEXP_EXTRACT(salary_range, '\\$?(\\d+)(K|k)?-', 2) IN ('K', 'k') THEN 1000
                    ELSE 1
                END
                +
                CAST(
                    REGEXP_EXTRACT(salary_range, '-\\$?(\\d+)(K|k)?', 1) AS INT
                ) * 
                CASE 
                    WHEN REGEXP_EXTRACT(salary_range, '-\\$?(\\d+)(K|k)?', 2) IN ('K', 'k') THEN 1000
                    ELSE 1
                END
            ) / 2
            ELSE NULL
        END
    ) as avg_salary
FROM job_descriptions_part
WHERE country IS NOT NULL 
    AND work_type IS NOT NULL
    AND salary_range IS NOT NULL
GROUP BY country, work_type
HAVING COUNT(*) > 10
ORDER BY job_count DESC, avg_salary DESC;

-- This query provides insights into:
-- Job market size by country
-- Salary trends across different work types
-- Geographic distribution of job opportunities
-- Helps job seekers understand where to find the best opportunities
