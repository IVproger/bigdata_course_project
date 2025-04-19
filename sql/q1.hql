-- Analyze job distribution and average salary ranges by country and work type
SELECT 
    country,
    work_type,
    COUNT(*) as job_count,
    AVG(
        CASE 
            WHEN salary_range RLIKE '\\d+-\\d+' 
            THEN (
		CAST(SPLIT(REGEXP_REPLACE(salary_range, '[^0-9-]', ''), '-')[0] AS INT) +
		CAST(SPLIT(REGEXP_REPLACE(salary_range, '[^0-9-]', ''), '-')[1] AS INT)
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
