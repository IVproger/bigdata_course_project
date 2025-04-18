-- Analyze salary ranges by experience level and work type
SELECT 
    experience,
    work_type,
    COUNT(*) as job_count,
    MIN(
        CASE 
            WHEN salary_range ~ '(\d+)-(\d+)' 
            THEN CAST(SPLIT_PART(REGEXP_REPLACE(salary_range, '[^0-9-]', ''), '-', 1) AS INT)
            ELSE NULL
        END
    ) as min_salary,
    MAX(
        CASE 
            WHEN salary_range ~ '(\d+)-(\d+)' 
            THEN CAST(SPLIT_PART(REGEXP_REPLACE(salary_range, '[^0-9-]', ''), '-', 2) AS INT)
            ELSE NULL
        END
    ) as max_salary,
    ROUND(AVG(
        CASE 
            WHEN salary_range ~ '(\d+)-(\d+)' 
            THEN (CAST(SPLIT_PART(REGEXP_REPLACE(salary_range, '[^0-9-]', ''), '-', 1) AS INT) + 
                  CAST(SPLIT_PART(REGEXP_REPLACE(salary_range, '[^0-9-]', ''), '-', 2) AS INT)) / 2
            ELSE NULL
        END
    ), 0) as avg_salary
FROM job_descriptions_part
WHERE experience IS NOT NULL 
    AND work_type IS NOT NULL
    AND salary_range IS NOT NULL
GROUP BY experience, work_type
HAVING COUNT(*) > 5
ORDER BY experience, work_type;

-- This query provides:
-- Clear salary expectations by experience level
-- Salary ranges for different work types
-- Helps job seekers negotiate salaries
-- Guides career progression planning