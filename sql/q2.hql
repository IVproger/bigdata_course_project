-- This query counts jobs by work type and calculates the percentage.
SELECT 
    work_type, 
    COUNT(*) AS job_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM job_descriptions_part
WHERE work_type IS NOT NULL AND work_type != ''
GROUP BY work_type
ORDER BY job_count DESC;