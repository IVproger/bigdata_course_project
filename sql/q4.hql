-- This query counts the top 20 most frequent job titles.
SELECT 
    job_title, 
    COUNT(*) AS job_count
FROM job_descriptions_part -- Changed from job_descriptions
WHERE job_title IS NOT NULL AND job_title != ''
GROUP BY job_title
ORDER BY job_count DESC
LIMIT 20; 