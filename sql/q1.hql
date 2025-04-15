-- This query extracts the top 10 countries by job posting count.
SELECT
    country,
    COUNT(*) AS job_count
FROM job_descriptions_part
GROUP BY country
ORDER BY job_count DESC
LIMIT 10;