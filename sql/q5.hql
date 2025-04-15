-- This query extracts the top 30 most frequent skills mentioned in job descriptions.
SELECT 
    skill, 
    COUNT(*) AS job_count
FROM (
    SELECT 
        explode(split(regexp_replace(regexp_replace(skills, '\\[|\\]', ''), '\\'', ''), ',')) AS skill
    FROM job_descriptions_part -- Changed from job_descriptions
    WHERE skills IS NOT NULL AND skills != ''
) skills_exploded
WHERE skill != ''
GROUP BY skill
ORDER BY job_count DESC
LIMIT 30; 