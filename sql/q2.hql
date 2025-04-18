-- Analyze most in-demand skills by work type and experience level
SELECT 
    work_type,
    experience,
    skill,
    COUNT(*) as demand_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(PARTITION BY work_type, experience), 2) as percentage
FROM (
    SELECT 
        work_type,
        experience,
        TRIM(skill) as skill
    FROM job_descriptions_part
    LATERAL VIEW explode(split(regexp_replace(regexp_replace(skills, '\\[|\\]', ''), '\\'', ''), ',')) skills_exploded AS skill
    WHERE skills IS NOT NULL 
        AND work_type IS NOT NULL
        AND experience IS NOT NULL
) skills_data
WHERE skill != ''
GROUP BY work_type, experience, skill
HAVING COUNT(*) > 5
ORDER BY work_type, experience, demand_count DESC;

-- This query helps:
-- Identify which skills are most valuable for different work types
-- Understand skill requirements at different experience levels
-- Guide career development and training decisions
-- Show skill trends in the job market