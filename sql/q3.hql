-- Analyze benefits offered by company size and work type
SELECT 
    CASE 
        WHEN company_size <= 50 THEN 'Small (1-50)'
        WHEN company_size <= 200 THEN 'Medium (51-200)'
        WHEN company_size <= 1000 THEN 'Large (201-1000)'
        ELSE 'Enterprise (1000+)'
    END as company_category,
    work_type,
    benefit,
    COUNT(*) as offer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(PARTITION BY company_category, work_type), 2) as percentage
FROM (
    SELECT 
        company_size,
        work_type,
        TRIM(benefit) as benefit
    FROM job_descriptions_part
    LATERAL VIEW explode(split(regexp_replace(regexp_replace(benefits, '\\[|\\]', ''), '\\'', ''), ',')) benefits_exploded AS benefit
    WHERE benefits IS NOT NULL 
        AND company_size > 0
        AND work_type IS NOT NULL
) benefits_data
WHERE benefit != ''
GROUP BY company_category, work_type, benefit
HAVING COUNT(*) > 3
ORDER BY company_category, work_type, offer_count DESC;

-- This query provides insights into:
-- Benefits offered by different company sizes
-- How benefits vary by work type
-- Helps job seekers understand what to expect from different employers
-- Guides companies in designing competitive benefits packages