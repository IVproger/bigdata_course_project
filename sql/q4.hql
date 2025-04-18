-- Analyze job posting trends by month and work type
SELECT 
    DATE_TRUNC('month', job_posting_date) as month,
    work_type,
    COUNT(*) as job_count,
    COUNT(DISTINCT company_name) as unique_companies,
    ROUND(AVG(company_size), 0) as avg_company_size
FROM job_descriptions_part
WHERE job_posting_date >= DATE_SUB(CURRENT_DATE, 365)
    AND work_type IS NOT NULL
    AND company_size > 0
GROUP BY DATE_TRUNC('month', job_posting_date), work_type
ORDER BY month DESC, job_count DESC;

-- This query helps:
-- Track job market trends over time
-- Identify seasonal patterns in hiring
-- Understand which work types are growing
-- Monitor company participation in the job market