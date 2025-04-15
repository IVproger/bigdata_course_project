-- This query categorizes companies by size and counts jobs in each category.
SELECT 
    CASE 
        WHEN company_size <= 50 THEN '1-50'
        WHEN company_size <= 200 THEN '51-200'
        WHEN company_size <= 1000 THEN '201-1000'
        WHEN company_size <= 5000 THEN '1001-5000'
        ELSE '5000+'
    END AS company_size_range,
    COUNT(*) AS job_count
FROM job_descriptions_part
WHERE company_size > 0
GROUP BY 
    CASE 
        WHEN company_size <= 50 THEN '1-50'
        WHEN company_size <= 200 THEN '51-200'
        WHEN company_size <= 1000 THEN '201-1000'
        WHEN company_size <= 5000 THEN '1001-5000'
        ELSE '5000+'
    END
ORDER BY 
    CASE company_size_range
        WHEN '1-50' THEN 1
        WHEN '51-200' THEN 2
        WHEN '201-1000' THEN 3
        WHEN '1001-5000' THEN 4
        WHEN '5000+' THEN 5
    END;