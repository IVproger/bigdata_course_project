-- Query 3: Monthly aggregated job posting count
SELECT 
    FROM_UNIXTIME(UNIX_TIMESTAMP(`job_posting_date`, 'yyyy-MM-dd'), 'yyyy-MM') AS month,
    COUNT(*) AS job_posting_count
FROM job_descriptions_part
WHERE job_posting_date IS NOT NULL
GROUP BY FROM_UNIXTIME(UNIX_TIMESTAMP(`job_posting_date`, 'yyyy-MM-dd'), 'yyyy-MM')
ORDER BY month;
