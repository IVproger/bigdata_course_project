WITH top_roles AS (
  SELECT 
    role,
    COUNT(*) AS total_jobs
  FROM job_descriptions_part
  WHERE role IS NOT NULL
    AND role NOT LIKE '%Other%'  -- Exclude generic categories
  GROUP BY role
  ORDER BY total_jobs DESC
  LIMIT 5
),


bi_annual_periods AS (
  SELECT 
    role,
    CASE 
      WHEN MONTH(job_posting_date) <= 6 THEN CONCAT(YEAR(job_posting_date), '-H1')
      ELSE CONCAT(YEAR(job_posting_date), '-H2')
    END AS half_year,
    COUNT(*) AS job_count
  FROM job_descriptions_part
  WHERE role IN (SELECT role FROM top_roles)
    AND job_posting_date IS NOT NULL
  GROUP BY 
    role,
    CASE 
      WHEN MONTH(job_posting_date) <= 6 THEN CONCAT(YEAR(job_posting_date), '-H1')
      ELSE CONCAT(YEAR(job_posting_date), '-H2')
    END
)

SELECT 
  tr.role,
  p.half_year,
  COALESCE(bp.job_count, 0) AS job_count,
  ROUND(100 * COALESCE(bp.job_count, 0) / tr.total_jobs, 1) AS percentage_of_total
FROM 
  top_roles tr
CROSS JOIN (
  SELECT DISTINCT half_year 
  FROM (
    SELECT CONCAT(YEAR(job_posting_date), '-H1') AS half_year FROM job_descriptions_part
    UNION
    SELECT CONCAT(YEAR(job_posting_date), '-H2') AS half_year FROM job_descriptions_part
  ) t
  WHERE half_year IS NOT NULL
) p
LEFT JOIN bi_annual_periods bp ON tr.role = bp.role AND p.half_year = bp.half_year
ORDER BY 
  tr.total_jobs DESC,
  p.half_year,
  bp.job_count DESC;
