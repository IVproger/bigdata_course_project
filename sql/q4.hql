-- Query 4 Categories by job count
SELECT
  t.job_category,
  COUNT(*) AS job_count,
  ROUND(
    AVG(
      CASE
        WHEN salary_range RLIKE '\\$?\\d+(K|k)?-\\$?\\d+(K|k)?' THEN (
          -- lower bound in dollars
          CAST(
            REGEXP_EXTRACT(salary_range, '\\$?(\\d+)(K|k)?-', 1)
            AS INT
          )
          *
          CASE
            WHEN REGEXP_EXTRACT(salary_range, '\\$?(\\d+)(K|k)?-', 2) IN ('K','k') THEN 1000
            ELSE 1
          END
          +
          -- upper bound in dollars
          CAST(
            REGEXP_EXTRACT(salary_range, '-\\$?(\\d+)(K|k)?', 1)
            AS INT
          )
          *
          CASE
            WHEN REGEXP_EXTRACT(salary_range, '-\\$?(\\d+)(K|k)?', 2) IN ('K','k') THEN 1000
            ELSE 1
          END
        ) / 2   -- midpoint
        ELSE NULL
      END
    ),
    0
  ) AS avg_salary_usd
FROM (
  SELECT
    *,
    -- categorize by job_title
    CASE
      WHEN lower(job_title) LIKE '%software%'   OR lower(job_title) LIKE '%developer%'  OR lower(job_title) LIKE '%engineer%'   THEN 'Technology'
      WHEN lower(job_title) LIKE '%data%'       OR lower(job_title) LIKE '%analyst%'                                                     THEN 'Data & Analytics'
      WHEN lower(job_title) LIKE '%marketing%'  OR lower(job_title) LIKE '%media%'     OR lower(job_title) LIKE '%brand%'        THEN 'Marketing & Communications'
      WHEN lower(job_title) LIKE '%nurse%'      OR lower(job_title) LIKE '%therapist%' OR lower(job_title) LIKE '%practitioner%' THEN 'Healthcare'
      WHEN lower(job_title) LIKE '%admin%'      OR lower(job_title) LIKE '%assistant%' OR lower(job_title) LIKE '%coordinator%' THEN 'Administration'
      WHEN lower(job_title) LIKE '%finance%'    OR lower(job_title) LIKE '%accountant%' OR lower(job_title) LIKE '%banker%'     THEN 'Finance'
      WHEN lower(job_title) LIKE '%manager%'    OR lower(job_title) LIKE '%project%'   OR lower(job_title) LIKE '%operations%'   THEN 'Management'
      WHEN lower(job_title) LIKE '%support%'    OR lower(job_title) LIKE '%customer%'  OR lower(job_title) LIKE '%service%'      THEN 'Customer Service'
      ELSE 'Other'
    END AS job_category
  FROM job_descriptions_part
  WHERE salary_range IS NOT NULL
    AND job_title    IS NOT NULL
) t
GROUP BY t.job_category
ORDER BY job_count DESC;
