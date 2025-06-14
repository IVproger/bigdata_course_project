-- Average salary by country
SELECT 
  country,
  AVG(
    CASE 
      WHEN salary_range RLIKE '\\$?\\d+(K|k)?-\\$?\\d+(K|k)?' THEN (
        CAST(
          REGEXP_EXTRACT(salary_range, '\\$?(\\d+)(K|k)?-', 1) AS INT
        ) * 
        CASE 
          WHEN REGEXP_EXTRACT(salary_range, '\\$?(\\d+)(K|k)?-', 2) IN ('K','k') THEN 1000 
          ELSE 1 
        END
        +
        CAST(
          REGEXP_EXTRACT(salary_range, '-\\$?(\\d+)(K|k)?', 1) AS INT
        ) * 
        CASE 
          WHEN REGEXP_EXTRACT(salary_range, '-\\$?(\\d+)(K|k)?', 2) IN ('K','k') THEN 1000 
          ELSE 1 
        END
      ) / 2
      ELSE NULL
    END
  ) AS avg_salary
FROM job_descriptions_part
WHERE country IS NOT NULL
  AND salary_range IS NOT NULL
GROUP BY country
HAVING COUNT(*) > 10
ORDER BY avg_salary DESC;
