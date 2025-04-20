-- Query 2: Top10 roles by sex preference
WITH gender_roles AS (
  SELECT
    CASE
      WHEN lower(preference) LIKE '%female%' THEN 'Female'
      WHEN lower(preference) LIKE '%male%'   THEN 'Male'
      ELSE 'Both'
    END AS gender,
    role
  FROM job_descriptions_part
  WHERE preference IS NOT NULL
    AND role IS NOT NULL
),
role_counts AS (
  SELECT
    gender,
    role,
    COUNT(*) AS role_count,
    ROW_NUMBER() OVER (PARTITION BY gender ORDER BY COUNT(*) DESC) AS rn
  FROM gender_roles
  GROUP BY gender, role
)
SELECT
  gender,
  role,
  role_count
FROM role_counts
WHERE rn <= 10
ORDER BY gender, role_count DESC;
