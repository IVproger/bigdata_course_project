-- Query 5: Top 10 job titles and their most common qualification
WITH top_job_titles AS (
  SELECT 
    job_title,
    COUNT(*) AS post_count
  FROM job_descriptions_part
  WHERE job_title IS NOT NULL
  GROUP BY job_title
  ORDER BY post_count DESC
  LIMIT 10
),

title_qualifications AS (
  SELECT 
    job_title,
    qualifications,
    COUNT(*) AS qual_count
  FROM job_descriptions_part
  WHERE job_title IS NOT NULL AND qualifications IS NOT NULL
  GROUP BY job_title, qualifications
),

most_common_qual AS (
  SELECT 
    job_title,
    qualifications,
    qual_count,
    ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY qual_count DESC) AS rn
  FROM title_qualifications
)

SELECT 
  tjt.job_title,
  tjt.post_count,
  mcq.qualifications AS most_common_qualification
FROM top_job_titles tjt
LEFT JOIN most_common_qual mcq 
  ON tjt.job_title = mcq.job_title AND mcq.rn = 1
ORDER BY tjt.post_count DESC;
