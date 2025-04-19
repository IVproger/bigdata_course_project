WITH benefit_counts AS (
  SELECT

    CASE
      WHEN company_size <= 43114     THEN 'Small (<= 43k)'
      WHEN company_size <= 73633     THEN 'Medium (43k - 73k)'
      WHEN company_size <= 104300    THEN 'Large (73k - 104k)'
      ELSE                               'Enterprise (> 104k)'
    END                                               AS company_category,
    work_type,
    TRIM(benefit)                                     AS benefit,
    COUNT(*)                                          AS offer_count,
    ROUND(
      COUNT(*) * 100.0 / 
      SUM(COUNT(*)) OVER (
        PARTITION BY
          CASE
            WHEN company_size <= 43114     THEN 'Small (<= 43k)'
            WHEN company_size <= 73633     THEN 'Medium (43k - 73k)'
            WHEN company_size <= 104300    THEN 'Large (73k - 104k)'
            ELSE                               'Enterprise (> 104k)'
          END,
          work_type
      ),
      2
    )                                                 AS percentage
  FROM job_descriptions_part


  LATERAL VIEW explode(
    split(
      regexp_replace(
        benefits,
        '[\\[\\]\\{\\}\\\']',
        ''
      ),
      ','
    )
  ) b AS benefit

  WHERE benefits IS NOT NULL
    AND company_size > 0
    AND work_type IS NOT NULL
    AND TRIM(benefit) <> ''

  GROUP BY
    CASE
      WHEN company_size <= 43114     THEN 'Small (<= 43k)'
      WHEN company_size <= 73633     THEN 'Medium (43k - 73k)'
      WHEN company_size <= 104300    THEN 'Large (73k - 104k)'
      ELSE                               'Enterprise (> 104k)'
    END,
    work_type,
    TRIM(benefit)

  HAVING COUNT(*) > 3
),

ranked_by_work AS (
  SELECT
    work_type,
    benefit,
    offer_count,
    percentage,
    ROW_NUMBER() OVER (
      PARTITION BY work_type
      ORDER BY percentage DESC
    ) AS rn
  FROM benefit_counts
)

SELECT
  work_type,
  benefit,
  offer_count,
  percentage
FROM ranked_by_work
WHERE rn <= 5
ORDER BY work_type, percentage DESC;
