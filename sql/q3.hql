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

ranked_by_category AS (
  SELECT
    company_category,
    benefit,
    offer_count,
    percentage,
    ROW_NUMBER() OVER (
      PARTITION BY company_category
      ORDER BY offer_count DESC
    ) AS rn
  FROM benefit_counts
)

SELECT
  company_category,
  benefit,
  offer_count,
  percentage
FROM ranked_by_category
WHERE rn <= 5
ORDER BY company_category, offer_count DESC;
