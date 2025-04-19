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


pivoted_benefits AS (
  SELECT
    benefit,
    MAX(CASE WHEN company_category = 'Small (<= 43k)'      THEN percentage END) AS small_pct,
    MAX(CASE WHEN company_category = 'Medium (43k - 73k)'  THEN percentage END) AS medium_pct,
    MAX(CASE WHEN company_category = 'Large (73k - 104k)'  THEN percentage END) AS large_pct,
    MAX(CASE WHEN company_category = 'Enterprise (> 104k)' THEN percentage END) AS enterprise_pct
  FROM benefit_counts
  GROUP BY benefit
)


SELECT
  benefit,
  ROUND(small_pct, 2)      AS small_pct,
  ROUND(medium_pct, 2)     AS medium_pct,
  ROUND(large_pct, 2)      AS large_pct,
  ROUND(enterprise_pct, 2) AS enterprise_pct
FROM pivoted_benefits
WHERE enterprise_pct IS NOT NULL
ORDER BY enterprise_pct DESC
LIMIT 20;
