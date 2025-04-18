-- Round latitude and longitude for meaningful clustering
SELECT 
  ROUND(latitude, 2) AS latitude,
  ROUND(longitude, 2) AS longitude,
  COUNT(*) AS job_count
FROM jobs
WHERE latitude IS NOT NULL AND longitude IS NOT NULL
GROUP BY ROUND(latitude, 2), ROUND(longitude, 2)
ORDER BY job_count DESC;
