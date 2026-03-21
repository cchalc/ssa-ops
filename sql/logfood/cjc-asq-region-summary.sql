-- cjc-asq-region-summary
-- Summary of open ASQs by region
-- No parameters - shows all regions
--
-- Quick overview of ASQ distribution across regions:
-- - Total open ASQs
-- - Overdue percentage
-- - Average days open

SELECT
  asq.region_level_1 AS region,
  asq.business_unit,
  COUNT(*) AS total_asqs,
  SUM(CASE WHEN asq.target_end_date < CURRENT_DATE THEN 1 ELSE 0 END) AS overdue,
  ROUND(100.0 * SUM(CASE WHEN asq.target_end_date < CURRENT_DATE THEN 1 ELSE 0 END) / COUNT(*), 1) AS overdue_pct,
  ROUND(AVG(DATEDIFF(CURRENT_DATE, asq.created_date)), 0) AS avg_days_open,
  MAX(DATEDIFF(CURRENT_DATE, asq.created_date)) AS max_days_open,
  SUM(CASE WHEN asq.request_status_notes IS NULL OR LENGTH(asq.request_status_notes) < 10 THEN 1 ELSE 0 END) AS no_notes,
  COUNT(DISTINCT asq.owner_user_id) AS unique_owners

FROM main.gtm_silver.approval_request_detail asq
WHERE asq.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
  AND asq.status IN ('New', 'Submitted', 'Under Review', 'In Progress')
GROUP BY asq.region_level_1, asq.business_unit
ORDER BY COUNT(*) DESC
