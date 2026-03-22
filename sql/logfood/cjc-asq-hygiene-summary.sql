-- cjc-asq-hygiene-summary
-- Summary of ASQ hygiene status by region and owner
-- Parameters: {{ region }} (e.g., CAN, RCT, FINS, MFG)
--
-- Aggregates hygiene rule violations to identify:
-- - SSAs with the most hygiene issues
-- - Regions with systemic problems
-- - Overall compliance rate

WITH asq_hygiene AS (
  SELECT
    asq.approval_request_name AS asq_number,
    asq.owner_user_name,
    asq.region_level_1,
    asq.business_unit,
    DATEDIFF(CURRENT_DATE, asq.created_date) AS days_open,
    CASE
      WHEN asq.target_end_date < CURRENT_DATE
      THEN DATEDIFF(CURRENT_DATE, asq.target_end_date)
      ELSE 0
    END AS days_overdue,
    CASE
      WHEN asq.request_status_notes IS NULL OR LENGTH(asq.request_status_notes) < 10 THEN true
      ELSE false
    END AS has_no_notes,
    CASE
      WHEN DATEDIFF(CURRENT_DATE, asq.created_date) > 90 THEN 'RULE5_EXCESSIVE'
      WHEN DATEDIFF(CURRENT_DATE, asq.created_date) BETWEEN 30 AND 90
        AND (asq.target_end_date IS NULL OR asq.target_end_date < CURRENT_DATE) THEN 'RULE3_STALE'
      WHEN asq.target_end_date < CURRENT_DATE - INTERVAL 7 DAYS THEN 'RULE4_EXPIRED'
      WHEN DATEDIFF(CURRENT_DATE, asq.created_date) > 7
        AND (asq.request_status_notes IS NULL OR LENGTH(asq.request_status_notes) < 10) THEN 'RULE1_MISSING_NOTES'
      ELSE 'COMPLIANT'
    END AS hygiene_status
  FROM main.gtm_silver.approval_request_detail asq
  WHERE asq.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
    AND asq.status IN ('New', 'Submitted', 'Under Review', 'In Progress')
    AND asq.region_level_1 = '{{ region }}'
)
SELECT
  owner_user_name AS ssa,
  region_level_1 AS region,
  business_unit,
  COUNT(*) AS total_asqs,
  SUM(CASE WHEN hygiene_status = 'COMPLIANT' THEN 1 ELSE 0 END) AS compliant,
  SUM(CASE WHEN hygiene_status = 'RULE1_MISSING_NOTES' THEN 1 ELSE 0 END) AS missing_notes,
  SUM(CASE WHEN hygiene_status = 'RULE3_STALE' THEN 1 ELSE 0 END) AS stale,
  SUM(CASE WHEN hygiene_status = 'RULE4_EXPIRED' THEN 1 ELSE 0 END) AS expired,
  SUM(CASE WHEN hygiene_status = 'RULE5_EXCESSIVE' THEN 1 ELSE 0 END) AS excessive,
  SUM(CASE WHEN days_overdue > 0 THEN 1 ELSE 0 END) AS overdue,
  ROUND(100.0 * SUM(CASE WHEN hygiene_status = 'COMPLIANT' THEN 1 ELSE 0 END) / COUNT(*), 1) AS compliance_pct,
  AVG(days_open) AS avg_days_open,
  MAX(days_overdue) AS max_days_overdue
FROM asq_hygiene
GROUP BY owner_user_name, region_level_1, business_unit
ORDER BY
  SUM(CASE WHEN hygiene_status != 'COMPLIANT' THEN 1 ELSE 0 END) DESC,
  owner_user_name
