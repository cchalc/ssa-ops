-- cjc-asq-evaluation
-- Complete ASQ evaluation with hygiene rules, priority scoring, and urgency classification
-- Parameters: {{ region }} (e.g., CAN, RCT, FINS, MFG, EMEA)
--
-- Hygiene Rules:
--   RULE1_MISSING_NOTES: Assigned >7 days with no status notes
--   RULE3_STALE: Open 30-90 days with past/no due date
--   RULE4_EXPIRED: Due date expired >7 days ago
--   RULE5_EXCESSIVE: Open >90 days
--
-- Urgency Classification:
--   CRITICAL: >14 days overdue
--   HIGH: 7-14 days overdue
--   MEDIUM: 1-7 days overdue or stale notes
--   NORMAL: On track

SELECT
  asq.approval_request_name AS asq_number,
  asq.Title AS asq_title,
  asq.status,
  asq.account_name,
  asq.owner_user_name AS assigned_to,
  asq.technical_specialization,
  asq.support_type,
  asq.region_level_1 AS region,
  asq.business_unit,

  -- Dates
  DATE(asq.created_date) AS request_date,
  DATE(asq.target_end_date) AS due_date,
  DATEDIFF(CURRENT_DATE, asq.created_date) AS days_open,
  CASE
    WHEN asq.target_end_date < CURRENT_DATE
    THEN DATEDIFF(CURRENT_DATE, asq.target_end_date)
    ELSE 0
  END AS days_overdue,

  -- Notes Status
  CASE
    WHEN asq.request_status_notes IS NULL THEN 'NO_NOTES'
    WHEN LENGTH(asq.request_status_notes) < 10 THEN 'NO_NOTES'
    ELSE 'HAS_NOTES'
  END AS notes_status,

  -- Hygiene Status (5-Rule Framework)
  CASE
    WHEN DATEDIFF(CURRENT_DATE, asq.created_date) > 90 THEN 'RULE5_EXCESSIVE'
    WHEN DATEDIFF(CURRENT_DATE, asq.created_date) BETWEEN 30 AND 90
      AND (asq.target_end_date IS NULL OR asq.target_end_date < CURRENT_DATE) THEN 'RULE3_STALE'
    WHEN asq.target_end_date < CURRENT_DATE - INTERVAL 7 DAYS THEN 'RULE4_EXPIRED'
    WHEN DATEDIFF(CURRENT_DATE, asq.created_date) > 7
      AND (asq.request_status_notes IS NULL OR LENGTH(asq.request_status_notes) < 10) THEN 'RULE1_MISSING_NOTES'
    ELSE 'COMPLIANT'
  END AS hygiene_status,

  -- Urgency Classification
  CASE
    WHEN asq.target_end_date < CURRENT_DATE - INTERVAL 14 DAYS THEN 'CRITICAL'
    WHEN asq.target_end_date < CURRENT_DATE - INTERVAL 7 DAYS THEN 'HIGH'
    WHEN asq.target_end_date < CURRENT_DATE THEN 'MEDIUM'
    WHEN DATEDIFF(CURRENT_DATE, asq.created_date) > 14
      AND (asq.request_status_notes IS NULL OR LENGTH(asq.request_status_notes) < 10) THEN 'MEDIUM'
    ELSE 'NORMAL'
  END AS urgency,

  -- Request Details
  LEFT(asq.request_description, 300) AS description_preview,
  LEFT(asq.request_status_notes, 300) AS notes_preview,
  COALESCE(asq.estimated_effort_in_days, 5) AS estimated_days,
  asq.expected_dbu_dollar_impact,

  -- Links
  CONCAT('https://databricks.lightning.force.com/', asq.approval_request_id) AS sf_link

FROM main.gtm_silver.approval_request_detail asq
WHERE asq.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
  AND asq.status IN ('New', 'Submitted', 'Under Review', 'In Progress')
  AND asq.region_level_1 = '{{ region }}'
ORDER BY
  CASE
    WHEN asq.target_end_date < CURRENT_DATE - INTERVAL 14 DAYS THEN 1
    WHEN asq.target_end_date < CURRENT_DATE - INTERVAL 7 DAYS THEN 2
    WHEN asq.target_end_date < CURRENT_DATE THEN 3
    ELSE 4
  END,
  asq.created_date
