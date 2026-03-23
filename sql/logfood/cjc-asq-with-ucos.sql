-- cjc-asq-with-ucos
-- ASQs with linked Use Case Opportunities (UCOs) and pipeline value
-- Parameters: {{ region }} (e.g., CAN, RCT, FINS, MFG)
--
-- Links ASQs to UCOs on the same account to show:
-- - Pipeline value (estimated monthly DBUs)
-- - UCO stage (U1-U6)
-- - Competitive status
-- - Near-win opportunities

WITH region_asqs AS (
  SELECT
    asq.approval_request_id AS asq_id,
    asq.approval_request_name AS asq_number,
    asq.Title AS asq_title,
    asq.status,
    asq.account_id,
    asq.account_name,
    asq.owner_user_name,
    asq.technical_specialization,
    asq.created_date,
    asq.target_end_date,
    asq.request_description,
    asq.region_level_1
  FROM main.gtm_silver.approval_request_detail asq
  WHERE asq.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
    AND asq.region_level_1 = '{{ region }}'
    AND asq.status IN ('New', 'Submitted', 'Under Review', 'In Progress')
),
uco_data AS (
  SELECT
    uco.account_id,
    uco.usecase_id,
    uco.usecase_name,
    uco.stage,
    uco.estimated_monthly_dollar_dbus,
    uco.competitors,
    uco.primary_competitor,
    uco.competitor_category,
    uco.target_onboarding_month,
    uco.type AS uco_type
  FROM main.gtm_silver.use_case_detail uco
  WHERE uco.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.use_case_detail)
    AND uco.is_active_ind = true
)
SELECT
  a.asq_number,
  a.asq_title,
  a.status AS asq_status,
  a.account_name,
  a.owner_user_name AS ssa,
  a.technical_specialization,
  a.region_level_1 AS region,
  DATE(a.created_date) AS asq_created,
  DATE(a.target_end_date) AS asq_due,

  -- UCO details
  u.usecase_name AS uco_name,
  u.stage AS uco_stage,
  u.estimated_monthly_dollar_dbus AS monthly_dbu,
  u.competitors,
  u.primary_competitor,
  u.competitor_category,
  u.uco_type,
  u.target_onboarding_month,

  -- Stage milestone classification
  CASE
    WHEN u.stage IN ('U4', 'U5') THEN 'NEAR_WIN'
    WHEN u.stage = 'U6' THEN 'LIVE'
    WHEN u.stage = 'U3' THEN 'SCOPING'
    WHEN u.stage IN ('U1', 'U2') THEN 'EARLY'
    WHEN u.stage IN ('Lost', 'Disqualified') THEN 'CLOSED'
    ELSE 'UNKNOWN'
  END AS uco_milestone,

  -- Request context
  LEFT(a.request_description, 200) AS description_preview,
  CONCAT('https://databricks.lightning.force.com/', a.asq_id) AS asq_link,
  CONCAT('https://databricks.lightning.force.com/', u.usecase_id) AS uco_link

FROM region_asqs a
INNER JOIN uco_data u ON a.account_id = u.account_id
ORDER BY
  a.status,
  u.estimated_monthly_dollar_dbus DESC NULLS LAST,
  a.created_date
