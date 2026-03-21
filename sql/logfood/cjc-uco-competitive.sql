-- cjc-uco-competitive
-- Competitive analysis for UCOs linked to ASQ accounts
-- Parameters: {{ region }} (e.g., CAN, RCT, FINS, MFG)
--
-- Identifies competitive displacement opportunities:
-- - Primary competitors by stage
-- - Win/loss against specific competitors
-- - Pipeline at risk

WITH asq_accounts AS (
  SELECT DISTINCT account_id, account_name
  FROM main.gtm_silver.approval_request_detail
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
    AND region_level_1 = '{{ region }}'
    AND status IN ('New', 'Submitted', 'Under Review', 'In Progress')
),
competitive_ucos AS (
  SELECT
    uco.account_id,
    uco.usecase_name,
    uco.stage,
    uco.competitor_status,
    uco.primary_competitor,
    uco.estimated_monthly_dollar_dbus,
    uco.type AS uco_type,
    aa.account_name
  FROM main.gtm_silver.use_case_detail uco
  JOIN asq_accounts aa ON uco.account_id = aa.account_id
  WHERE uco.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.use_case_detail)
    AND uco.is_active_ind = true
    AND uco.competitor_status IS NOT NULL
)
SELECT
  primary_competitor,
  competitor_status,
  COUNT(*) AS uco_count,
  SUM(estimated_monthly_dollar_dbus) AS total_monthly_dbu,

  -- Stage breakdown
  SUM(CASE WHEN stage IN ('U1 - Identified', 'U2 - Qualifying') THEN 1 ELSE 0 END) AS early_stage,
  SUM(CASE WHEN stage = 'U3 - Scoping' THEN 1 ELSE 0 END) AS scoping,
  SUM(CASE WHEN stage IN ('U4 - Confirming', 'U5 - Onboarding') THEN 1 ELSE 0 END) AS near_win,
  SUM(CASE WHEN stage = 'U6 - Live' THEN 1 ELSE 0 END) AS live,

  -- Example accounts
  CONCAT_WS(', ', SLICE(COLLECT_SET(account_name), 1, 3)) AS example_accounts

FROM competitive_ucos
WHERE primary_competitor IS NOT NULL
GROUP BY primary_competitor, competitor_status
ORDER BY
  SUM(estimated_monthly_dollar_dbus) DESC NULLS LAST,
  COUNT(*) DESC
