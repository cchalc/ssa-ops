-- ============================================================================
-- validate_metric_views.sql - Core Metric View Validation Tests
-- ============================================================================
-- Validates the metric views for data quality, join integrity, and correctness
-- Run against: ${catalog}.${schema} (parameterized deployment target)
-- Source validation: main.gtm_silver.* and main.gtm_gold.* on logfood
-- ============================================================================

-- ============================================================================
-- TEST 1: Source Data Freshness
-- ============================================================================
-- Ensure source tables have recent data (within 48 hours)

-- ASQ snapshot freshness
SELECT
  'ASQ Snapshot Freshness' AS test_name,
  CASE WHEN DATEDIFF(CURRENT_DATE(), MAX(snapshot_date)) <= 2
       THEN 'PASS' ELSE 'FAIL' END AS result,
  MAX(snapshot_date) AS latest_snapshot,
  DATEDIFF(CURRENT_DATE(), MAX(snapshot_date)) AS days_since_snapshot
FROM main.gtm_silver.approval_request_detail;

-- Account OBT freshness
SELECT
  'Account OBT Freshness' AS test_name,
  CASE WHEN MAX(fiscal_year_quarter) IS NOT NULL
       THEN 'PASS' ELSE 'FAIL' END AS result,
  MAX(fiscal_year_quarter) AS latest_fyq
FROM main.gtm_gold.account_obt;

-- ============================================================================
-- TEST 2: NULL Validation - Key Columns
-- ============================================================================
-- Critical columns should never be null

-- ASQ key columns
SELECT
  'ASQ Key Columns Not Null' AS test_name,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
  COUNT(*) AS null_count,
  'approval_request_id, owner_user_id, account_id, status, created_date' AS columns_tested
FROM main.gtm_silver.approval_request_detail
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
  AND (approval_request_id IS NULL
       OR owner_user_id IS NULL
       OR account_id IS NULL
       OR status IS NULL
       OR created_date IS NULL);

-- Business unit not null
SELECT
  'Business Unit Not Null' AS test_name,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
  COUNT(*) AS null_count
FROM main.gtm_silver.approval_request_detail
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
  AND business_unit IS NULL;

-- ============================================================================
-- TEST 3: Duplicate Detection
-- ============================================================================
-- Each ASQ should appear exactly once per snapshot

SELECT
  'No Duplicate ASQs Per Snapshot' AS test_name,
  CASE WHEN MAX(cnt) = 1 THEN 'PASS' ELSE 'FAIL' END AS result,
  MAX(cnt) AS max_duplicates,
  COUNT(*) FILTER (WHERE cnt > 1) AS records_with_duplicates
FROM (
  SELECT approval_request_id, snapshot_date, COUNT(*) AS cnt
  FROM main.gtm_silver.approval_request_detail
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
  GROUP BY approval_request_id, snapshot_date
);

-- ============================================================================
-- TEST 4: Join Integrity - Hierarchy Table
-- ============================================================================
-- SSA owners should have hierarchy records

SELECT
  'Hierarchy Join Coverage' AS test_name,
  CASE WHEN matched_pct >= 0.95 THEN 'PASS' ELSE 'WARN' END AS result,
  ROUND(matched_pct * 100, 2) AS match_percentage,
  total_ssas,
  matched_ssas,
  unmatched_ssas
FROM (
  SELECT
    COUNT(DISTINCT asq.owner_user_id) AS total_ssas,
    COUNT(DISTINCT CASE WHEN hier.user_id IS NOT NULL THEN asq.owner_user_id END) AS matched_ssas,
    COUNT(DISTINCT CASE WHEN hier.user_id IS NULL THEN asq.owner_user_id END) AS unmatched_ssas,
    COUNT(DISTINCT CASE WHEN hier.user_id IS NOT NULL THEN asq.owner_user_id END) * 1.0
      / NULLIF(COUNT(DISTINCT asq.owner_user_id), 0) AS matched_pct
  FROM main.gtm_silver.approval_request_detail asq
  LEFT JOIN main.gtm_silver.individual_hierarchy_salesforce hier
    ON asq.owner_user_id = hier.user_id
  WHERE asq.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
);

-- ============================================================================
-- TEST 5: Join Integrity - Account OBT
-- ============================================================================
-- ASQ accounts should generally have account_obt records

SELECT
  'Account OBT Join Coverage' AS test_name,
  CASE WHEN matched_pct >= 0.90 THEN 'PASS' ELSE 'WARN' END AS result,
  ROUND(matched_pct * 100, 2) AS match_percentage,
  total_accounts,
  matched_accounts,
  unmatched_accounts
FROM (
  SELECT
    COUNT(DISTINCT asq.account_id) AS total_accounts,
    COUNT(DISTINCT CASE WHEN ao.account_id IS NOT NULL THEN asq.account_id END) AS matched_accounts,
    COUNT(DISTINCT CASE WHEN ao.account_id IS NULL THEN asq.account_id END) AS unmatched_accounts,
    COUNT(DISTINCT CASE WHEN ao.account_id IS NOT NULL THEN asq.account_id END) * 1.0
      / NULLIF(COUNT(DISTINCT asq.account_id), 0) AS matched_pct
  FROM main.gtm_silver.approval_request_detail asq
  LEFT JOIN main.gtm_gold.account_obt ao
    ON asq.account_id = ao.account_id
    AND ao.fiscal_year_quarter = (SELECT MAX(fiscal_year_quarter) FROM main.gtm_gold.account_obt)
  WHERE asq.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
);

-- ============================================================================
-- TEST 6: Business Unit Values
-- ============================================================================
-- Validate expected business unit values

SELECT
  'Valid Business Units' AS test_name,
  CASE WHEN unexpected_count = 0 THEN 'PASS' ELSE 'WARN' END AS result,
  unexpected_count,
  unexpected_values
FROM (
  SELECT
    COUNT(DISTINCT business_unit) FILTER (
      WHERE business_unit NOT IN (
        'AMER Enterprise & Emerging',
        'AMER Industries',
        'EMEA',
        'APJ'
      )
    ) AS unexpected_count,
    ARRAY_AGG(DISTINCT business_unit) FILTER (
      WHERE business_unit NOT IN (
        'AMER Enterprise & Emerging',
        'AMER Industries',
        'EMEA',
        'APJ'
      )
    ) AS unexpected_values
  FROM main.gtm_silver.approval_request_detail
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
);

-- ============================================================================
-- TEST 7: Status Values
-- ============================================================================
-- Validate expected status values

SELECT
  'Valid ASQ Statuses' AS test_name,
  CASE WHEN unexpected_count = 0 THEN 'PASS' ELSE 'WARN' END AS result,
  unexpected_count,
  actual_statuses
FROM (
  SELECT
    COUNT(DISTINCT status) FILTER (
      WHERE status NOT IN (
        'New', 'Assigned', 'In Progress', 'Under Review',
        'On Hold', 'Complete', 'Closed', 'Rejected'
      )
    ) AS unexpected_count,
    ARRAY_AGG(DISTINCT status) AS actual_statuses
  FROM main.gtm_silver.approval_request_detail
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
);

-- ============================================================================
-- TEST 8: Date Sanity Checks
-- ============================================================================
-- Dates should be reasonable (not in future, not too old)

SELECT
  'Date Sanity - Created Date' AS test_name,
  CASE WHEN future_dates = 0 AND ancient_dates = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
  future_dates,
  ancient_dates,
  min_date,
  max_date
FROM (
  SELECT
    COUNT(*) FILTER (WHERE created_date > CURRENT_DATE()) AS future_dates,
    COUNT(*) FILTER (WHERE created_date < '2015-01-01') AS ancient_dates,
    MIN(created_date) AS min_date,
    MAX(created_date) AS max_date
  FROM main.gtm_silver.approval_request_detail
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
);

-- Completion date after created date
SELECT
  'Date Sanity - Completion After Created' AS test_name,
  CASE WHEN invalid_count = 0 THEN 'PASS' ELSE 'WARN' END AS result,
  invalid_count,
  'ASQs with completion before creation' AS description
FROM (
  SELECT COUNT(*) AS invalid_count
  FROM main.gtm_silver.approval_request_detail
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
    AND actual_completion_date IS NOT NULL
    AND actual_completion_date < created_date
);

-- ============================================================================
-- TEST 9: Effort Data Quality
-- ============================================================================
-- Effort values should be reasonable

SELECT
  'Effort Data Quality' AS test_name,
  CASE WHEN negative_effort = 0 AND extreme_effort = 0 THEN 'PASS' ELSE 'WARN' END AS result,
  negative_effort,
  extreme_effort,
  avg_estimated,
  avg_actual,
  max_actual
FROM (
  SELECT
    COUNT(*) FILTER (WHERE estimated_effort_in_days < 0 OR actual_effort_in_days < 0) AS negative_effort,
    COUNT(*) FILTER (WHERE actual_effort_in_days > 100) AS extreme_effort,
    ROUND(AVG(estimated_effort_in_days), 2) AS avg_estimated,
    ROUND(AVG(actual_effort_in_days), 2) AS avg_actual,
    MAX(actual_effort_in_days) AS max_actual
  FROM main.gtm_silver.approval_request_detail
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
);

-- ============================================================================
-- TEST 10: Metric Calculation Validation
-- ============================================================================
-- Validate that calculated metrics are mathematically correct

-- Open + Closed should equal Total
SELECT
  'Open + Closed = Total' AS test_name,
  CASE WHEN ABS(total - (open_asqs + closed_asqs)) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
  total,
  open_asqs,
  closed_asqs,
  total - (open_asqs + closed_asqs) AS difference
FROM (
  SELECT
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE status IN ('New', 'Assigned', 'In Progress', 'Under Review', 'On Hold')) AS open_asqs,
    COUNT(*) FILTER (WHERE status IN ('Complete', 'Closed', 'Rejected')) AS closed_asqs
  FROM main.gtm_silver.approval_request_detail
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
);

-- On-Time + Late should equal Completed (for those with completion date)
SELECT
  'On-Time + Late = Completed' AS test_name,
  CASE WHEN ABS(completed - (on_time + late)) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
  completed,
  on_time,
  late,
  completed - (on_time + late) AS difference
FROM (
  SELECT
    COUNT(*) FILTER (WHERE actual_completion_date IS NOT NULL) AS completed,
    COUNT(*) FILTER (WHERE actual_completion_date IS NOT NULL
                       AND actual_completion_date <= target_end_date) AS on_time,
    COUNT(*) FILTER (WHERE actual_completion_date IS NOT NULL
                       AND actual_completion_date > target_end_date) AS late
  FROM main.gtm_silver.approval_request_detail
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
);

-- ============================================================================
-- TEST 11: Cross-BU Consistency
-- ============================================================================
-- Hierarchy rollups should be consistent

SELECT
  'Hierarchy Rollup Consistency' AS test_name,
  CASE WHEN orphan_managers = 0 THEN 'PASS' ELSE 'WARN' END AS result,
  total_l1_managers,
  orphan_managers,
  'L1 managers without L2 hierarchy' AS description
FROM (
  SELECT
    COUNT(DISTINCT line_manager_name) AS total_l1_managers,
    COUNT(DISTINCT line_manager_name) FILTER (WHERE `2nd_line_manager_name` IS NULL) AS orphan_managers
  FROM main.gtm_silver.individual_hierarchy_salesforce
);

-- ============================================================================
-- TEST 12: Account Segmentation Join (Focus & Discipline)
-- ============================================================================
-- Validate account segmentation data availability for priority account tracking

SELECT
  'Account Segmentation Coverage' AS test_name,
  CASE WHEN matched_pct >= 0.80 THEN 'PASS' ELSE 'WARN' END AS result,
  ROUND(matched_pct * 100, 2) AS match_percentage,
  total_accounts,
  matched_accounts,
  accounts_with_tier
FROM (
  SELECT
    COUNT(DISTINCT asq.account_id) AS total_accounts,
    COUNT(DISTINCT CASE WHEN acct.account_id IS NOT NULL THEN asq.account_id END) AS matched_accounts,
    COUNT(DISTINCT CASE WHEN acct.account_tier IS NOT NULL THEN asq.account_id END) AS accounts_with_tier,
    COUNT(DISTINCT CASE WHEN acct.account_id IS NOT NULL THEN asq.account_id END) * 1.0
      / NULLIF(COUNT(DISTINCT asq.account_id), 0) AS matched_pct
  FROM main.gtm_silver.approval_request_detail asq
  LEFT JOIN main.gtm_gold.rpt_account_dim acct
    ON asq.account_id = acct.account_id
  WHERE asq.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
);

-- ============================================================================
-- TEST 13: Account Tier Distribution
-- ============================================================================
-- Validate A+/A/B/C/Focus Account tier distribution

SELECT
  'Account Tier Distribution' AS test_name,
  CASE WHEN priority_pct > 0 THEN 'PASS' ELSE 'WARN' END AS result,
  ROUND(priority_pct * 100, 2) AS priority_percentage,
  total_with_tier,
  a_plus_count,
  a_count,
  b_count,
  c_count,
  focus_account_count
FROM (
  SELECT
    COUNT(DISTINCT account_id) AS total_with_tier,
    COUNT(DISTINCT account_id) FILTER (WHERE account_tier = 'A+') AS a_plus_count,
    COUNT(DISTINCT account_id) FILTER (WHERE account_tier = 'A') AS a_count,
    COUNT(DISTINCT account_id) FILTER (WHERE account_tier = 'B') AS b_count,
    COUNT(DISTINCT account_id) FILTER (WHERE account_tier = 'C') AS c_count,
    COUNT(DISTINCT account_id) FILTER (WHERE account_tier LIKE 'Focus Account%') AS focus_account_count,
    COUNT(DISTINCT account_id) FILTER (WHERE account_tier IN ('A+', 'A') OR account_tier LIKE 'Focus Account%') * 1.0
      / NULLIF(COUNT(DISTINCT account_id), 0) AS priority_pct
  FROM main.gtm_gold.rpt_account_dim
  WHERE account_tier IS NOT NULL
);

-- ============================================================================
-- TEST 14: UCO Data Availability (Velocity & Competitive)
-- ============================================================================
-- Validate UCO data for velocity and competitive tracking

SELECT
  'UCO Data Availability' AS test_name,
  CASE WHEN uco_count > 0 THEN 'PASS' ELSE 'FAIL' END AS result,
  uco_count,
  with_stage_count,
  with_competitor_count,
  with_days_in_stage
FROM (
  SELECT
    COUNT(DISTINCT usecase_id) AS uco_count,
    COUNT(DISTINCT usecase_id) FILTER (WHERE stage IS NOT NULL) AS with_stage_count,
    COUNT(DISTINCT usecase_id) FILTER (WHERE competitors IS NOT NULL AND competitors != '' AND competitors != 'No Competitor') AS with_competitor_count,
    COUNT(DISTINCT usecase_id) FILTER (WHERE days_in_stage IS NOT NULL) AS with_days_in_stage
  FROM main.gtm_silver.use_case_detail
);

-- ============================================================================
-- TEST 15: ASQ-UCO Linkage Rate
-- ============================================================================
-- Validate ASQ-UCO linkage for pipeline impact metrics

SELECT
  'ASQ-UCO Linkage Rate' AS test_name,
  CASE WHEN linkage_rate >= 0.50 THEN 'PASS' ELSE 'WARN' END AS result,
  ROUND(linkage_rate * 100, 2) AS linkage_percentage,
  total_asqs,
  asqs_with_uco_linkage,
  unique_linked_ucos
FROM (
  SELECT
    COUNT(DISTINCT asq.approval_request_id) AS total_asqs,
    COUNT(DISTINCT CASE WHEN uco.usecase_id IS NOT NULL THEN asq.approval_request_id END) AS asqs_with_uco_linkage,
    COUNT(DISTINCT uco.usecase_id) AS unique_linked_ucos,
    COUNT(DISTINCT CASE WHEN uco.usecase_id IS NOT NULL THEN asq.approval_request_id END) * 1.0
      / NULLIF(COUNT(DISTINCT asq.approval_request_id), 0) AS linkage_rate
  FROM main.gtm_silver.approval_request_detail asq
  LEFT JOIN main.gtm_silver.use_case_detail uco
    ON asq.account_id = uco.account_id
  WHERE asq.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
);

-- ============================================================================
-- TEST 16: UCO Stage Distribution
-- ============================================================================
-- Validate UCO stages for velocity tracking (U1-U6, Lost, Disqualified)

SELECT
  'UCO Stage Distribution' AS test_name,
  CASE WHEN production_plus_pct > 0 THEN 'PASS' ELSE 'WARN' END AS result,
  total_ucos,
  u1_u2_early,
  u3_scoping,
  u4_confirming,
  u5_onboarding,
  u6_live,
  lost_count,
  ROUND(production_plus_pct * 100, 2) AS production_plus_percentage
FROM (
  SELECT
    COUNT(DISTINCT usecase_id) AS total_ucos,
    COUNT(DISTINCT usecase_id) FILTER (WHERE stage IN ('U1', 'U2')) AS u1_u2_early,
    COUNT(DISTINCT usecase_id) FILTER (WHERE stage = 'U3') AS u3_scoping,
    COUNT(DISTINCT usecase_id) FILTER (WHERE stage = 'U4') AS u4_confirming,
    COUNT(DISTINCT usecase_id) FILTER (WHERE stage = 'U5') AS u5_onboarding,
    COUNT(DISTINCT usecase_id) FILTER (WHERE stage = 'U6') AS u6_live,
    COUNT(DISTINCT usecase_id) FILTER (WHERE stage = 'Lost') AS lost_count,
    COUNT(DISTINCT usecase_id) FILTER (WHERE stage IN ('U5', 'U6')) * 1.0
      / NULLIF(COUNT(DISTINCT usecase_id), 0) AS production_plus_pct
  FROM main.gtm_silver.use_case_detail
);

-- ============================================================================
-- TEST 17: Competitive Win/Loss Data
-- ============================================================================
-- Validate win/loss tracking data for competitive analysis
-- Win = stage U6 (Live), Loss = stage Lost

SELECT
  'Competitive Win/Loss Data' AS test_name,
  CASE WHEN total_resolved > 0 AND win_rate BETWEEN 0.3 AND 0.9 THEN 'PASS' ELSE 'WARN' END AS result,
  total_resolved,
  won_count,
  lost_count,
  ROUND(win_rate * 100, 2) AS win_rate_pct,
  competitive_count,
  competitive_won
FROM (
  SELECT
    COUNT(DISTINCT usecase_id) FILTER (WHERE stage IN ('U6', 'Lost')) AS total_resolved,
    COUNT(DISTINCT usecase_id) FILTER (WHERE stage = 'U6') AS won_count,
    COUNT(DISTINCT usecase_id) FILTER (WHERE stage = 'Lost') AS lost_count,
    COUNT(DISTINCT usecase_id) FILTER (WHERE stage = 'U6') * 1.0
      / NULLIF(COUNT(DISTINCT usecase_id) FILTER (WHERE stage IN ('U6', 'Lost')), 0) AS win_rate,
    COUNT(DISTINCT usecase_id) FILTER (WHERE competitors IS NOT NULL AND competitors != '' AND competitors != 'No Competitor') AS competitive_count,
    COUNT(DISTINCT usecase_id) FILTER (WHERE competitors IS NOT NULL AND competitors != '' AND competitors != 'No Competitor'
                                          AND stage = 'U6') AS competitive_won
  FROM main.gtm_silver.use_case_detail
);

-- ============================================================================
-- TEST 18: Focus & Discipline - Priority Effort
-- ============================================================================
-- Validate that we can calculate priority effort rates
-- Priority = A+/A tier, Focus Account, or Strategic Account

SELECT
  'Focus & Discipline Data Quality' AS test_name,
  CASE WHEN priority_effort_rate > 0 THEN 'PASS' ELSE 'WARN' END AS result,
  total_asqs,
  priority_asqs,
  ROUND(priority_effort_rate * 100, 2) AS priority_effort_rate_pct,
  total_effort_days,
  priority_effort_days
FROM (
  SELECT
    COUNT(*) AS total_asqs,
    COUNT(*) FILTER (WHERE acct.account_tier IN ('A+', 'A') OR acct.account_tier LIKE 'Focus Account%' OR acct.is_strategic_account_ind = TRUE) AS priority_asqs,
    SUM(COALESCE(asq.actual_effort_in_days, asq.estimated_effort_in_days, 5)) AS total_effort_days,
    SUM(COALESCE(asq.actual_effort_in_days, asq.estimated_effort_in_days, 5))
      FILTER (WHERE acct.account_tier IN ('A+', 'A') OR acct.account_tier LIKE 'Focus Account%' OR acct.is_strategic_account_ind = TRUE) AS priority_effort_days,
    SUM(COALESCE(asq.actual_effort_in_days, asq.estimated_effort_in_days, 5))
      FILTER (WHERE acct.account_tier IN ('A+', 'A') OR acct.account_tier LIKE 'Focus Account%' OR acct.is_strategic_account_ind = TRUE) * 1.0
      / NULLIF(SUM(COALESCE(asq.actual_effort_in_days, asq.estimated_effort_in_days, 5)), 0) AS priority_effort_rate
  FROM main.gtm_silver.approval_request_detail asq
  LEFT JOIN main.gtm_gold.rpt_account_dim acct
    ON asq.account_id = acct.account_id
  WHERE asq.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
    AND asq.status IN ('In Progress', 'Complete', 'Closed', 'Completed')
);

-- ============================================================================
-- SUMMARY: All Tests
-- ============================================================================
-- Run this to get a consolidated view of all test results

SELECT
  'TEST SUMMARY' AS test_name,
  'See individual test results above' AS result,
  CURRENT_TIMESTAMP() AS run_timestamp,
  (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail) AS asq_snapshot,
  (SELECT COUNT(DISTINCT usecase_id) FROM main.gtm_silver.use_case_detail) AS uco_count,
  (SELECT MAX(fiscal_year_quarter) FROM main.gtm_gold.account_obt) AS account_obt_fyq;
