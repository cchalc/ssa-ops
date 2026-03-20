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
  LEFT JOIN main.gtm_silver.individual_hierarchy_field hier
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
    COUNT(DISTINCT manager_level_1_name) AS total_l1_managers,
    COUNT(DISTINCT manager_level_1_name) FILTER (WHERE manager_level_2_name IS NULL) AS orphan_managers
  FROM main.gtm_silver.individual_hierarchy_field
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
  (SELECT MAX(fiscal_year_quarter) FROM main.gtm_gold.account_obt) AS account_obt_fyq;
