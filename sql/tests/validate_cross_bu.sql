-- ============================================================================
-- validate_cross_bu.sql - Cross-BU Consistency Validation
-- ============================================================================
-- Validates consistency across business units and manager hierarchies
-- Ensures metric calculations are consistent regardless of filter dimensions
-- ============================================================================

-- ============================================================================
-- TEST 1: BU Coverage Completeness
-- ============================================================================
-- All major BUs should have data

SELECT
  'BU Coverage' AS test_name,
  business_unit,
  COUNT(*) AS asq_count,
  COUNT(DISTINCT owner_user_id) AS unique_ssas,
  COUNT(DISTINCT account_id) AS unique_accounts,
  MIN(created_date) AS earliest_asq,
  MAX(created_date) AS latest_asq,
  CASE WHEN COUNT(*) > 0 THEN 'COVERED' ELSE 'MISSING' END AS coverage_status
FROM main.gtm_silver.approval_request_detail
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
GROUP BY business_unit
ORDER BY asq_count DESC;

-- ============================================================================
-- TEST 2: Region Level 1 Distribution by BU
-- ============================================================================
-- Validate expected region mappings

SELECT
  'Region Distribution by BU' AS test_name,
  business_unit,
  region_level_1,
  COUNT(*) AS asq_count,
  COUNT(DISTINCT owner_user_id) AS unique_ssas
FROM main.gtm_silver.approval_request_detail
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
GROUP BY business_unit, region_level_1
ORDER BY business_unit, asq_count DESC;

-- ============================================================================
-- TEST 3: Hierarchy Rollup Consistency
-- ============================================================================
-- Verify manager hierarchies roll up correctly

-- L1 to L2 rollup consistency
SELECT
  'L1 to L2 Rollup' AS test_name,
  manager_level_1_name,
  COUNT(DISTINCT manager_level_2_name) AS l2_count,
  ARRAY_AGG(DISTINCT manager_level_2_name) AS l2_managers,
  CASE
    WHEN COUNT(DISTINCT manager_level_2_name) > 1 THEN 'WARN: Multiple L2 for same L1'
    ELSE 'OK'
  END AS consistency_check
FROM main.gtm_silver.individual_hierarchy_field
WHERE manager_level_1_name IS NOT NULL
GROUP BY manager_level_1_name
HAVING COUNT(DISTINCT manager_level_2_name) > 1
LIMIT 20;

-- L2 to L3 rollup consistency
SELECT
  'L2 to L3 Rollup' AS test_name,
  manager_level_2_name,
  COUNT(DISTINCT manager_level_3_name) AS l3_count,
  ARRAY_AGG(DISTINCT manager_level_3_name) AS l3_managers,
  CASE
    WHEN COUNT(DISTINCT manager_level_3_name) > 1 THEN 'WARN: Multiple L3 for same L2'
    ELSE 'OK'
  END AS consistency_check
FROM main.gtm_silver.individual_hierarchy_field
WHERE manager_level_2_name IS NOT NULL
GROUP BY manager_level_2_name
HAVING COUNT(DISTINCT manager_level_3_name) > 1
LIMIT 20;

-- ============================================================================
-- TEST 4: Metric Consistency Across BUs
-- ============================================================================
-- Core metrics should compute consistently regardless of BU filter

-- Total should equal sum of BU parts
SELECT
  'Total = Sum of BU Parts' AS test_name,
  grand_total,
  bu_sum,
  CASE WHEN grand_total = bu_sum THEN 'PASS' ELSE 'FAIL' END AS result,
  grand_total - bu_sum AS difference
FROM (
  SELECT
    (SELECT COUNT(*) FROM main.gtm_silver.approval_request_detail
     WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
    ) AS grand_total,
    (SELECT SUM(cnt) FROM (
      SELECT business_unit, COUNT(*) AS cnt
      FROM main.gtm_silver.approval_request_detail
      WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
      GROUP BY business_unit
    )) AS bu_sum
);

-- Completion rate should be similar across comparable BUs (within reasonable variance)
SELECT
  'Completion Rate Variance' AS test_name,
  business_unit,
  total_asqs,
  completed_asqs,
  ROUND(completion_rate * 100, 2) AS completion_rate_pct,
  ROUND(AVG(completion_rate) OVER () * 100, 2) AS avg_rate_pct,
  ROUND(ABS(completion_rate - AVG(completion_rate) OVER ()) * 100, 2) AS variance_from_avg_pct,
  CASE
    WHEN ABS(completion_rate - AVG(completion_rate) OVER ()) > 0.2 THEN 'HIGH VARIANCE'
    ELSE 'NORMAL'
  END AS variance_status
FROM (
  SELECT
    business_unit,
    COUNT(*) AS total_asqs,
    COUNT(*) FILTER (WHERE status IN ('Complete', 'Closed')) AS completed_asqs,
    COUNT(*) FILTER (WHERE status IN ('Complete', 'Closed')) * 1.0 / NULLIF(COUNT(*), 0) AS completion_rate
  FROM main.gtm_silver.approval_request_detail
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
  GROUP BY business_unit
)
ORDER BY total_asqs DESC;

-- ============================================================================
-- TEST 5: SSA Distribution Fairness
-- ============================================================================
-- Check for extreme imbalances in workload distribution

SELECT
  'SSA Workload Distribution by BU' AS test_name,
  business_unit,
  COUNT(DISTINCT owner_user_id) AS unique_ssas,
  COUNT(*) AS total_asqs,
  ROUND(COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT owner_user_id), 0), 2) AS asqs_per_ssa,
  MIN(asq_cnt) AS min_asqs_per_ssa,
  MAX(asq_cnt) AS max_asqs_per_ssa,
  ROUND(MAX(asq_cnt) * 1.0 / NULLIF(MIN(asq_cnt), 0), 2) AS max_min_ratio,
  CASE
    WHEN MAX(asq_cnt) * 1.0 / NULLIF(MIN(asq_cnt), 0) > 5 THEN 'HIGH IMBALANCE'
    WHEN MAX(asq_cnt) * 1.0 / NULLIF(MIN(asq_cnt), 0) > 3 THEN 'MODERATE IMBALANCE'
    ELSE 'BALANCED'
  END AS balance_status
FROM (
  SELECT
    business_unit,
    owner_user_id,
    COUNT(*) AS asq_cnt
  FROM main.gtm_silver.approval_request_detail
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
  GROUP BY business_unit, owner_user_id
) sub
GROUP BY business_unit
ORDER BY total_asqs DESC;

-- ============================================================================
-- TEST 6: Time Period Consistency
-- ============================================================================
-- Metrics should be consistent when aggregated by different time grains

-- Fiscal quarter totals should equal monthly sums
SELECT
  'FQ Totals = Monthly Sums' AS test_name,
  fiscal_year,
  fiscal_quarter,
  fq_total,
  monthly_sum,
  CASE WHEN fq_total = monthly_sum THEN 'PASS' ELSE 'FAIL' END AS result
FROM (
  SELECT
    CASE WHEN MONTH(created_date) = 1 THEN YEAR(created_date)
         ELSE YEAR(created_date) + 1 END AS fiscal_year,
    CASE
      WHEN MONTH(created_date) IN (2, 3, 4) THEN 1
      WHEN MONTH(created_date) IN (5, 6, 7) THEN 2
      WHEN MONTH(created_date) IN (8, 9, 10) THEN 3
      ELSE 4
    END AS fiscal_quarter,
    COUNT(*) AS fq_total
  FROM main.gtm_silver.approval_request_detail
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
  GROUP BY fiscal_year, fiscal_quarter
) fq
JOIN (
  SELECT
    CASE WHEN MONTH(created_date) = 1 THEN YEAR(created_date)
         ELSE YEAR(created_date) + 1 END AS fiscal_year,
    CASE
      WHEN MONTH(created_date) IN (2, 3, 4) THEN 1
      WHEN MONTH(created_date) IN (5, 6, 7) THEN 2
      WHEN MONTH(created_date) IN (8, 9, 10) THEN 3
      ELSE 4
    END AS fiscal_quarter,
    SUM(monthly_cnt) AS monthly_sum
  FROM (
    SELECT
      created_date,
      MONTH(created_date) AS month,
      COUNT(*) AS monthly_cnt
    FROM main.gtm_silver.approval_request_detail
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
    GROUP BY created_date, MONTH(created_date)
  )
  GROUP BY fiscal_year, fiscal_quarter
) mo USING (fiscal_year, fiscal_quarter)
ORDER BY fiscal_year DESC, fiscal_quarter DESC
LIMIT 8;

-- ============================================================================
-- TEST 7: Account Tier Consistency
-- ============================================================================
-- Account tiers from account_obt should be consistent

SELECT
  'Account Tier Distribution by BU' AS test_name,
  asq.business_unit,
  ao.spend_tier,
  COUNT(DISTINCT asq.account_id) AS unique_accounts,
  COUNT(*) AS asq_count,
  ROUND(SUM(ao.dbu_dollars_qtd), 0) AS total_dbu
FROM main.gtm_silver.approval_request_detail asq
LEFT JOIN main.gtm_gold.account_obt ao
  ON asq.account_id = ao.account_id
  AND ao.fiscal_year_quarter = (SELECT MAX(fiscal_year_quarter) FROM main.gtm_gold.account_obt)
WHERE asq.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
GROUP BY asq.business_unit, ao.spend_tier
ORDER BY asq.business_unit, asq_count DESC;

-- ============================================================================
-- SUMMARY: Cross-BU Health Check
-- ============================================================================

SELECT
  'CROSS-BU HEALTH SUMMARY' AS test_section,
  (SELECT COUNT(DISTINCT business_unit)
   FROM main.gtm_silver.approval_request_detail
   WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
  ) AS active_bus,
  (SELECT COUNT(DISTINCT region_level_1)
   FROM main.gtm_silver.approval_request_detail
   WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
  ) AS active_regions,
  (SELECT COUNT(DISTINCT owner_user_id)
   FROM main.gtm_silver.approval_request_detail
   WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
  ) AS active_ssas,
  (SELECT COUNT(DISTINCT manager_level_1_name)
   FROM main.gtm_silver.individual_hierarchy_field
  ) AS unique_l1_managers,
  'Review individual tests for specific issues' AS guidance;
