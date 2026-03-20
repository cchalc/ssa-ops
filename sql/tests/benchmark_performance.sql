-- ============================================================================
-- benchmark_performance.sql - Query Performance Benchmarks
-- ============================================================================
-- Performance tests for metric view queries
-- Measures execution time and resource usage for common query patterns
-- Run these on a SQL warehouse to establish baseline performance
-- ============================================================================

-- ============================================================================
-- BENCHMARK 1: Full Table Scan Performance
-- ============================================================================
-- Baseline: How fast can we scan the entire ASQ dataset?

-- Time a full count
SELECT
  'Full Table Scan' AS benchmark,
  'approval_request_detail' AS table_name,
  COUNT(*) AS row_count,
  'Check query profile for execution time' AS note
FROM main.gtm_silver.approval_request_detail
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail);

-- ============================================================================
-- BENCHMARK 2: BU Filter Performance
-- ============================================================================
-- Common pattern: Filter by business unit

SELECT
  'BU Filter - AMER Enterprise' AS benchmark,
  COUNT(*) AS row_count,
  COUNT(DISTINCT owner_user_id) AS unique_ssas,
  COUNT(DISTINCT account_id) AS unique_accounts
FROM main.gtm_silver.approval_request_detail
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
  AND business_unit = 'AMER Enterprise & Emerging';

SELECT
  'BU Filter - EMEA' AS benchmark,
  COUNT(*) AS row_count,
  COUNT(DISTINCT owner_user_id) AS unique_ssas,
  COUNT(DISTINCT account_id) AS unique_accounts
FROM main.gtm_silver.approval_request_detail
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
  AND business_unit = 'EMEA';

-- ============================================================================
-- BENCHMARK 3: Join Performance
-- ============================================================================
-- Critical: Hierarchy + Account OBT joins

-- Hierarchy join only
SELECT
  'Hierarchy Join' AS benchmark,
  COUNT(*) AS row_count,
  COUNT(DISTINCT hier.manager_level_1_name) AS unique_l1_managers
FROM main.gtm_silver.approval_request_detail asq
LEFT JOIN main.gtm_silver.individual_hierarchy_field hier
  ON asq.owner_user_id = hier.user_id
WHERE asq.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail);

-- Account OBT join only
SELECT
  'Account OBT Join' AS benchmark,
  COUNT(*) AS row_count,
  SUM(ao.dbu_dollars_qtd) AS total_dbu
FROM main.gtm_silver.approval_request_detail asq
LEFT JOIN main.gtm_gold.account_obt ao
  ON asq.account_id = ao.account_id
  AND ao.fiscal_year_quarter = (SELECT MAX(fiscal_year_quarter) FROM main.gtm_gold.account_obt)
WHERE asq.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail);

-- Both joins (full metric view pattern)
SELECT
  'Full Join Pattern (Hierarchy + Account OBT)' AS benchmark,
  COUNT(*) AS row_count,
  COUNT(DISTINCT hier.manager_level_1_name) AS unique_l1_managers,
  SUM(ao.dbu_dollars_qtd) AS total_dbu
FROM main.gtm_silver.approval_request_detail asq
LEFT JOIN main.gtm_silver.individual_hierarchy_field hier
  ON asq.owner_user_id = hier.user_id
LEFT JOIN main.gtm_gold.account_obt ao
  ON asq.account_id = ao.account_id
  AND ao.fiscal_year_quarter = (SELECT MAX(fiscal_year_quarter) FROM main.gtm_gold.account_obt)
WHERE asq.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail);

-- ============================================================================
-- BENCHMARK 4: Aggregation Performance
-- ============================================================================
-- Test GROUP BY performance at different granularities

-- Coarse aggregation (by BU)
SELECT
  'Aggregation - By BU' AS benchmark,
  business_unit,
  COUNT(*) AS total_asqs,
  COUNT(*) FILTER (WHERE status IN ('Complete', 'Closed')) AS completed,
  AVG(DATEDIFF(actual_completion_date, created_date)) AS avg_days_to_complete
FROM main.gtm_silver.approval_request_detail
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
GROUP BY business_unit;

-- Medium aggregation (by BU + Region)
SELECT
  'Aggregation - By BU + Region' AS benchmark,
  business_unit,
  region_level_1,
  COUNT(*) AS total_asqs,
  COUNT(*) FILTER (WHERE status IN ('Complete', 'Closed')) AS completed
FROM main.gtm_silver.approval_request_detail
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
GROUP BY business_unit, region_level_1;

-- Fine aggregation (by SSA)
SELECT
  'Aggregation - By SSA' AS benchmark,
  owner_user_id,
  owner_user_name,
  COUNT(*) AS total_asqs,
  COUNT(*) FILTER (WHERE status IN ('Complete', 'Closed')) AS completed
FROM main.gtm_silver.approval_request_detail
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
GROUP BY owner_user_id, owner_user_name;

-- ============================================================================
-- BENCHMARK 5: Complex Metric Calculations
-- ============================================================================
-- Test performance of rate calculations and percentiles

SELECT
  'Complex Metrics - Rates' AS benchmark,
  business_unit,
  COUNT(*) AS total,
  ROUND(COUNT(*) FILTER (WHERE status IN ('Complete', 'Closed')) * 100.0 / NULLIF(COUNT(*), 0), 2) AS completion_rate,
  ROUND(COUNT(*) FILTER (WHERE status IN ('Complete', 'Closed') AND actual_completion_date <= target_end_date) * 100.0
    / NULLIF(COUNT(*) FILTER (WHERE status IN ('Complete', 'Closed')), 0), 2) AS on_time_rate
FROM main.gtm_silver.approval_request_detail
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
GROUP BY business_unit;

SELECT
  'Complex Metrics - Percentiles' AS benchmark,
  business_unit,
  AVG(DATEDIFF(actual_completion_date, created_date)) AS avg_days,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY DATEDIFF(actual_completion_date, created_date)) AS median_days,
  PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY DATEDIFF(actual_completion_date, created_date)) AS p90_days
FROM main.gtm_silver.approval_request_detail
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
  AND actual_completion_date IS NOT NULL
GROUP BY business_unit;

-- ============================================================================
-- BENCHMARK 6: Time Series Query
-- ============================================================================
-- Test trend analysis query patterns

SELECT
  'Time Series - Monthly Trend' AS benchmark,
  YEAR(created_date) AS year,
  MONTH(created_date) AS month,
  COUNT(*) AS total_asqs,
  COUNT(*) FILTER (WHERE status IN ('Complete', 'Closed')) AS completed_asqs
FROM main.gtm_silver.approval_request_detail
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
  AND created_date >= DATE_SUB(CURRENT_DATE(), 365)
GROUP BY YEAR(created_date), MONTH(created_date)
ORDER BY year, month;

-- ============================================================================
-- BENCHMARK 7: Manager Hierarchy Rollup
-- ============================================================================
-- Test hierarchical aggregation performance

SELECT
  'Hierarchy Rollup - L1 Manager' AS benchmark,
  hier.manager_level_1_name,
  COUNT(*) AS total_asqs,
  COUNT(DISTINCT asq.owner_user_id) AS team_size,
  COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT asq.owner_user_id), 0) AS asqs_per_ssa
FROM main.gtm_silver.approval_request_detail asq
LEFT JOIN main.gtm_silver.individual_hierarchy_field hier
  ON asq.owner_user_id = hier.user_id
WHERE asq.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
GROUP BY hier.manager_level_1_name
HAVING COUNT(*) >= 5
ORDER BY total_asqs DESC
LIMIT 20;

-- ============================================================================
-- BENCHMARK 8: Full Metric View Query Pattern
-- ============================================================================
-- Simulates the full metric view query with all dimensions and measures

SELECT
  'Full Metric View Pattern' AS benchmark,
  asq.business_unit,
  asq.region_level_1,
  hier.manager_level_1_name,
  YEAR(asq.created_date) AS created_year,
  -- Volume measures
  COUNT(*) AS total_asqs,
  COUNT(*) FILTER (WHERE asq.status IN ('New', 'Assigned', 'In Progress', 'Under Review', 'On Hold')) AS open_asqs,
  COUNT(*) FILTER (WHERE asq.status IN ('Complete', 'Closed')) AS completed_asqs,
  -- Rate measures
  ROUND(COUNT(*) FILTER (WHERE asq.status IN ('Complete', 'Closed')) * 100.0 / NULLIF(COUNT(*), 0), 2) AS completion_rate,
  ROUND(COUNT(*) FILTER (WHERE asq.actual_completion_date <= asq.target_end_date) * 100.0
    / NULLIF(COUNT(*) FILTER (WHERE asq.actual_completion_date IS NOT NULL), 0), 2) AS on_time_rate,
  -- Speed measures
  AVG(DATEDIFF(asq.actual_completion_date, asq.created_date)) AS avg_days_to_complete,
  -- Consumption measures
  SUM(ao.dbu_dollars_qtd) AS total_dbu_qtd,
  AVG(ao.dbu_dollars_qtd) AS avg_dbu_per_account
FROM main.gtm_silver.approval_request_detail asq
LEFT JOIN main.gtm_silver.individual_hierarchy_field hier
  ON asq.owner_user_id = hier.user_id
LEFT JOIN main.gtm_gold.account_obt ao
  ON asq.account_id = ao.account_id
  AND ao.fiscal_year_quarter = (SELECT MAX(fiscal_year_quarter) FROM main.gtm_gold.account_obt)
WHERE asq.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
GROUP BY
  asq.business_unit,
  asq.region_level_1,
  hier.manager_level_1_name,
  YEAR(asq.created_date);

-- ============================================================================
-- PERFORMANCE SUMMARY
-- ============================================================================
-- Guidelines for acceptable performance

SELECT
  'PERFORMANCE GUIDELINES' AS section,
  'Full Table Scan' AS benchmark,
  '< 5 seconds' AS target_time,
  'Baseline performance' AS note
UNION ALL
SELECT 'PERFORMANCE GUIDELINES', 'BU Filter', '< 2 seconds', 'Single BU filter'
UNION ALL
SELECT 'PERFORMANCE GUIDELINES', 'Full Join Pattern', '< 10 seconds', 'All tables joined'
UNION ALL
SELECT 'PERFORMANCE GUIDELINES', 'Complex Metrics', '< 5 seconds', 'With rate calculations'
UNION ALL
SELECT 'PERFORMANCE GUIDELINES', 'Full Metric View', '< 15 seconds', 'Complete pattern';
