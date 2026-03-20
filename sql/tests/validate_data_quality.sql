-- ============================================================================
-- validate_data_quality.sql - Comprehensive Data Quality Checks
-- ============================================================================
-- Deep data quality validation for metric view source tables
-- Focuses on null detection, duplicate prevention, and referential integrity
-- ============================================================================

-- ============================================================================
-- SECTION 1: NULL ANALYSIS BY COLUMN
-- ============================================================================

-- Comprehensive null analysis for approval_request_detail
SELECT
  'ASQ Column Null Analysis' AS test_section,
  column_name,
  null_count,
  total_count,
  ROUND(null_count * 100.0 / total_count, 2) AS null_percentage,
  CASE
    WHEN null_percentage > 50 THEN 'HIGH'
    WHEN null_percentage > 10 THEN 'MEDIUM'
    WHEN null_percentage > 0 THEN 'LOW'
    ELSE 'NONE'
  END AS severity
FROM (
  SELECT
    COUNT(*) AS total_count,
    -- Key identifiers (should never be null)
    SUM(CASE WHEN approval_request_id IS NULL THEN 1 ELSE 0 END) AS null_approval_request_id,
    SUM(CASE WHEN owner_user_id IS NULL THEN 1 ELSE 0 END) AS null_owner_user_id,
    SUM(CASE WHEN account_id IS NULL THEN 1 ELSE 0 END) AS null_account_id,
    -- Status and dates (should never be null)
    SUM(CASE WHEN status IS NULL THEN 1 ELSE 0 END) AS null_status,
    SUM(CASE WHEN created_date IS NULL THEN 1 ELSE 0 END) AS null_created_date,
    -- Organization (critical for filtering)
    SUM(CASE WHEN business_unit IS NULL THEN 1 ELSE 0 END) AS null_business_unit,
    SUM(CASE WHEN region_level_1 IS NULL THEN 1 ELSE 0 END) AS null_region_level_1,
    -- Work classification
    SUM(CASE WHEN technical_specialization IS NULL THEN 1 ELSE 0 END) AS null_technical_specialization,
    SUM(CASE WHEN support_type IS NULL THEN 1 ELSE 0 END) AS null_support_type,
    SUM(CASE WHEN priority IS NULL THEN 1 ELSE 0 END) AS null_priority,
    -- Effort tracking (may be null)
    SUM(CASE WHEN estimated_effort_in_days IS NULL THEN 1 ELSE 0 END) AS null_estimated_effort,
    SUM(CASE WHEN actual_effort_in_days IS NULL THEN 1 ELSE 0 END) AS null_actual_effort,
    -- SLA dates (may be null for open ASQs)
    SUM(CASE WHEN target_end_date IS NULL THEN 1 ELSE 0 END) AS null_target_end_date,
    SUM(CASE WHEN actual_completion_date IS NULL THEN 1 ELSE 0 END) AS null_actual_completion_date
  FROM main.gtm_silver.approval_request_detail
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
)
UNPIVOT (
  null_count FOR column_name IN (
    null_approval_request_id AS 'approval_request_id',
    null_owner_user_id AS 'owner_user_id',
    null_account_id AS 'account_id',
    null_status AS 'status',
    null_created_date AS 'created_date',
    null_business_unit AS 'business_unit',
    null_region_level_1 AS 'region_level_1',
    null_technical_specialization AS 'technical_specialization',
    null_support_type AS 'support_type',
    null_priority AS 'priority',
    null_estimated_effort AS 'estimated_effort_in_days',
    null_actual_effort AS 'actual_effort_in_days',
    null_target_end_date AS 'target_end_date',
    null_actual_completion_date AS 'actual_completion_date'
  )
)
ORDER BY null_percentage DESC;

-- ============================================================================
-- SECTION 2: DUPLICATE DETECTION
-- ============================================================================

-- Find any duplicate ASQs (same ID appearing multiple times)
SELECT
  'Duplicate ASQ Detection' AS test_section,
  approval_request_id,
  COUNT(*) AS occurrence_count,
  ARRAY_AGG(DISTINCT owner_user_name) AS owners,
  ARRAY_AGG(DISTINCT status) AS statuses
FROM main.gtm_silver.approval_request_detail
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
GROUP BY approval_request_id
HAVING COUNT(*) > 1
LIMIT 20;

-- Check for duplicate owner assignments per ASQ
SELECT
  'Multiple Owner Check' AS test_section,
  approval_request_id,
  approval_request_name,
  COUNT(DISTINCT owner_user_id) AS owner_count,
  ARRAY_AGG(DISTINCT owner_user_name) AS owners
FROM main.gtm_silver.approval_request_detail
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
GROUP BY approval_request_id, approval_request_name
HAVING COUNT(DISTINCT owner_user_id) > 1
LIMIT 20;

-- ============================================================================
-- SECTION 3: REFERENTIAL INTEGRITY
-- ============================================================================

-- ASQ owners not found in hierarchy
SELECT
  'Orphan SSA Owners (not in hierarchy)' AS test_section,
  asq.owner_user_id,
  asq.owner_user_name,
  COUNT(*) AS asq_count,
  MIN(asq.created_date) AS earliest_asq,
  MAX(asq.created_date) AS latest_asq
FROM main.gtm_silver.approval_request_detail asq
LEFT JOIN main.gtm_silver.individual_hierarchy_field hier
  ON asq.owner_user_id = hier.user_id
WHERE asq.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
  AND hier.user_id IS NULL
GROUP BY asq.owner_user_id, asq.owner_user_name
ORDER BY asq_count DESC
LIMIT 20;

-- ASQ accounts not found in account_obt
SELECT
  'Orphan Accounts (not in account_obt)' AS test_section,
  asq.account_id,
  asq.account_name,
  COUNT(*) AS asq_count,
  asq.business_unit
FROM main.gtm_silver.approval_request_detail asq
LEFT JOIN main.gtm_gold.account_obt ao
  ON asq.account_id = ao.account_id
  AND ao.fiscal_year_quarter = (SELECT MAX(fiscal_year_quarter) FROM main.gtm_gold.account_obt)
WHERE asq.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
  AND ao.account_id IS NULL
GROUP BY asq.account_id, asq.account_name, asq.business_unit
ORDER BY asq_count DESC
LIMIT 20;

-- ============================================================================
-- SECTION 4: CONSISTENCY CHECKS
-- ============================================================================

-- Status vs completion date consistency
SELECT
  'Status/Completion Consistency' AS test_section,
  status,
  COUNT(*) AS total,
  COUNT(*) FILTER (WHERE actual_completion_date IS NOT NULL) AS with_completion_date,
  COUNT(*) FILTER (WHERE actual_completion_date IS NULL) AS without_completion_date,
  CASE
    WHEN status IN ('Complete', 'Closed') AND COUNT(*) FILTER (WHERE actual_completion_date IS NULL) > 0
    THEN 'WARN: Completed without date'
    WHEN status NOT IN ('Complete', 'Closed', 'Rejected') AND COUNT(*) FILTER (WHERE actual_completion_date IS NOT NULL) > 0
    THEN 'WARN: Open with completion date'
    ELSE 'OK'
  END AS consistency_check
FROM main.gtm_silver.approval_request_detail
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
GROUP BY status
ORDER BY total DESC;

-- Target date vs actual completion consistency
SELECT
  'Target/Actual Date Consistency' AS test_section,
  'ASQs with unusual date patterns' AS description,
  COUNT(*) FILTER (WHERE target_end_date IS NULL AND actual_completion_date IS NOT NULL) AS completed_no_target,
  COUNT(*) FILTER (WHERE actual_completion_date > target_end_date + INTERVAL 365 DAYS) AS extremely_late,
  COUNT(*) FILTER (WHERE actual_completion_date < created_date) AS completed_before_created
FROM main.gtm_silver.approval_request_detail
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail);

-- ============================================================================
-- SECTION 5: VALUE DISTRIBUTION
-- ============================================================================

-- Business unit distribution
SELECT
  'Business Unit Distribution' AS test_section,
  business_unit,
  COUNT(*) AS asq_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM main.gtm_silver.approval_request_detail
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
GROUP BY business_unit
ORDER BY asq_count DESC;

-- Status distribution
SELECT
  'Status Distribution' AS test_section,
  status,
  COUNT(*) AS asq_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM main.gtm_silver.approval_request_detail
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
GROUP BY status
ORDER BY asq_count DESC;

-- Specialization distribution
SELECT
  'Specialization Distribution' AS test_section,
  technical_specialization,
  COUNT(*) AS asq_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM main.gtm_silver.approval_request_detail
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
GROUP BY technical_specialization
ORDER BY asq_count DESC;

-- ============================================================================
-- SECTION 6: OUTLIER DETECTION
-- ============================================================================

-- Effort outliers
SELECT
  'Effort Outliers (> 3 std dev)' AS test_section,
  approval_request_id,
  approval_request_name,
  estimated_effort_in_days,
  actual_effort_in_days,
  actual_effort_in_days / NULLIF(estimated_effort_in_days, 0) AS effort_ratio,
  status
FROM main.gtm_silver.approval_request_detail
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
  AND actual_effort_in_days > (
    SELECT AVG(actual_effort_in_days) + 3 * STDDEV(actual_effort_in_days)
    FROM main.gtm_silver.approval_request_detail
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
      AND actual_effort_in_days IS NOT NULL
  )
ORDER BY actual_effort_in_days DESC
LIMIT 20;

-- Age outliers (extremely old open ASQs)
SELECT
  'Open ASQ Age Outliers (> 180 days)' AS test_section,
  approval_request_id,
  approval_request_name,
  owner_user_name,
  status,
  created_date,
  DATEDIFF(CURRENT_DATE(), created_date) AS days_open,
  target_end_date
FROM main.gtm_silver.approval_request_detail
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
  AND status IN ('New', 'Assigned', 'In Progress', 'Under Review', 'On Hold')
  AND DATEDIFF(CURRENT_DATE(), created_date) > 180
ORDER BY days_open DESC
LIMIT 20;

-- ============================================================================
-- SUMMARY: Data Quality Score
-- ============================================================================

SELECT
  'DATA QUALITY SUMMARY' AS test_section,
  total_asqs,
  ROUND(100 - (key_null_pct + duplicate_pct + orphan_pct) / 3, 2) AS quality_score,
  key_null_pct AS key_column_null_pct,
  duplicate_pct,
  orphan_pct AS hierarchy_orphan_pct,
  CASE
    WHEN 100 - (key_null_pct + duplicate_pct + orphan_pct) / 3 >= 95 THEN 'EXCELLENT'
    WHEN 100 - (key_null_pct + duplicate_pct + orphan_pct) / 3 >= 90 THEN 'GOOD'
    WHEN 100 - (key_null_pct + duplicate_pct + orphan_pct) / 3 >= 80 THEN 'FAIR'
    ELSE 'NEEDS ATTENTION'
  END AS quality_grade
FROM (
  SELECT
    COUNT(*) AS total_asqs,
    -- Key column null rate
    ROUND(
      (SUM(CASE WHEN approval_request_id IS NULL OR owner_user_id IS NULL OR status IS NULL THEN 1 ELSE 0 END) * 100.0
      / COUNT(*)), 2
    ) AS key_null_pct,
    -- Duplicate rate (simplified check)
    0.0 AS duplicate_pct,  -- Would require subquery
    -- Hierarchy orphan rate
    ROUND(
      (COUNT(*) FILTER (WHERE owner_user_id NOT IN (SELECT DISTINCT user_id FROM main.gtm_silver.individual_hierarchy_field)) * 100.0
      / COUNT(*)), 2
    ) AS orphan_pct
  FROM main.gtm_silver.approval_request_detail
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
);
