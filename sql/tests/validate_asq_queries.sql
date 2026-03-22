-- ============================================================================
-- validate_asq_queries.sql - ASQ Logfood Query Validation Tests
-- ============================================================================
-- Validates the 7 ASQ queries for data quality, field correctness, and hygiene
-- Run against: logfood workspace (profile: logfood)
-- Source tables: main.gtm_silver.* and main.gtm_gold.*
-- ============================================================================

-- ============================================================================
-- TEST 1: Source Tables Available
-- ============================================================================
-- Verify all required source tables have data

SELECT
  'Source Tables Available' AS test_name,
  CASE WHEN asq_count > 0 AND uco_count > 0 AND hier_count > 0 THEN 'PASS' ELSE 'FAIL' END AS result,
  asq_count AS approval_request_records,
  uco_count AS use_case_records,
  hier_count AS hierarchy_records
FROM (
  SELECT
    (SELECT COUNT(*) FROM main.gtm_silver.approval_request_detail
     WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)) AS asq_count,
    (SELECT COUNT(*) FROM main.gtm_silver.use_case_detail
     WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.use_case_detail)) AS uco_count,
    (SELECT COUNT(*) FROM main.gtm_silver.individual_hierarchy_salesforce) AS hier_count
);

-- ============================================================================
-- TEST 2: Region Summary Query Structure
-- ============================================================================
-- Validate cjc-asq-region-summary returns expected regions

SELECT
  'Region Summary Regions' AS test_name,
  CASE WHEN region_count >= 10 THEN 'PASS' ELSE 'WARN' END AS result,
  region_count,
  sample_regions
FROM (
  SELECT
    COUNT(DISTINCT region_level_1) AS region_count,
    CONCAT_WS(', ', SLICE(COLLECT_SET(region_level_1), 1, 5)) AS sample_regions
  FROM main.gtm_silver.approval_request_detail
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
    AND status IN ('New', 'Submitted', 'Under Review', 'In Progress')
);

-- ============================================================================
-- TEST 3: Hygiene Rule Coverage
-- ============================================================================
-- Validate hygiene rule logic produces expected distribution

SELECT
  'Hygiene Rule Coverage' AS test_name,
  CASE WHEN total_asqs > 0 AND compliant_count >= 0 THEN 'PASS' ELSE 'FAIL' END AS result,
  total_asqs,
  rule1_missing_notes,
  rule3_stale,
  rule4_expired,
  rule5_excessive,
  compliant_count,
  ROUND(100.0 * compliant_count / NULLIF(total_asqs, 0), 1) AS compliance_rate_pct
FROM (
  SELECT
    COUNT(*) AS total_asqs,
    SUM(CASE WHEN DATEDIFF(CURRENT_DATE, created_date) > 7
              AND (request_status_notes IS NULL OR LENGTH(request_status_notes) < 10)
              AND DATEDIFF(CURRENT_DATE, created_date) <= 90
              THEN 1 ELSE 0 END) AS rule1_missing_notes,
    SUM(CASE WHEN DATEDIFF(CURRENT_DATE, created_date) BETWEEN 30 AND 90
              AND (target_end_date IS NULL OR target_end_date < CURRENT_DATE)
              THEN 1 ELSE 0 END) AS rule3_stale,
    SUM(CASE WHEN target_end_date < CURRENT_DATE - INTERVAL 7 DAYS
              THEN 1 ELSE 0 END) AS rule4_expired,
    SUM(CASE WHEN DATEDIFF(CURRENT_DATE, created_date) > 90 THEN 1 ELSE 0 END) AS rule5_excessive,
    SUM(CASE WHEN DATEDIFF(CURRENT_DATE, created_date) <= 90
              AND (target_end_date IS NULL OR target_end_date >= CURRENT_DATE)
              AND (DATEDIFF(CURRENT_DATE, created_date) <= 7 OR (request_status_notes IS NOT NULL AND LENGTH(request_status_notes) >= 10))
              THEN 1 ELSE 0 END) AS compliant_count
  FROM main.gtm_silver.approval_request_detail
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
    AND status IN ('New', 'Submitted', 'Under Review', 'In Progress')
);

-- ============================================================================
-- TEST 4: Urgency Classification Coverage
-- ============================================================================
-- Validate urgency levels are assigned correctly

SELECT
  'Urgency Classification' AS test_name,
  CASE WHEN total_asqs > 0 THEN 'PASS' ELSE 'FAIL' END AS result,
  critical_count,
  high_count,
  medium_count,
  normal_count,
  total_asqs,
  ROUND(100.0 * (critical_count + high_count) / NULLIF(total_asqs, 0), 1) AS urgent_pct
FROM (
  SELECT
    COUNT(*) AS total_asqs,
    SUM(CASE WHEN target_end_date < CURRENT_DATE - INTERVAL 14 DAYS THEN 1 ELSE 0 END) AS critical_count,
    SUM(CASE WHEN target_end_date >= CURRENT_DATE - INTERVAL 14 DAYS
              AND target_end_date < CURRENT_DATE - INTERVAL 7 DAYS THEN 1 ELSE 0 END) AS high_count,
    SUM(CASE WHEN target_end_date >= CURRENT_DATE - INTERVAL 7 DAYS
              AND target_end_date < CURRENT_DATE THEN 1 ELSE 0 END) AS medium_count,
    SUM(CASE WHEN target_end_date IS NULL OR target_end_date >= CURRENT_DATE THEN 1 ELSE 0 END) AS normal_count
  FROM main.gtm_silver.approval_request_detail
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
    AND status IN ('New', 'Submitted', 'Under Review', 'In Progress')
);

-- ============================================================================
-- TEST 5: Hierarchy Join for By-Manager Query
-- ============================================================================
-- Validate individual_hierarchy_salesforce can join with ASQ owners

SELECT
  'Hierarchy Join for Manager Query' AS test_name,
  CASE WHEN matched_pct >= 0.80 THEN 'PASS' ELSE 'WARN' END AS result,
  ROUND(matched_pct * 100, 2) AS match_percentage,
  total_owners,
  matched_owners,
  managers_with_teams
FROM (
  SELECT
    COUNT(DISTINCT asq.owner_user_id) AS total_owners,
    COUNT(DISTINCT CASE WHEN hier.user_id IS NOT NULL THEN asq.owner_user_id END) AS matched_owners,
    COUNT(DISTINCT CASE WHEN hier.user_id IS NOT NULL THEN asq.owner_user_id END) * 1.0
      / NULLIF(COUNT(DISTINCT asq.owner_user_id), 0) AS matched_pct,
    (SELECT COUNT(DISTINCT line_manager_id)
     FROM main.gtm_silver.individual_hierarchy_salesforce
     WHERE line_manager_id IS NOT NULL) AS managers_with_teams
  FROM main.gtm_silver.approval_request_detail asq
  LEFT JOIN main.gtm_silver.individual_hierarchy_salesforce hier
    ON asq.owner_user_id = hier.user_id
  WHERE asq.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
    AND asq.status IN ('New', 'Submitted', 'Under Review', 'In Progress')
);

-- ============================================================================
-- TEST 6: UCO Stage Values Match Expected
-- ============================================================================
-- Validate UCO stages are U1-U6, Lost, Disqualified (not long form)

SELECT
  'UCO Stage Values' AS test_name,
  CASE WHEN unexpected_stages = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
  valid_stages_count,
  unexpected_stages,
  actual_stages
FROM (
  SELECT
    COUNT(DISTINCT stage) FILTER (WHERE stage IN ('U1', 'U2', 'U3', 'U4', 'U5', 'U6', 'Lost', 'Disqualified')) AS valid_stages_count,
    COUNT(DISTINCT stage) FILTER (WHERE stage NOT IN ('U1', 'U2', 'U3', 'U4', 'U5', 'U6', 'Lost', 'Disqualified') AND stage IS NOT NULL) AS unexpected_stages,
    ARRAY_AGG(DISTINCT stage) AS actual_stages
  FROM main.gtm_silver.use_case_detail
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.use_case_detail)
);

-- ============================================================================
-- TEST 7: UCO-ASQ Account Linkage
-- ============================================================================
-- Validate ASQs can be linked to UCOs via account_id

SELECT
  'UCO-ASQ Account Linkage' AS test_name,
  CASE WHEN linked_accounts > 0 THEN 'PASS' ELSE 'FAIL' END AS result,
  total_asq_accounts,
  linked_accounts,
  ROUND(100.0 * linked_accounts / NULLIF(total_asq_accounts, 0), 1) AS linkage_rate_pct,
  linked_ucos
FROM (
  SELECT
    COUNT(DISTINCT asq.account_id) AS total_asq_accounts,
    COUNT(DISTINCT CASE WHEN uco.account_id IS NOT NULL THEN asq.account_id END) AS linked_accounts,
    COUNT(DISTINCT uco.usecase_id) AS linked_ucos
  FROM main.gtm_silver.approval_request_detail asq
  LEFT JOIN main.gtm_silver.use_case_detail uco
    ON asq.account_id = uco.account_id
    AND uco.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.use_case_detail)
    AND uco.is_active_ind = true
  WHERE asq.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
    AND asq.status IN ('New', 'Submitted', 'Under Review', 'In Progress')
);

-- ============================================================================
-- TEST 8: Competitive Data Availability
-- ============================================================================
-- Validate competitive data fields (competitors, primary_competitor)

SELECT
  'Competitive Data Availability' AS test_name,
  CASE WHEN competitive_ucos > 0 THEN 'PASS' ELSE 'WARN' END AS result,
  total_active_ucos,
  has_competitors_field,
  competitive_ucos,
  has_primary_competitor,
  top_competitors
FROM (
  SELECT
    COUNT(DISTINCT usecase_id) AS total_active_ucos,
    COUNT(DISTINCT usecase_id) FILTER (WHERE competitors IS NOT NULL) AS has_competitors_field,
    COUNT(DISTINCT usecase_id) FILTER (WHERE competitors IS NOT NULL AND competitors != 'No Competitor' AND competitors != '') AS competitive_ucos,
    COUNT(DISTINCT usecase_id) FILTER (WHERE primary_competitor IS NOT NULL) AS has_primary_competitor,
    CONCAT_WS(', ', SLICE(COLLECT_SET(primary_competitor), 1, 5)) FILTER (WHERE primary_competitor IS NOT NULL) AS top_competitors
  FROM main.gtm_silver.use_case_detail
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.use_case_detail)
    AND is_active_ind = true
);

-- ============================================================================
-- TEST 9: Team Capacity Workload Distribution
-- ============================================================================
-- Validate workload calculation fields are available

SELECT
  'Workload Fields Available' AS test_name,
  CASE WHEN effort_available_pct > 0.50 THEN 'PASS' ELSE 'WARN' END AS result,
  total_open_asqs,
  with_estimated_effort,
  ROUND(effort_available_pct * 100, 1) AS effort_available_pct,
  avg_estimated_effort,
  max_estimated_effort
FROM (
  SELECT
    COUNT(*) AS total_open_asqs,
    COUNT(*) FILTER (WHERE estimated_effort_in_days IS NOT NULL) AS with_estimated_effort,
    COUNT(*) FILTER (WHERE estimated_effort_in_days IS NOT NULL) * 1.0
      / NULLIF(COUNT(*), 0) AS effort_available_pct,
    ROUND(AVG(COALESCE(estimated_effort_in_days, 5)), 1) AS avg_estimated_effort,
    MAX(estimated_effort_in_days) AS max_estimated_effort
  FROM main.gtm_silver.approval_request_detail
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
    AND status IN ('New', 'Submitted', 'Under Review', 'In Progress')
);

-- ============================================================================
-- TEST 10: Notes Status Detection
-- ============================================================================
-- Validate notes field can be used for hygiene detection

SELECT
  'Notes Status Detection' AS test_name,
  CASE WHEN no_notes_pct BETWEEN 0.10 AND 0.90 THEN 'PASS' ELSE 'WARN' END AS result,
  total_asqs,
  with_notes,
  without_notes,
  ROUND(no_notes_pct * 100, 1) AS no_notes_pct,
  avg_notes_length
FROM (
  SELECT
    COUNT(*) AS total_asqs,
    COUNT(*) FILTER (WHERE request_status_notes IS NOT NULL AND LENGTH(request_status_notes) >= 10) AS with_notes,
    COUNT(*) FILTER (WHERE request_status_notes IS NULL OR LENGTH(request_status_notes) < 10) AS without_notes,
    COUNT(*) FILTER (WHERE request_status_notes IS NULL OR LENGTH(request_status_notes) < 10) * 1.0
      / NULLIF(COUNT(*), 0) AS no_notes_pct,
    ROUND(AVG(LENGTH(request_status_notes)), 0) AS avg_notes_length
  FROM main.gtm_silver.approval_request_detail
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
    AND status IN ('New', 'Submitted', 'Under Review', 'In Progress')
);

-- ============================================================================
-- TEST 11: Query Execution - Region Summary
-- ============================================================================
-- Test cjc-asq-region-summary returns valid results

SELECT
  'Region Summary Execution' AS test_name,
  CASE WHEN total_regions > 0 AND total_asqs > 0 THEN 'PASS' ELSE 'FAIL' END AS result,
  total_regions,
  total_asqs,
  max_region,
  max_region_asqs
FROM (
  SELECT
    COUNT(DISTINCT region_level_1) AS total_regions,
    SUM(cnt) AS total_asqs,
    FIRST_VALUE(region_level_1) OVER (ORDER BY cnt DESC) AS max_region,
    MAX(cnt) AS max_region_asqs
  FROM (
    SELECT
      region_level_1,
      COUNT(*) AS cnt
    FROM main.gtm_silver.approval_request_detail
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
      AND status IN ('New', 'Submitted', 'Under Review', 'In Progress')
    GROUP BY region_level_1
  )
  GROUP BY region_level_1, cnt
  LIMIT 1
);

-- ============================================================================
-- TEST 12: Salesforce Link Format
-- ============================================================================
-- Validate SF link format is correct (approval_request_id format)

SELECT
  'Salesforce Link Format' AS test_name,
  CASE WHEN valid_id_format_pct >= 0.95 THEN 'PASS' ELSE 'FAIL' END AS result,
  total_asqs,
  valid_id_count,
  ROUND(valid_id_format_pct * 100, 2) AS valid_pct,
  sample_ids
FROM (
  SELECT
    COUNT(*) AS total_asqs,
    COUNT(*) FILTER (WHERE approval_request_id LIKE 'a%' AND LENGTH(approval_request_id) = 18) AS valid_id_count,
    COUNT(*) FILTER (WHERE approval_request_id LIKE 'a%' AND LENGTH(approval_request_id) = 18) * 1.0
      / NULLIF(COUNT(*), 0) AS valid_id_format_pct,
    CONCAT_WS(', ', SLICE(COLLECT_SET(approval_request_id), 1, 3)) AS sample_ids
  FROM main.gtm_silver.approval_request_detail
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
);

-- ============================================================================
-- TEST SUMMARY
-- ============================================================================

SELECT
  'ASQ QUERIES TEST SUMMARY' AS test_name,
  'See individual test results above' AS result,
  CURRENT_TIMESTAMP() AS run_timestamp,
  (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail) AS asq_snapshot,
  (SELECT MAX(snapshot_date) FROM main.gtm_silver.use_case_detail) AS uco_snapshot,
  (SELECT COUNT(*) FROM main.gtm_silver.approval_request_detail
   WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
     AND status IN ('New', 'Submitted', 'Under Review', 'In Progress')) AS open_asqs;
