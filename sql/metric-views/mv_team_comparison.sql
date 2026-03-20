-- ============================================================================
-- mv_team_comparison - Cross-BU Benchmarking
-- ============================================================================
-- Compare operational metrics across business units and teams
-- Enables benchmarking and best practice identification
-- Sources: GTM Silver (ASQ) + GTM Gold (account metrics)
-- NO HARDCODED FILTERS - all filtering at query time
-- ============================================================================

CREATE OR REPLACE METRIC VIEW ${catalog}.${schema}.mv_team_comparison
COMMENT 'Cross-BU and cross-team benchmarking metrics. Compare operational performance across business units, regions, and manager hierarchies.'
AS
SELECT
  -- ========================================================================
  -- ORGANIZATIONAL DIMENSIONS (for comparison grouping)
  -- ========================================================================

  asq.business_unit
    COMMENT 'BU: AMER Enterprise & Emerging, AMER Industries, EMEA, APJ' AS `Business Unit`,
  asq.region_level_1
    COMMENT 'Region: CAN, RCT, FINS, etc.' AS `Region`,
  asq.region_level_2
    COMMENT 'Sub-region' AS `Sub-Region`,

  -- Manager Hierarchy (multiple levels for rollup comparisons)
  hier.manager_level_1_name
    COMMENT 'Direct manager (L1)' AS `Manager L1`,
  hier.manager_level_2_name
    COMMENT 'Second-level manager (L2)' AS `Manager L2`,
  hier.manager_level_3_name
    COMMENT 'Third-level manager (L3)' AS `Manager L3`,

  -- ========================================================================
  -- WORK CLASSIFICATION DIMENSIONS
  -- ========================================================================

  asq.technical_specialization
    COMMENT 'Technical focus' AS `Specialization`,
  asq.support_type
    COMMENT 'Support type' AS `Support Type`,
  asq.priority
    COMMENT 'Request priority' AS `Priority`,

  -- ========================================================================
  -- TIME DIMENSIONS
  -- ========================================================================

  YEAR(asq.created_date)
    COMMENT 'Calendar year' AS `Created Year`,
  CONCAT(YEAR(asq.created_date), '-Q', QUARTER(asq.created_date))
    COMMENT 'Calendar year-quarter' AS `Created Year-Quarter`,
  MONTH(asq.created_date)
    COMMENT 'Calendar month' AS `Created Month`,

  -- Fiscal
  CASE WHEN MONTH(asq.created_date) = 1 THEN YEAR(asq.created_date)
       ELSE YEAR(asq.created_date) + 1 END
    COMMENT 'Fiscal year (ends Jan 31)' AS `Fiscal Year`,
  CASE
    WHEN MONTH(asq.created_date) IN (2, 3, 4) THEN 1
    WHEN MONTH(asq.created_date) IN (5, 6, 7) THEN 2
    WHEN MONTH(asq.created_date) IN (8, 9, 10) THEN 3
    ELSE 4
  END
    COMMENT 'Fiscal quarter (1-4)' AS `Fiscal Quarter`,

  -- ========================================================================
  -- TEAM SIZE MEASURES
  -- ========================================================================

  COUNT(DISTINCT asq.owner_user_id)
    COMMENT 'Number of unique SSAs' AS `Team Size`,
  COUNT(DISTINCT asq.account_id)
    COMMENT 'Number of unique accounts served' AS `Accounts Served`,

  -- ========================================================================
  -- VOLUME MEASURES
  -- ========================================================================

  COUNT(1)
    COMMENT 'Total number of ASQs' AS `Total ASQs`,
  COUNT(1) FILTER (WHERE asq.status IN ('Complete', 'Closed'))
    COMMENT 'Completed ASQs' AS `Completed ASQs`,
  COUNT(1) FILTER (WHERE asq.status NOT IN ('Complete', 'Closed', 'Rejected')
                     AND asq.target_end_date < CURRENT_DATE())
    COMMENT 'Overdue ASQs' AS `Overdue ASQs`,

  -- ========================================================================
  -- PRODUCTIVITY MEASURES (per SSA)
  -- ========================================================================

  COUNT(1) * 1.0 / NULLIF(COUNT(DISTINCT asq.owner_user_id), 0)
    COMMENT 'Average ASQs handled per SSA' AS `ASQs per SSA`,
  COUNT(1) FILTER (WHERE asq.status IN ('Complete', 'Closed')) * 1.0
    / NULLIF(COUNT(DISTINCT asq.owner_user_id), 0)
    COMMENT 'Completed ASQs per SSA' AS `Completions per SSA`,
  COUNT(DISTINCT asq.account_id) * 1.0 / NULLIF(COUNT(DISTINCT asq.owner_user_id), 0)
    COMMENT 'Unique accounts per SSA' AS `Accounts per SSA`,
  SUM(asq.actual_effort_in_days) / NULLIF(COUNT(DISTINCT asq.owner_user_id), 0)
    COMMENT 'Total effort days per SSA' AS `Effort Days per SSA`,

  -- ========================================================================
  -- QUALITY MEASURES
  -- ========================================================================

  -- On-Time Rate
  COUNT(1) FILTER (WHERE asq.status IN ('Complete', 'Closed')
                     AND asq.actual_completion_date <= asq.target_end_date) * 1.0
    / NULLIF(COUNT(1) FILTER (WHERE asq.status IN ('Complete', 'Closed')), 0)
    COMMENT 'Percentage of ASQs completed on time' AS `On-Time Rate`,

  -- Overdue Rate
  COUNT(1) FILTER (WHERE asq.status NOT IN ('Complete', 'Closed', 'Rejected')
                     AND asq.target_end_date < CURRENT_DATE()) * 1.0
    / NULLIF(COUNT(1) FILTER (WHERE asq.status IN ('New', 'Assigned', 'In Progress', 'Under Review', 'On Hold')), 0)
    COMMENT 'Percentage of open ASQs that are overdue' AS `Overdue Rate`,

  -- Completion Rate
  COUNT(1) FILTER (WHERE asq.status IN ('Complete', 'Closed')) * 1.0
    / NULLIF(COUNT(1), 0)
    COMMENT 'Overall completion rate' AS `Completion Rate`,

  -- ========================================================================
  -- SPEED MEASURES
  -- ========================================================================

  AVG(DATEDIFF(asq.actual_completion_date, asq.created_date))
    COMMENT 'Average days to completion' AS `Avg Days to Complete`,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY DATEDIFF(asq.actual_completion_date, asq.created_date))
    COMMENT 'Median days to completion' AS `Median Days to Complete`,
  PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY DATEDIFF(asq.actual_completion_date, asq.created_date))
    COMMENT '90th percentile days to completion' AS `P90 Days to Complete`,

  -- ========================================================================
  -- EFFORT EFFICIENCY MEASURES
  -- ========================================================================

  AVG(asq.actual_effort_in_days / NULLIF(asq.estimated_effort_in_days, 0))
    COMMENT 'Average effort ratio (actual/estimated)' AS `Avg Effort Ratio`,
  SUM(asq.actual_effort_in_days)
    COMMENT 'Total actual effort in days' AS `Total Effort Days`,
  SUM(asq.estimated_effort_in_days)
    COMMENT 'Total estimated effort in days' AS `Total Estimated Days`,

  -- ========================================================================
  -- BUSINESS IMPACT MEASURES
  -- ========================================================================

  SUM(ao.dbu_dollars_qtd)
    COMMENT 'Total DBU consumption of served accounts' AS `Account DBU QTD`,
  SUM(ao.total_arr)
    COMMENT 'Total ARR of served accounts' AS `Account ARR`,
  SUM(ao.dbu_dollars_qtd) / NULLIF(COUNT(1), 0)
    COMMENT 'DBU per ASQ' AS `DBU per ASQ`,
  SUM(ao.total_arr) / NULLIF(COUNT(DISTINCT asq.account_id), 0)
    COMMENT 'Average ARR of served accounts' AS `Avg Account ARR`,

  -- ========================================================================
  -- BENCHMARK RANKING MEASURES
  -- ========================================================================

  -- These can be used with window functions at query time
  COUNT(1) FILTER (WHERE asq.priority = 'Critical')
    COMMENT 'Critical priority ASQs' AS `Critical ASQs`,
  COUNT(1) FILTER (WHERE asq.priority = 'Critical') * 1.0 / NULLIF(COUNT(1), 0)
    COMMENT 'Percentage of critical ASQs' AS `Critical Rate`

FROM main.gtm_silver.approval_request_detail asq
LEFT JOIN main.gtm_silver.individual_hierarchy_field hier
  ON asq.owner_user_id = hier.user_id
LEFT JOIN main.gtm_gold.account_obt ao
  ON asq.account_id = ao.account_id
  AND ao.fiscal_year_quarter = (
    SELECT MAX(fiscal_year_quarter) FROM main.gtm_gold.account_obt
  )
WHERE asq.snapshot_date = (
  SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail
);

-- ============================================================================
-- Sample Queries
-- ============================================================================
--
-- BU Comparison (Global View):
-- SELECT `Business Unit`,
--        MEASURE(`Team Size`), MEASURE(`Total ASQs`), MEASURE(`ASQs per SSA`),
--        MEASURE(`On-Time Rate`), MEASURE(`Median Days to Complete`)
-- FROM mv_team_comparison
-- WHERE `Fiscal Year` = 2026
-- GROUP BY ALL
-- ORDER BY `On-Time Rate` DESC;
--
-- Manager Comparison within BU:
-- SELECT `Manager L1`,
--        MEASURE(`Team Size`), MEASURE(`Completions per SSA`),
--        MEASURE(`On-Time Rate`), MEASURE(`Account ARR`)
-- FROM mv_team_comparison
-- WHERE `Business Unit` = 'AMER Enterprise & Emerging'
-- GROUP BY ALL;
--
-- Region Benchmarking:
-- SELECT `Region`,
--        MEASURE(`Total ASQs`), MEASURE(`Median Days to Complete`),
--        MEASURE(`On-Time Rate`), MEASURE(`DBU per ASQ`)
-- FROM mv_team_comparison
-- GROUP BY ALL
-- ORDER BY `Total ASQs` DESC;
--
-- Specialization Performance:
-- SELECT `Specialization`,
--        MEASURE(`Total ASQs`), MEASURE(`Avg Days to Complete`),
--        MEASURE(`On-Time Rate`), MEASURE(`Avg Effort Ratio`)
-- FROM mv_team_comparison
-- GROUP BY ALL;
