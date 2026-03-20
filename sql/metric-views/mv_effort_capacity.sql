-- ============================================================================
-- mv_effort_capacity - Effort Estimation & Capacity Planning
-- ============================================================================
-- Effort tracking and capacity planning metrics for SSA teams
-- Sources: GTM Silver tables on logfood (main catalog)
-- NO HARDCODED FILTERS - all BU/manager filtering at query time
-- ============================================================================

CREATE OR REPLACE METRIC VIEW ${catalog}.${schema}.mv_effort_capacity
COMMENT 'Effort estimation accuracy and capacity planning metrics. Filter by business_unit, manager hierarchy, or time period. No hardcoded team filters.'
AS
SELECT
  -- ========================================================================
  -- ORGANIZATIONAL DIMENSIONS
  -- ========================================================================

  asq.business_unit
    COMMENT 'BU: AMER Enterprise & Emerging, AMER Industries, EMEA, APJ' AS `Business Unit`,
  asq.region_level_1
    COMMENT 'Region: CAN, RCT, FINS, etc.' AS `Region`,
  asq.owner_user_id
    COMMENT 'SSA owner user ID' AS `Owner ID`,
  asq.owner_user_name
    COMMENT 'SSA owner name' AS `Owner`,

  -- Manager Hierarchy
  hier.manager_level_1_name
    COMMENT 'Direct manager (L1)' AS `Manager L1`,
  hier.manager_level_1_id
    COMMENT 'Direct manager ID' AS `Manager L1 ID`,
  hier.manager_level_2_name
    COMMENT 'Second-level manager (L2)' AS `Manager L2`,

  -- ========================================================================
  -- WORK CLASSIFICATION DIMENSIONS
  -- ========================================================================

  asq.status
    COMMENT 'Current ASQ status' AS `ASQ Status`,
  CASE WHEN asq.status IN ('New', 'Assigned', 'In Progress', 'Under Review', 'On Hold')
       THEN 'Open' ELSE 'Closed' END
    COMMENT 'Open vs Closed flag' AS `Is Open`,
  asq.technical_specialization
    COMMENT 'Technical focus' AS `Specialization`,
  asq.support_type
    COMMENT 'Support type' AS `Support Type`,
  asq.priority
    COMMENT 'Request priority level' AS `Priority`,

  -- Effort Bands
  CASE
    WHEN asq.estimated_effort_in_days IS NULL THEN 'Not Estimated'
    WHEN asq.estimated_effort_in_days <= 1 THEN '0-1 days'
    WHEN asq.estimated_effort_in_days <= 3 THEN '2-3 days'
    WHEN asq.estimated_effort_in_days <= 5 THEN '4-5 days'
    WHEN asq.estimated_effort_in_days <= 10 THEN '6-10 days'
    ELSE '10+ days'
  END
    COMMENT 'Estimated effort band' AS `Estimated Effort Band`,

  -- ========================================================================
  -- TIME DIMENSIONS
  -- ========================================================================

  YEAR(asq.created_date)
    COMMENT 'Calendar year ASQ was created' AS `Created Year`,
  CONCAT(YEAR(asq.created_date), '-Q', QUARTER(asq.created_date))
    COMMENT 'Calendar year-quarter' AS `Created Year-Quarter`,
  MONTH(asq.created_date)
    COMMENT 'Calendar month (1-12)' AS `Created Month`,
  WEEKOFYEAR(asq.created_date)
    COMMENT 'Calendar week of year' AS `Created Week`,
  DATE(asq.created_date)
    COMMENT 'Date ASQ was created' AS `Created Date`,

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
  -- EFFORT VOLUME MEASURES
  -- ========================================================================

  COUNT(1)
    COMMENT 'Total number of ASQs' AS `Total ASQs`,
  COUNT(1) FILTER (WHERE asq.estimated_effort_in_days IS NOT NULL)
    COMMENT 'ASQs with effort estimates' AS `Estimated ASQs`,
  COUNT(1) FILTER (WHERE asq.actual_effort_in_days IS NOT NULL)
    COMMENT 'ASQs with actual effort recorded' AS `Tracked ASQs`,
  COUNT(1) FILTER (WHERE asq.estimated_effort_in_days IS NULL)
    COMMENT 'ASQs missing effort estimates' AS `Unestimated ASQs`,

  -- ========================================================================
  -- EFFORT TOTAL MEASURES
  -- ========================================================================

  SUM(asq.estimated_effort_in_days)
    COMMENT 'Total estimated effort in days' AS `Total Estimated Days`,
  SUM(asq.actual_effort_in_days)
    COMMENT 'Total actual effort in days' AS `Total Actual Days`,
  SUM(asq.actual_effort_in_days) - SUM(asq.estimated_effort_in_days)
    COMMENT 'Total effort variance (actual - estimated)' AS `Total Effort Variance`,

  -- ========================================================================
  -- EFFORT AVERAGE MEASURES
  -- ========================================================================

  AVG(asq.estimated_effort_in_days)
    COMMENT 'Average estimated effort per ASQ' AS `Avg Estimated Days`,
  AVG(asq.actual_effort_in_days)
    COMMENT 'Average actual effort per ASQ' AS `Avg Actual Days`,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY asq.estimated_effort_in_days)
    COMMENT 'Median estimated effort' AS `Median Estimated Days`,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY asq.actual_effort_in_days)
    COMMENT 'Median actual effort' AS `Median Actual Days`,
  PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY asq.actual_effort_in_days)
    COMMENT '90th percentile actual effort' AS `P90 Actual Days`,

  -- ========================================================================
  -- EFFORT ACCURACY MEASURES
  -- ========================================================================

  AVG(asq.actual_effort_in_days / NULLIF(asq.estimated_effort_in_days, 0))
    COMMENT 'Average ratio of actual to estimated effort (1.0 = perfect)' AS `Avg Effort Ratio`,
  AVG(ABS(asq.actual_effort_in_days - asq.estimated_effort_in_days))
    COMMENT 'Average absolute error in days' AS `Avg Estimation Error`,

  -- Accuracy within 20%
  COUNT(1) FILTER (
    WHERE asq.estimated_effort_in_days IS NOT NULL
      AND asq.actual_effort_in_days IS NOT NULL
      AND ABS(asq.actual_effort_in_days - asq.estimated_effort_in_days)
          / asq.estimated_effort_in_days <= 0.2
  )
    COMMENT 'ASQs with estimates within 20% of actual' AS `Accurate Estimates`,

  COUNT(1) FILTER (
    WHERE asq.estimated_effort_in_days IS NOT NULL
      AND asq.actual_effort_in_days IS NOT NULL
      AND ABS(asq.actual_effort_in_days - asq.estimated_effort_in_days)
          / asq.estimated_effort_in_days <= 0.2
  ) * 1.0 / NULLIF(COUNT(1) FILTER (
    WHERE asq.estimated_effort_in_days IS NOT NULL
      AND asq.actual_effort_in_days IS NOT NULL
  ), 0)
    COMMENT 'Percentage of estimates within 20% of actual' AS `Estimation Accuracy Rate`,

  -- Under/Over estimation
  COUNT(1) FILTER (
    WHERE asq.actual_effort_in_days > asq.estimated_effort_in_days * 1.2
  )
    COMMENT 'ASQs underestimated by more than 20%' AS `Underestimated ASQs`,

  COUNT(1) FILTER (
    WHERE asq.actual_effort_in_days < asq.estimated_effort_in_days * 0.8
  )
    COMMENT 'ASQs overestimated by more than 20%' AS `Overestimated ASQs`,

  -- ========================================================================
  -- CAPACITY MEASURES (per SSA)
  -- ========================================================================

  COUNT(DISTINCT asq.owner_user_id)
    COMMENT 'Number of unique SSAs' AS `Unique SSAs`,

  SUM(asq.actual_effort_in_days) / NULLIF(COUNT(DISTINCT asq.owner_user_id), 0)
    COMMENT 'Average actual effort per SSA' AS `Effort per SSA`,

  COUNT(1) * 1.0 / NULLIF(COUNT(DISTINCT asq.owner_user_id), 0)
    COMMENT 'Average ASQ count per SSA' AS `ASQs per SSA`,

  SUM(asq.estimated_effort_in_days) FILTER (WHERE asq.status IN ('New', 'Assigned', 'In Progress', 'Under Review', 'On Hold'))
    COMMENT 'Total estimated effort in open backlog' AS `Open Backlog Days`,

  SUM(asq.estimated_effort_in_days) FILTER (WHERE asq.status IN ('New', 'Assigned', 'In Progress', 'Under Review', 'On Hold'))
    / NULLIF(COUNT(DISTINCT asq.owner_user_id), 0)
    COMMENT 'Open backlog days per SSA' AS `Backlog per SSA`,

  -- ========================================================================
  -- UTILIZATION MEASURES
  -- ========================================================================

  -- Assuming 20 working days per month
  SUM(asq.actual_effort_in_days) / NULLIF(COUNT(DISTINCT asq.owner_user_id) * 20, 0)
    COMMENT 'Monthly utilization rate (actual effort / available days)' AS `Monthly Utilization`,

  -- Throughput
  COUNT(1) FILTER (WHERE asq.status IN ('Complete', 'Closed'))
    / NULLIF(COUNT(DISTINCT asq.owner_user_id), 0)
    COMMENT 'Completed ASQs per SSA' AS `Throughput per SSA`

FROM main.gtm_silver.approval_request_detail asq
LEFT JOIN main.gtm_silver.individual_hierarchy_field hier
  ON asq.owner_user_id = hier.user_id
WHERE asq.snapshot_date = (
  SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail
);

-- ============================================================================
-- Sample Queries
-- ============================================================================
--
-- Team Capacity Overview:
-- SELECT `Manager L1`, MEASURE(`Unique SSAs`), MEASURE(`ASQs per SSA`),
--        MEASURE(`Backlog per SSA`), MEASURE(`Monthly Utilization`)
-- FROM mv_effort_capacity
-- WHERE `Business Unit` = 'AMER Enterprise & Emerging'
-- GROUP BY ALL;
--
-- Estimation Accuracy by Specialization:
-- SELECT `Specialization`, MEASURE(`Total ASQs`),
--        MEASURE(`Avg Effort Ratio`), MEASURE(`Estimation Accuracy Rate`)
-- FROM mv_effort_capacity
-- WHERE `Fiscal Year` = 2026
-- GROUP BY ALL;
--
-- SSA Individual Capacity:
-- SELECT `Owner`, MEASURE(`Total ASQs`), MEASURE(`Total Actual Days`),
--        MEASURE(`Open Backlog Days`), MEASURE(`Throughput per SSA`)
-- FROM mv_effort_capacity
-- WHERE `Manager L1` = 'Christopher Chalcraft'
-- GROUP BY ALL;
--
-- Effort Distribution by Band:
-- SELECT `Estimated Effort Band`, MEASURE(`Total ASQs`),
--        MEASURE(`Avg Actual Days`), MEASURE(`Estimation Accuracy Rate`)
-- FROM mv_effort_capacity
-- GROUP BY ALL
-- ORDER BY `Estimated Effort Band`;
