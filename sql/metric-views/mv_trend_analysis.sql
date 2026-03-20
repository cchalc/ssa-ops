-- ============================================================================
-- mv_trend_analysis - YoY/QoQ Trend Analysis with Fiscal Periods
-- ============================================================================
-- Time-series metrics for trend analysis and forecasting
-- Supports YoY, QoQ, MoM comparisons using Databricks fiscal calendar
-- Sources: GTM Silver (ASQ history)
-- NO HARDCODED FILTERS - all filtering at query time
-- ============================================================================

CREATE OR REPLACE METRIC VIEW ${catalog}.${schema}.mv_trend_analysis
COMMENT 'Time-series trend analysis with fiscal period comparisons. Supports YoY, QoQ, MoM analysis for operational metrics.'
AS
SELECT
  -- ========================================================================
  -- ORGANIZATIONAL DIMENSIONS
  -- ========================================================================

  asq.business_unit
    COMMENT 'BU: AMER Enterprise & Emerging, AMER Industries, EMEA, APJ' AS `Business Unit`,
  asq.region_level_1
    COMMENT 'Region: CAN, RCT, FINS, etc.' AS `Region`,

  -- Manager Hierarchy
  hier.manager_level_1_name
    COMMENT 'Direct manager (L1)' AS `Manager L1`,
  hier.manager_level_2_name
    COMMENT 'Second-level manager (L2)' AS `Manager L2`,

  -- ========================================================================
  -- WORK CLASSIFICATION DIMENSIONS
  -- ========================================================================

  asq.technical_specialization
    COMMENT 'Technical focus' AS `Specialization`,
  asq.support_type
    COMMENT 'Support type' AS `Support Type`,

  -- ========================================================================
  -- TIME DIMENSIONS (Calendar)
  -- ========================================================================

  YEAR(asq.created_date)
    COMMENT 'Calendar year' AS `Calendar Year`,
  QUARTER(asq.created_date)
    COMMENT 'Calendar quarter (1-4)' AS `Calendar Quarter`,
  MONTH(asq.created_date)
    COMMENT 'Calendar month (1-12)' AS `Calendar Month`,
  WEEKOFYEAR(asq.created_date)
    COMMENT 'Calendar week' AS `Calendar Week`,
  DATE(asq.created_date)
    COMMENT 'Date' AS `Date`,
  DATE_TRUNC('MONTH', asq.created_date)
    COMMENT 'First day of month' AS `Month Start`,
  DATE_TRUNC('WEEK', asq.created_date)
    COMMENT 'First day of week' AS `Week Start`,

  -- ========================================================================
  -- TIME DIMENSIONS (Fiscal - FY ends Jan 31)
  -- ========================================================================

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
  CONCAT('FY', CASE WHEN MONTH(asq.created_date) = 1 THEN YEAR(asq.created_date)
                    ELSE YEAR(asq.created_date) + 1 END,
         ' Q', CASE
           WHEN MONTH(asq.created_date) IN (2, 3, 4) THEN 1
           WHEN MONTH(asq.created_date) IN (5, 6, 7) THEN 2
           WHEN MONTH(asq.created_date) IN (8, 9, 10) THEN 3
           ELSE 4
         END)
    COMMENT 'Fiscal year-quarter string' AS `Fiscal Year-Quarter`,

  -- ========================================================================
  -- RELATIVE TIME DIMENSIONS
  -- ========================================================================

  CASE WHEN (CASE WHEN MONTH(asq.created_date) = 1 THEN YEAR(asq.created_date)
                  ELSE YEAR(asq.created_date) + 1 END) =
            (CASE WHEN MONTH(CURRENT_DATE()) = 1 THEN YEAR(CURRENT_DATE())
                  ELSE YEAR(CURRENT_DATE()) + 1 END)
       THEN 'Current FY' ELSE 'Prior FY' END
    COMMENT 'Current vs prior fiscal year' AS `Is Current FY`,

  CASE WHEN DATE(asq.created_date) >= DATE_TRUNC('YEAR', CURRENT_DATE())
       THEN 'YTD' ELSE 'Prior' END
    COMMENT 'Year-to-date flag' AS `Is YTD`,

  CASE WHEN DATE(asq.created_date) >= DATE_TRUNC('QUARTER', CURRENT_DATE())
       THEN 'QTD' ELSE 'Prior' END
    COMMENT 'Quarter-to-date flag' AS `Is QTD`,

  CASE WHEN DATE(asq.created_date) >= DATE_TRUNC('MONTH', CURRENT_DATE())
       THEN 'MTD' ELSE 'Prior' END
    COMMENT 'Month-to-date flag' AS `Is MTD`,

  -- ========================================================================
  -- VOLUME MEASURES
  -- ========================================================================

  COUNT(1)
    COMMENT 'Total number of ASQs' AS `Total ASQs`,
  COUNT(1) FILTER (WHERE asq.status IN ('Complete', 'Closed'))
    COMMENT 'Completed ASQs' AS `Completed ASQs`,
  COUNT(1) FILTER (WHERE asq.status IN ('New', 'Assigned', 'In Progress', 'Under Review', 'On Hold'))
    COMMENT 'Open ASQs' AS `Open ASQs`,
  COUNT(DISTINCT asq.owner_user_id)
    COMMENT 'Unique SSAs' AS `Unique SSAs`,
  COUNT(DISTINCT asq.account_id)
    COMMENT 'Unique accounts' AS `Unique Accounts`,

  -- ========================================================================
  -- RATE MEASURES
  -- ========================================================================

  COUNT(1) FILTER (WHERE asq.status IN ('Complete', 'Closed')
                     AND asq.actual_completion_date <= asq.target_end_date) * 1.0
    / NULLIF(COUNT(1) FILTER (WHERE asq.status IN ('Complete', 'Closed')), 0)
    COMMENT 'On-time completion rate' AS `On-Time Rate`,

  COUNT(1) FILTER (WHERE asq.status IN ('Complete', 'Closed')) * 1.0
    / NULLIF(COUNT(1), 0)
    COMMENT 'Completion rate' AS `Completion Rate`,

  -- ========================================================================
  -- SPEED MEASURES
  -- ========================================================================

  AVG(DATEDIFF(asq.actual_completion_date, asq.created_date))
    COMMENT 'Average days to completion' AS `Avg Days to Complete`,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY DATEDIFF(asq.actual_completion_date, asq.created_date))
    COMMENT 'Median days to completion' AS `Median Days to Complete`,

  -- ========================================================================
  -- EFFORT MEASURES
  -- ========================================================================

  SUM(asq.estimated_effort_in_days)
    COMMENT 'Total estimated effort' AS `Total Estimated Days`,
  SUM(asq.actual_effort_in_days)
    COMMENT 'Total actual effort' AS `Total Actual Days`,
  AVG(asq.actual_effort_in_days)
    COMMENT 'Average effort per ASQ' AS `Avg Effort per ASQ`,

  -- ========================================================================
  -- PRODUCTIVITY MEASURES
  -- ========================================================================

  COUNT(1) * 1.0 / NULLIF(COUNT(DISTINCT asq.owner_user_id), 0)
    COMMENT 'ASQs per SSA' AS `ASQs per SSA`,
  COUNT(1) FILTER (WHERE asq.status IN ('Complete', 'Closed')) * 1.0
    / NULLIF(COUNT(DISTINCT asq.owner_user_id), 0)
    COMMENT 'Completions per SSA' AS `Completions per SSA`,

  -- ========================================================================
  -- CUMULATIVE MEASURES (for running totals)
  -- ========================================================================

  -- These work with window functions at query time
  COUNT(1)
    COMMENT 'Period count for cumulative sum' AS `Period ASQs`

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
-- Monthly Trend:
-- SELECT `Calendar Year`, `Calendar Month`,
--        MEASURE(`Total ASQs`), MEASURE(`On-Time Rate`),
--        MEASURE(`Avg Days to Complete`)
-- FROM mv_trend_analysis
-- WHERE `Calendar Year` >= 2025
-- GROUP BY ALL
-- ORDER BY `Calendar Year`, `Calendar Month`;
--
-- Fiscal Quarter Comparison:
-- SELECT `Fiscal Year`, `Fiscal Quarter`,
--        MEASURE(`Total ASQs`), MEASURE(`Completed ASQs`),
--        MEASURE(`On-Time Rate`), MEASURE(`Completions per SSA`)
-- FROM mv_trend_analysis
-- WHERE `Fiscal Year` IN (2025, 2026)
-- GROUP BY ALL
-- ORDER BY `Fiscal Year`, `Fiscal Quarter`;
--
-- YoY Comparison (Same Quarter):
-- SELECT `Fiscal Year`,
--        MEASURE(`Total ASQs`), MEASURE(`On-Time Rate`),
--        MEASURE(`Median Days to Complete`)
-- FROM mv_trend_analysis
-- WHERE `Fiscal Quarter` = 4
-- GROUP BY ALL;
--
-- Weekly Volume Trend:
-- SELECT `Week Start`, MEASURE(`Total ASQs`), MEASURE(`Completed ASQs`)
-- FROM mv_trend_analysis
-- WHERE `Business Unit` = 'AMER Enterprise & Emerging'
--   AND `Week Start` >= DATE_SUB(CURRENT_DATE(), 90)
-- GROUP BY ALL
-- ORDER BY `Week Start`;
--
-- YTD vs Prior Year:
-- SELECT `Is Current FY`, MEASURE(`Total ASQs`), MEASURE(`On-Time Rate`)
-- FROM mv_trend_analysis
-- WHERE `Is YTD` = 'YTD' OR `Is Current FY` = 'Prior FY'
-- GROUP BY ALL;
