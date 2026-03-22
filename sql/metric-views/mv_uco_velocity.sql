-- ============================================================================
-- mv_uco_velocity - UCO Stage Velocity & Time-to-Production
-- ============================================================================
-- Tracks how quickly SSA-engaged UCOs progress through pipeline stages
-- Charter Metric #3: Time-to-Production
-- Milestones: U3→U4 (Tech Win), U4→U5 (Production), U5→U6 (Go Live)
-- Sources: GTM Silver (ASQ, use_case_detail, individual_hierarchy) + GTM Gold (core_usecase_curated)
-- NO HARDCODED FILTERS - all BU/manager filtering at query time
-- ============================================================================

CREATE OR REPLACE METRIC VIEW ${catalog}.${schema}.mv_uco_velocity
COMMENT 'UCO stage velocity metrics. Track time-to-production (Charter Metric #3). Shows stage progression rates and days-in-stage for SSA-engaged UCOs.'
AS
SELECT
  -- ========================================================================
  -- ORGANIZATIONAL DIMENSIONS
  -- ========================================================================

  asq.business_unit
    COMMENT 'BU: AMER Enterprise & Emerging, AMER Industries, EMEA, APJ' AS `Business Unit`,
  asq.region_level_1
    COMMENT 'Region: CAN, RCT, FINS, etc.' AS `Region`,
  asq.owner_user_name
    COMMENT 'SSA owner name' AS `Owner`,
  asq.owner_user_id
    COMMENT 'SSA owner user ID' AS `Owner ID`,

  -- Manager Hierarchy (from individual_hierarchy_salesforce)
  hier.line_manager_name
    COMMENT 'Direct manager (L1)' AS `Manager L1`,
  hier.line_manager_id
    COMMENT 'Direct manager ID' AS `Manager L1 ID`,
  hier.`2nd_line_manager_name`
    COMMENT 'Second-level manager (L2)' AS `Manager L2`,

  -- ========================================================================
  -- CUSTOMER DIMENSIONS
  -- ========================================================================

  asq.account_id
    COMMENT 'Account ID' AS `Account ID`,
  asq.account_name
    COMMENT 'Customer account name' AS `Account`,
  ao.account_segment
    COMMENT 'Customer segment' AS `Account Segment`,
  ao.vertical_segment
    COMMENT 'Industry vertical' AS `Account Vertical`,

  -- ========================================================================
  -- UCO CLASSIFICATION DIMENSIONS
  -- ========================================================================

  uco.stage
    COMMENT 'Current UCO stage: U1-U6, Lost, Disqualified' AS `UCO Stage`,
  -- Stage Category
  CASE
    WHEN uco.stage IN ('U1', 'U2') THEN 'Early'
    WHEN uco.stage = 'U3' THEN 'Engaged (SSA Active)'
    WHEN uco.stage = 'U4' THEN 'Tech Win'
    WHEN uco.stage = 'U5' THEN 'Production'
    WHEN uco.stage = 'U6' THEN 'Go Live'
    WHEN uco.stage = 'Lost' THEN 'Lost'
    WHEN uco.stage = 'Disqualified' THEN 'Disqualified'
    ELSE 'Unknown'
  END
    COMMENT 'Stage category: Early, Engaged, Tech Win, Production, Go Live, Lost, Disqualified' AS `Stage Category`,
  uco.implementation_status
    COMMENT 'UCO implementation status: Green, Yellow, Red' AS `Implementation Status`,
  uco.use_case_product
    COMMENT 'Product category' AS `Product Category`,
  uco.type
    COMMENT 'UCO type' AS `UCO Type`,

  -- ========================================================================
  -- WORK CLASSIFICATION DIMENSIONS
  -- ========================================================================

  asq.technical_specialization
    COMMENT 'Technical focus' AS `Specialization`,
  asq.support_type
    COMMENT 'Support type' AS `Support Type`,

  -- ========================================================================
  -- TIME DIMENSIONS
  -- ========================================================================

  YEAR(asq.created_date)
    COMMENT 'Calendar year' AS `Created Year`,
  CONCAT(YEAR(asq.created_date), '-Q', QUARTER(asq.created_date))
    COMMENT 'Calendar year-quarter' AS `Created Year-Quarter`,

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
  -- ASQ LINKAGE MEASURES
  -- ========================================================================

  COUNT(DISTINCT asq.approval_request_id)
    COMMENT 'Total ASQs linked to UCOs' AS `Total ASQs`,
  COUNT(DISTINCT asq.approval_request_id) FILTER (WHERE asq.status IN ('Complete', 'Closed', 'Completed'))
    COMMENT 'Completed ASQs linked to UCOs' AS `Completed ASQs`,

  -- ========================================================================
  -- UCO VOLUME BY STAGE
  -- ========================================================================

  COUNT(DISTINCT uco.usecase_id)
    COMMENT 'Total unique UCOs linked to ASQs' AS `Total UCOs`,

  -- Stage Distribution
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.stage IN ('U1', 'U2'))
    COMMENT 'UCOs in early stages (U1-U2)' AS `Early Stage UCOs`,
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.stage = 'U3')
    COMMENT 'UCOs in scoping (U3) - SSA actively engaged' AS `Scoping UCOs`,
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.stage = 'U4')
    COMMENT 'UCOs at tech win (U4)' AS `Tech Win UCOs`,
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.stage = 'U5')
    COMMENT 'UCOs at production (U5)' AS `Production UCOs`,
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.stage = 'U6')
    COMMENT 'UCOs live (U6)' AS `Go Live UCOs`,

  -- Production+ (U5 or U6)
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.stage IN ('U5', 'U6'))
    COMMENT 'UCOs at production or live (U5+U6)' AS `Production+ UCOs`,

  -- Lost/Disqualified
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.stage = 'Lost')
    COMMENT 'UCOs lost' AS `Lost UCOs`,
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.stage = 'Disqualified')
    COMMENT 'UCOs disqualified' AS `Disqualified UCOs`,

  -- ========================================================================
  -- VELOCITY MEASURES (Days in Stage)
  -- ========================================================================

  AVG(uco.days_in_stage)
    COMMENT 'Average days UCOs have been in current stage' AS `Avg Days in Stage`,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY uco.days_in_stage)
    COMMENT 'Median days in current stage' AS `Median Days in Stage`,
  PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY uco.days_in_stage)
    COMMENT '90th percentile days in current stage' AS `P90 Days in Stage`,
  MAX(uco.days_in_stage)
    COMMENT 'Maximum days in current stage (stalled?)' AS `Max Days in Stage`,

  -- Average by Stage Category
  AVG(uco.days_in_stage) FILTER (WHERE uco.stage = 'U3')
    COMMENT 'Avg days in scoping (U3)' AS `Avg Days in Scoping`,
  AVG(uco.days_in_stage) FILTER (WHERE uco.stage = 'U4')
    COMMENT 'Avg days in confirming (U4)' AS `Avg Days in Confirming`,
  AVG(uco.days_in_stage) FILTER (WHERE uco.stage = 'U5')
    COMMENT 'Avg days in onboarding (U5)' AS `Avg Days in Onboarding`,

  -- ========================================================================
  -- MILESTONE RATES
  -- ========================================================================

  -- Tech Win Rate (reached U4+)
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.stage IN ('U4', 'U5', 'U6')) * 1.0
    / NULLIF(COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.stage NOT IN ('Lost', 'Disqualified')), 0)
    COMMENT 'Percentage of UCOs that reached tech win (U4+)' AS `Tech Win Rate`,

  -- Production Rate (reached U5+)
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.stage IN ('U5', 'U6')) * 1.0
    / NULLIF(COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.stage NOT IN ('Lost', 'Disqualified')), 0)
    COMMENT 'Percentage of UCOs that reached production (U5+)' AS `Production Rate`,

  -- Go Live Rate (reached U6)
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.stage = 'U6') * 1.0
    / NULLIF(COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.stage NOT IN ('Lost', 'Disqualified')), 0)
    COMMENT 'Percentage of UCOs that went live (U6)' AS `Go Live Rate`,

  -- Loss Rate
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.stage = 'Lost') * 1.0
    / NULLIF(COUNT(DISTINCT uco.usecase_id), 0)
    COMMENT 'Percentage of UCOs lost' AS `Loss Rate`,

  -- ========================================================================
  -- PIPELINE MEASURES BY STAGE
  -- ========================================================================

  SUM(uco.estimated_quarterly_dollar_dbus)
    COMMENT 'Total UCO estimated quarterly DBU dollars' AS `Total Quarterly DBUs`,
  SUM(curated.estimated_arr_usd)
    COMMENT 'Total estimated ARR from UCOs' AS `Total Estimated ARR`,
  SUM(uco.estimated_monthly_dollar_dbus)
    COMMENT 'Total monthly DBUs from UCOs' AS `Total Monthly DBUs`,

  -- Pipeline at Production+
  SUM(uco.estimated_quarterly_dollar_dbus) FILTER (WHERE uco.stage IN ('U5', 'U6'))
    COMMENT 'Quarterly DBUs at production (U5+U6)' AS `Production Quarterly DBUs`,
  SUM(curated.estimated_arr_usd) FILTER (WHERE uco.stage IN ('U5', 'U6'))
    COMMENT 'Estimated ARR at production (U5+U6)' AS `Production ARR`,

  -- ========================================================================
  -- STALLED UCO TRACKING
  -- ========================================================================

  -- Stalled UCOs (>30 days in same stage)
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.days_in_stage > 30 AND uco.stage NOT IN ('U6', 'Lost', 'Disqualified'))
    COMMENT 'UCOs stalled (>30 days in stage, excluding U6/Lost)' AS `Stalled UCOs`,
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.days_in_stage > 30 AND uco.stage NOT IN ('U6', 'Lost', 'Disqualified')) * 1.0
    / NULLIF(COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.stage NOT IN ('U6', 'Lost', 'Disqualified')), 0)
    COMMENT 'Percentage of in-progress UCOs stalled' AS `Stalled Rate`,

  -- Critical stalls (>60 days)
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.days_in_stage > 60 AND uco.stage NOT IN ('U6', 'Lost', 'Disqualified'))
    COMMENT 'UCOs critically stalled (>60 days)' AS `Critical Stalls`,

  -- Using stuck_in_stage flag if available
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.stuck_in_stage = TRUE)
    COMMENT 'UCOs flagged as stuck in stage' AS `Stuck UCOs`,

  -- ========================================================================
  -- ENGAGEMENT EFFICIENCY
  -- ========================================================================

  SUM(asq.actual_effort_in_days)
    COMMENT 'Total SSA effort days' AS `Total Effort Days`,
  SUM(uco.estimated_quarterly_dollar_dbus) / NULLIF(SUM(asq.actual_effort_in_days), 0)
    COMMENT 'Quarterly DBUs generated per effort day' AS `DBUs per Effort Day`,
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.stage IN ('U5', 'U6')) * 1.0
    / NULLIF(SUM(asq.actual_effort_in_days), 0)
    COMMENT 'Production UCOs per effort day' AS `Production UCOs per Effort Day`

FROM main.gtm_silver.approval_request_detail asq
LEFT JOIN main.gtm_silver.individual_hierarchy_salesforce hier
  ON asq.owner_user_id = hier.user_id
LEFT JOIN main.gtm_gold.account_obt ao
  ON asq.account_id = ao.account_id
  AND ao.fiscal_year_quarter = (
    SELECT MAX(fiscal_year_quarter) FROM main.gtm_gold.account_obt
  )
LEFT JOIN main.gtm_silver.use_case_detail uco
  ON asq.account_id = uco.account_id
LEFT JOIN main.gtm_gold.core_usecase_curated curated
  ON uco.usecase_id = curated.use_case_id
WHERE asq.snapshot_date = (
  SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail
)
  AND uco.usecase_id IS NOT NULL;  -- Only include ASQs with UCO linkage

-- ============================================================================
-- Sample Queries
-- ============================================================================
--
-- Velocity by Team:
-- SELECT `Manager L1`, MEASURE(`Total UCOs`), MEASURE(`Production+ UCOs`),
--        MEASURE(`Production Rate`), MEASURE(`Avg Days in Stage`)
-- FROM mv_uco_velocity
-- WHERE `Business Unit` = 'AMER Enterprise & Emerging'
-- GROUP BY ALL;
--
-- Stage Distribution by SSA:
-- SELECT `Owner`, MEASURE(`Scoping UCOs`), MEASURE(`Tech Win UCOs`),
--        MEASURE(`Production UCOs`), MEASURE(`Go Live UCOs`)
-- FROM mv_uco_velocity
-- WHERE `Manager L1` = 'Christopher Chalcraft'
-- GROUP BY ALL;
--
-- Stalled UCO Tracking:
-- SELECT `UCO Stage`, MEASURE(`Total UCOs`), MEASURE(`Avg Days in Stage`),
--        MEASURE(`Stalled UCOs`), MEASURE(`Stalled Rate`)
-- FROM mv_uco_velocity
-- WHERE `Fiscal Year` = 2026
-- GROUP BY ALL;
--
-- Production ARR by Specialization:
-- SELECT `Specialization`, MEASURE(`Total UCOs`), MEASURE(`Production+ UCOs`),
--        MEASURE(`Production Rate`), MEASURE(`Production ARR`)
-- FROM mv_uco_velocity
-- GROUP BY ALL
-- ORDER BY `Production ARR` DESC;
--
-- Time-to-Production Trend:
-- SELECT `Created Year-Quarter`, MEASURE(`Total UCOs`), MEASURE(`Production Rate`),
--        MEASURE(`Avg Days in Stage`), MEASURE(`Stalled Rate`)
-- FROM mv_uco_velocity
-- WHERE `Business Unit` = 'AMER Enterprise & Emerging'
-- GROUP BY ALL
-- ORDER BY `Created Year-Quarter`;
