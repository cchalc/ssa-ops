-- ============================================================================
-- mv_pipeline_impact - UCO Linkage & Pipeline Generation
-- ============================================================================
-- Connects SSA ASQ work to UCO (Use Case Opportunity) pipeline
-- Shows how SSA work influences pipe generation and deal progression
-- Sources: GTM Silver (ASQ, UCO) + GTM Gold (pipe gen)
-- NO HARDCODED FILTERS - all BU/manager filtering at query time
-- ============================================================================

CREATE OR REPLACE METRIC VIEW ${catalog}.${schema}.mv_pipeline_impact
COMMENT 'ASQ work linked to UCO pipeline and pipe generation. Shows how SSA engagements influence deal progression. Filter by business_unit, manager hierarchy, or time period.'
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

  -- Manager Hierarchy
  hier.manager_level_1_name
    COMMENT 'Direct manager (L1)' AS `Manager L1`,
  hier.manager_level_2_name
    COMMENT 'Second-level manager (L2)' AS `Manager L2`,

  -- ========================================================================
  -- CUSTOMER DIMENSIONS
  -- ========================================================================

  asq.account_id
    COMMENT 'Account ID' AS `Account ID`,
  asq.account_name
    COMMENT 'Customer account name' AS `Account`,

  -- ========================================================================
  -- WORK CLASSIFICATION DIMENSIONS
  -- ========================================================================

  asq.technical_specialization
    COMMENT 'Technical focus' AS `Specialization`,
  asq.support_type
    COMMENT 'Support type' AS `Support Type`,

  -- UCO Classification
  uco.use_case_stage
    COMMENT 'UCO stage' AS `UCO Stage`,
  uco.use_case_status
    COMMENT 'UCO status' AS `UCO Status`,
  uco.product_category
    COMMENT 'Product category' AS `Product Category`,

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
  -- ASQ VOLUME MEASURES
  -- ========================================================================

  COUNT(DISTINCT asq.approval_request_id)
    COMMENT 'Total number of ASQs' AS `Total ASQs`,
  COUNT(DISTINCT asq.approval_request_id) FILTER (WHERE asq.status IN ('Complete', 'Closed'))
    COMMENT 'Completed ASQs' AS `Completed ASQs`,

  -- ========================================================================
  -- LINKAGE MEASURES
  -- ========================================================================

  COUNT(DISTINCT asq.approval_request_id) FILTER (WHERE uco.use_case_id IS NOT NULL)
    COMMENT 'ASQs linked to a UCO' AS `Linked ASQs`,
  COUNT(DISTINCT asq.approval_request_id) FILTER (WHERE uco.use_case_id IS NOT NULL) * 1.0
    / NULLIF(COUNT(DISTINCT asq.approval_request_id), 0)
    COMMENT 'Percentage of ASQs with UCO linkage' AS `Linkage Rate`,

  COUNT(DISTINCT uco.use_case_id)
    COMMENT 'Unique UCOs linked to ASQs' AS `Linked UCOs`,
  COUNT(DISTINCT asq.account_id)
    COMMENT 'Unique accounts with ASQ engagement' AS `Engaged Accounts`,

  -- ========================================================================
  -- UCO PIPELINE MEASURES
  -- ========================================================================

  SUM(uco.amount)
    COMMENT 'Total UCO pipeline amount linked to ASQs' AS `Total Pipeline`,
  AVG(uco.amount)
    COMMENT 'Average UCO amount per linked ASQ' AS `Avg Pipeline per ASQ`,
  SUM(uco.amount) / NULLIF(COUNT(DISTINCT asq.approval_request_id) FILTER (WHERE uco.use_case_id IS NOT NULL), 0)
    COMMENT 'Pipeline per linked ASQ' AS `Pipeline per Linked ASQ`,

  -- ========================================================================
  -- UCO STAGE DISTRIBUTION
  -- ========================================================================

  COUNT(DISTINCT uco.use_case_id) FILTER (WHERE uco.use_case_stage = 'Discovery')
    COMMENT 'UCOs in Discovery stage' AS `Discovery UCOs`,
  COUNT(DISTINCT uco.use_case_id) FILTER (WHERE uco.use_case_stage = 'POC')
    COMMENT 'UCOs in POC stage' AS `POC UCOs`,
  COUNT(DISTINCT uco.use_case_id) FILTER (WHERE uco.use_case_stage = 'Validation')
    COMMENT 'UCOs in Validation stage' AS `Validation UCOs`,
  COUNT(DISTINCT uco.use_case_id) FILTER (WHERE uco.use_case_stage = 'Production')
    COMMENT 'UCOs in Production stage' AS `Production UCOs`,

  SUM(uco.amount) FILTER (WHERE uco.use_case_stage = 'Discovery')
    COMMENT 'Pipeline in Discovery' AS `Discovery Pipeline`,
  SUM(uco.amount) FILTER (WHERE uco.use_case_stage = 'POC')
    COMMENT 'Pipeline in POC' AS `POC Pipeline`,
  SUM(uco.amount) FILTER (WHERE uco.use_case_stage = 'Production')
    COMMENT 'Pipeline in Production' AS `Production Pipeline`,

  -- ========================================================================
  -- WIN MEASURES
  -- ========================================================================

  COUNT(DISTINCT uco.use_case_id) FILTER (WHERE uco.use_case_status = 'Closed Won')
    COMMENT 'UCOs closed won' AS `Won UCOs`,
  SUM(uco.amount) FILTER (WHERE uco.use_case_status = 'Closed Won')
    COMMENT 'Total won pipeline amount' AS `Won Pipeline`,
  COUNT(DISTINCT uco.use_case_id) FILTER (WHERE uco.use_case_status = 'Closed Won') * 1.0
    / NULLIF(COUNT(DISTINCT uco.use_case_id), 0)
    COMMENT 'UCO win rate' AS `Win Rate`,

  -- ========================================================================
  -- EFFORT TO PIPELINE EFFICIENCY
  -- ========================================================================

  SUM(asq.actual_effort_in_days)
    COMMENT 'Total SSA effort in days' AS `Total Effort Days`,
  SUM(uco.amount) / NULLIF(SUM(asq.actual_effort_in_days), 0)
    COMMENT 'Pipeline generated per effort day' AS `Pipeline per Effort Day`,
  SUM(uco.amount) FILTER (WHERE uco.use_case_status = 'Closed Won')
    / NULLIF(SUM(asq.actual_effort_in_days), 0)
    COMMENT 'Won pipeline per effort day' AS `Won Pipeline per Effort Day`

FROM main.gtm_silver.approval_request_detail asq
LEFT JOIN main.gtm_silver.individual_hierarchy_field hier
  ON asq.owner_user_id = hier.user_id
LEFT JOIN main.gtm_silver.use_case_detail uco
  ON asq.account_id = uco.account_id
  AND uco.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.use_case_detail)
WHERE asq.snapshot_date = (
  SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail
);

-- ============================================================================
-- Sample Queries
-- ============================================================================
--
-- Pipeline Impact by Team:
-- SELECT `Manager L1`, MEASURE(`Total ASQs`), MEASURE(`Linkage Rate`),
--        MEASURE(`Total Pipeline`), MEASURE(`Pipeline per Effort Day`)
-- FROM mv_pipeline_impact
-- WHERE `Business Unit` = 'AMER Enterprise & Emerging'
-- GROUP BY ALL;
--
-- UCO Stage Progression:
-- SELECT `UCO Stage`, MEASURE(`Linked UCOs`), MEASURE(`Total Pipeline`),
--        MEASURE(`Linked ASQs`)
-- FROM mv_pipeline_impact
-- WHERE `Fiscal Year` = 2026
-- GROUP BY ALL;
--
-- Win Rate by Specialization:
-- SELECT `Specialization`, MEASURE(`Linked UCOs`), MEASURE(`Won UCOs`),
--        MEASURE(`Win Rate`), MEASURE(`Won Pipeline`)
-- FROM mv_pipeline_impact
-- GROUP BY ALL
-- ORDER BY `Won Pipeline` DESC;
