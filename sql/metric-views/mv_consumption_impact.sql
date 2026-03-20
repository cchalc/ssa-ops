-- ============================================================================
-- mv_consumption_impact - ASQ Work Correlated with Consumption
-- ============================================================================
-- Connects SSA ASQ work to account consumption metrics from GTM Gold
-- Shows business impact of SSA engagements
-- Sources: GTM Silver (ASQ) + GTM Gold (consumption)
-- NO HARDCODED FILTERS - all BU/manager filtering at query time
-- ============================================================================

CREATE OR REPLACE METRIC VIEW ${catalog}.${schema}.mv_consumption_impact
COMMENT 'ASQ work correlated with account consumption metrics. Shows business impact of SSA engagements. Filter by business_unit, manager hierarchy, or time period.'
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
  ao.segment
    COMMENT 'Customer segment: ENT, COMM, MM, SMB' AS `Account Segment`,
  ao.vertical
    COMMENT 'Industry vertical' AS `Account Vertical`,
  ao.spend_tier
    COMMENT 'Account spend tier' AS `Spend Tier`,
  ao.adoption_tier
    COMMENT 'Product adoption tier' AS `Adoption Tier`,

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
  ao.fiscal_year_quarter
    COMMENT 'Fiscal year-quarter for consumption' AS `Consumption FYQ`,

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

  COUNT(1)
    COMMENT 'Total number of ASQs' AS `Total ASQs`,
  COUNT(1) FILTER (WHERE asq.status IN ('Complete', 'Closed'))
    COMMENT 'Completed ASQs' AS `Completed ASQs`,
  COUNT(DISTINCT asq.account_id)
    COMMENT 'Unique accounts with ASQ engagement' AS `Engaged Accounts`,

  -- ========================================================================
  -- CONSUMPTION MEASURES (from account_obt)
  -- ========================================================================

  SUM(ao.dbu_dollars_qtd)
    COMMENT 'Total DBU consumption QTD for engaged accounts' AS `Total DBU QTD`,
  AVG(ao.dbu_dollars_qtd)
    COMMENT 'Average DBU consumption QTD per account' AS `Avg DBU QTD`,
  SUM(ao.dbu_dollars_qtd) / NULLIF(COUNT(1), 0)
    COMMENT 'DBU consumption per ASQ' AS `DBU per ASQ`,

  SUM(ao.total_arr)
    COMMENT 'Total ARR for engaged accounts' AS `Total ARR`,
  AVG(ao.total_arr)
    COMMENT 'Average ARR per engaged account' AS `Avg Account ARR`,
  SUM(ao.total_arr) / NULLIF(COUNT(1), 0)
    COMMENT 'ARR per ASQ engagement' AS `ARR per ASQ`,

  -- ========================================================================
  -- CONSUMPTION GROWTH MEASURES
  -- ========================================================================

  AVG(ao.dbu_growth_qtd_pct)
    COMMENT 'Average DBU growth QTD %' AS `Avg DBU Growth QTD`,
  AVG(ao.dbu_growth_yoy_pct)
    COMMENT 'Average DBU growth YoY %' AS `Avg DBU Growth YoY`,

  -- ========================================================================
  -- EFFORT vs CONSUMPTION MEASURES
  -- ========================================================================

  SUM(asq.actual_effort_in_days)
    COMMENT 'Total SSA effort in days' AS `Total Effort Days`,
  SUM(ao.dbu_dollars_qtd) / NULLIF(SUM(asq.actual_effort_in_days), 0)
    COMMENT 'DBU consumption per effort day' AS `DBU per Effort Day`,
  SUM(ao.total_arr) / NULLIF(SUM(asq.actual_effort_in_days), 0)
    COMMENT 'ARR per effort day' AS `ARR per Effort Day`,

  -- ========================================================================
  -- ACCOUNT TIER DISTRIBUTION
  -- ========================================================================

  COUNT(1) FILTER (WHERE ao.spend_tier = 'Tier 1')
    COMMENT 'ASQs for Tier 1 accounts' AS `Tier 1 ASQs`,
  COUNT(1) FILTER (WHERE ao.spend_tier = 'Tier 2')
    COMMENT 'ASQs for Tier 2 accounts' AS `Tier 2 ASQs`,
  COUNT(1) FILTER (WHERE ao.spend_tier = 'Tier 3')
    COMMENT 'ASQs for Tier 3 accounts' AS `Tier 3 ASQs`,
  COUNT(1) FILTER (WHERE ao.spend_tier = 'Tier 4')
    COMMENT 'ASQs for Tier 4 accounts' AS `Tier 4 ASQs`,

  -- ========================================================================
  -- HIGH VALUE ENGAGEMENT
  -- ========================================================================

  COUNT(1) FILTER (WHERE ao.total_arr >= 1000000)
    COMMENT 'ASQs for accounts with ARR >= $1M' AS `$1M+ ARR ASQs`,
  COUNT(DISTINCT asq.account_id) FILTER (WHERE ao.total_arr >= 1000000)
    COMMENT 'Unique $1M+ ARR accounts engaged' AS `$1M+ ARR Accounts`,
  SUM(ao.dbu_dollars_qtd) FILTER (WHERE ao.total_arr >= 1000000)
    COMMENT 'DBU from $1M+ ARR accounts' AS `$1M+ Account DBU`

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
-- Business Impact by Team:
-- SELECT `Manager L1`, MEASURE(`Total ASQs`), MEASURE(`Total DBU QTD`),
--        MEASURE(`DBU per ASQ`), MEASURE(`ARR per Effort Day`)
-- FROM mv_consumption_impact
-- WHERE `Business Unit` = 'AMER Enterprise & Emerging'
-- GROUP BY ALL;
--
-- High Value Account Engagement:
-- SELECT `Spend Tier`, MEASURE(`Total ASQs`), MEASURE(`Engaged Accounts`),
--        MEASURE(`Total ARR`), MEASURE(`Avg DBU Growth YoY`)
-- FROM mv_consumption_impact
-- WHERE `Fiscal Year` = 2026
-- GROUP BY ALL;
--
-- Specialization Impact:
-- SELECT `Specialization`, MEASURE(`Total ASQs`), MEASURE(`Total Effort Days`),
--        MEASURE(`DBU per Effort Day`)
-- FROM mv_consumption_impact
-- GROUP BY ALL
-- ORDER BY `DBU per Effort Day` DESC;
