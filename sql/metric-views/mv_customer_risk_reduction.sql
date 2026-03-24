-- ============================================================================
-- mv_customer_risk_reduction - Competitive Wins & Risk Mitigation
-- ============================================================================
-- Charter Metric #8: Customer Risk Reduction
-- Tracks competitive displacement wins and risk-related ASQ engagements
-- Focus: Compete scenarios, migration ASQs, churn/mitigation-tagged work
-- Sources: GTM Silver (ASQ, UCO with competitor data, individual_hierarchy)
-- NO HARDCODED FILTERS - all BU/manager filtering at query time
-- ============================================================================

CREATE OR REPLACE METRIC VIEW home_christopher_chalcraft.cjc_views.mv_customer_risk_reduction
COMMENT 'Customer risk reduction metrics. Charter Metric #8. Tracks competitive wins, displacement scenarios, and risk mitigation ASQs.'
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
    COMMENT 'SSA owner ID' AS `Owner ID`,

  -- Manager Hierarchy
  hier.line_manager_name
    COMMENT 'Direct manager (L1)' AS `Manager L1`,
  hier.`2nd_line_manager_name`
    COMMENT 'Second-level manager (L2)' AS `Manager L2`,

  -- ========================================================================
  -- CUSTOMER DIMENSIONS
  -- ========================================================================

  asq.account_id
    COMMENT 'Account ID' AS `Account ID`,
  asq.account_name
    COMMENT 'Customer account name' AS `Account`,

  -- ========================================================================
  -- RISK CONTEXT DIMENSIONS
  -- ========================================================================

  -- Risk Context Classification (based on ASQ description/type)
  CASE
    WHEN LOWER(asq.support_type) LIKE '%migration%' THEN 'Migration'
    WHEN LOWER(asq.support_type) LIKE '%competitive%' THEN 'Competitive Review'
    WHEN LOWER(asq.request_description) LIKE '%churn%' THEN 'Churn Risk'
    WHEN LOWER(asq.request_description) LIKE '%mitigation%' THEN 'Mitigation'
    WHEN LOWER(asq.request_description) LIKE '%at risk%' THEN 'At Risk'
    WHEN LOWER(asq.request_description) LIKE '%competitive%' THEN 'Competitive'
    WHEN LOWER(asq.request_description) LIKE '%displacement%' THEN 'Displacement'
    WHEN LOWER(asq.request_description) LIKE '%snowflake%' THEN 'Snowflake Compete'
    WHEN LOWER(asq.request_description) LIKE '%fabric%' THEN 'Microsoft Compete'
    WHEN LOWER(asq.request_description) LIKE '%synapse%' THEN 'Microsoft Compete'
    WHEN uco.competitor_status = 'Active' THEN 'Active Compete'
    WHEN uco.primary_competitor IS NOT NULL THEN 'Has Competitor'
    ELSE 'Standard'
  END
    COMMENT 'Risk context classification' AS `Risk Context`,

  -- Competitor Classification
  CASE
    WHEN uco.primary_competitor LIKE '%Microsoft%' OR uco.primary_competitor LIKE '%Fabric%'
         OR uco.primary_competitor LIKE '%Synapse%' OR uco.primary_competitor LIKE '%Power BI%' THEN 'Microsoft'
    WHEN uco.primary_competitor LIKE '%Snowflake%' THEN 'Snowflake'
    WHEN uco.primary_competitor LIKE '%AWS%' OR uco.primary_competitor LIKE '%Redshift%'
         OR uco.primary_competitor LIKE '%Glue%' THEN 'AWS'
    WHEN uco.primary_competitor LIKE '%Google%' OR uco.primary_competitor LIKE '%BigQuery%' THEN 'Google Cloud'
    WHEN uco.primary_competitor IS NOT NULL THEN 'Other Competitor'
    ELSE 'No Competitor'
  END
    COMMENT 'Primary competitor category' AS `Competitor Category`,

  uco.primary_competitor
    COMMENT 'Primary competitor name' AS `Primary Competitor`,
  uco.competitor_status
    COMMENT 'Competitor status: Active, Won, Lost' AS `Competitor Status`,
  uco.stage
    COMMENT 'UCO stage' AS `UCO Stage`,

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

  -- ========================================================================
  -- RISK ASQ VOLUME MEASURES
  -- ========================================================================

  COUNT(DISTINCT asq.approval_request_id)
    COMMENT 'Total ASQs' AS `Total ASQs`,

  -- Risk-Related ASQs
  COUNT(DISTINCT asq.approval_request_id) FILTER (WHERE
    LOWER(asq.support_type) LIKE '%migration%'
    OR LOWER(asq.support_type) LIKE '%competitive%'
    OR LOWER(asq.request_description) LIKE '%churn%'
    OR LOWER(asq.request_description) LIKE '%mitigation%'
    OR LOWER(asq.request_description) LIKE '%at risk%'
    OR LOWER(asq.request_description) LIKE '%competitive%'
    OR uco.competitor_status = 'Active')
    COMMENT 'ASQs with risk/competitive context' AS `Risk-Related ASQs`,

  -- Migration ASQs
  COUNT(DISTINCT asq.approval_request_id) FILTER (WHERE
    LOWER(asq.support_type) LIKE '%migration%'
    OR LOWER(asq.request_description) LIKE '%migration%')
    COMMENT 'Migration-related ASQs' AS `Migration ASQs`,

  -- Churn/Mitigation ASQs
  COUNT(DISTINCT asq.approval_request_id) FILTER (WHERE
    LOWER(asq.request_description) LIKE '%churn%'
    OR LOWER(asq.request_description) LIKE '%mitigation%'
    OR LOWER(asq.request_description) LIKE '%at risk%')
    COMMENT 'Churn risk or mitigation ASQs' AS `Churn Mitigation ASQs`,

  -- ========================================================================
  -- COMPETITIVE UCO MEASURES
  -- ========================================================================

  COUNT(DISTINCT uco.usecase_id)
    COMMENT 'Total UCOs' AS `Total UCOs`,

  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.primary_competitor IS NOT NULL)
    COMMENT 'UCOs with competitor identified' AS `Competitive UCOs`,

  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.competitor_status = 'Active')
    COMMENT 'UCOs with active competitor' AS `Active Compete UCOs`,

  -- ========================================================================
  -- COMPETITIVE WIN MEASURES
  -- ========================================================================

  -- Wins (UCOs at U5/U6 with competitor)
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE
    uco.primary_competitor IS NOT NULL AND uco.stage IN ('U5', 'U6'))
    COMMENT 'Competitive wins (UCO at production with competitor)' AS `Competitive Wins`,

  -- Losses
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE
    uco.primary_competitor IS NOT NULL AND uco.stage = 'Lost')
    COMMENT 'Competitive losses' AS `Competitive Losses`,

  -- Win Rate
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE
    uco.primary_competitor IS NOT NULL AND uco.stage IN ('U5', 'U6')) * 1.0
    / NULLIF(COUNT(DISTINCT uco.usecase_id) FILTER (WHERE
        uco.primary_competitor IS NOT NULL AND uco.stage IN ('U5', 'U6', 'Lost')), 0)
    COMMENT 'Competitive win rate (wins / (wins + losses))' AS `Competitive Win Rate`,

  -- ========================================================================
  -- DISPLACEMENT WINS BY COMPETITOR
  -- ========================================================================

  -- Microsoft Displacements
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE
    (uco.primary_competitor LIKE '%Microsoft%' OR uco.primary_competitor LIKE '%Fabric%'
     OR uco.primary_competitor LIKE '%Synapse%' OR uco.primary_competitor LIKE '%Power BI%')
    AND uco.stage IN ('U5', 'U6'))
    COMMENT 'Wins against Microsoft (Fabric/Synapse/Power BI)' AS `Microsoft Displacement Wins`,

  -- Snowflake Displacements
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE
    uco.primary_competitor LIKE '%Snowflake%'
    AND uco.stage IN ('U5', 'U6'))
    COMMENT 'Wins against Snowflake' AS `Snowflake Displacement Wins`,

  -- AWS Displacements
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE
    (uco.primary_competitor LIKE '%AWS%' OR uco.primary_competitor LIKE '%Redshift%')
    AND uco.stage IN ('U5', 'U6'))
    COMMENT 'Wins against AWS (Redshift/Glue)' AS `AWS Displacement Wins`,

  -- ========================================================================
  -- RISK RESOLUTION MEASURES
  -- ========================================================================

  -- Risk ASQs resolved (completed)
  COUNT(DISTINCT asq.approval_request_id) FILTER (WHERE
    asq.status IN ('Complete', 'Completed', 'Closed')
    AND (LOWER(asq.request_description) LIKE '%churn%'
         OR LOWER(asq.request_description) LIKE '%mitigation%'
         OR LOWER(asq.request_description) LIKE '%at risk%'
         OR uco.competitor_status = 'Active'))
    COMMENT 'Risk-related ASQs completed' AS `Risk ASQs Resolved`,

  -- Risk resolution rate
  COUNT(DISTINCT asq.approval_request_id) FILTER (WHERE
    asq.status IN ('Complete', 'Completed', 'Closed')
    AND (LOWER(asq.request_description) LIKE '%churn%'
         OR LOWER(asq.request_description) LIKE '%mitigation%'
         OR uco.competitor_status = 'Active')) * 1.0
    / NULLIF(COUNT(DISTINCT asq.approval_request_id) FILTER (WHERE
        LOWER(asq.request_description) LIKE '%churn%'
        OR LOWER(asq.request_description) LIKE '%mitigation%'
        OR uco.competitor_status = 'Active'), 0)
    COMMENT 'Risk resolution rate (completed / total risk ASQs)' AS `Risk Resolution Rate`,

  -- ========================================================================
  -- PIPELINE PROTECTED (RISK UCOs REACHING PRODUCTION)
  -- ========================================================================

  SUM(uco.estimated_quarterly_dollar_dbus) FILTER (WHERE
    uco.primary_competitor IS NOT NULL AND uco.stage IN ('U5', 'U6'))
    COMMENT 'Quarterly DBUs from competitive wins' AS `Competitive Win DBUs`,

  SUM(curated.estimated_arr_usd) FILTER (WHERE
    uco.primary_competitor IS NOT NULL AND uco.stage IN ('U5', 'U6'))
    COMMENT 'ARR from competitive wins' AS `Competitive Win ARR`,

  -- Pipeline at risk (active compete not yet won)
  SUM(uco.estimated_quarterly_dollar_dbus) FILTER (WHERE
    uco.competitor_status = 'Active' AND uco.stage IN ('U3', 'U4'))
    COMMENT 'Quarterly DBUs at risk (active compete in progress)' AS `At Risk Pipeline DBUs`,

  -- ========================================================================
  -- EFFORT EFFICIENCY
  -- ========================================================================

  SUM(asq.actual_effort_in_days)
    COMMENT 'Total SSA effort days' AS `Total Effort Days`,

  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE
    uco.primary_competitor IS NOT NULL AND uco.stage IN ('U5', 'U6')) * 1.0
    / NULLIF(SUM(asq.actual_effort_in_days), 0)
    COMMENT 'Competitive wins per effort day' AS `Competitive Wins per Effort Day`

FROM main.gtm_silver.approval_request_detail asq
LEFT JOIN main.gtm_silver.individual_hierarchy_salesforce hier
  ON asq.owner_user_id = hier.user_id
LEFT JOIN main.gtm_silver.use_case_detail uco
  ON asq.account_id = uco.account_id
  AND uco.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.use_case_detail)
  AND uco.is_active_ind = true
LEFT JOIN main.gtm_gold.core_usecase_curated curated
  ON uco.usecase_id = curated.use_case_id
WHERE asq.snapshot_date = (
  SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail
);

-- ============================================================================
-- Sample Queries
-- ============================================================================
--
-- Risk Reduction by Team:
-- SELECT `Manager L1`, MEASURE(`Risk-Related ASQs`), MEASURE(`Competitive Wins`),
--        MEASURE(`Competitive Win Rate`), MEASURE(`Risk Resolution Rate`)
-- FROM mv_customer_risk_reduction
-- WHERE `Business Unit` = 'AMER Enterprise & Emerging'
-- GROUP BY ALL;
--
-- Displacement Wins by Competitor:
-- SELECT `Competitor Category`, MEASURE(`Competitive UCOs`), MEASURE(`Competitive Wins`),
--        MEASURE(`Competitive Losses`), MEASURE(`Competitive Win Rate`)
-- FROM mv_customer_risk_reduction
-- WHERE `Competitor Category` != 'No Competitor'
-- GROUP BY ALL
-- ORDER BY `Competitive Wins` DESC;
--
-- Microsoft Displacement Leaderboard:
-- SELECT `Owner`, MEASURE(`Microsoft Displacement Wins`),
--        MEASURE(`Snowflake Displacement Wins`), MEASURE(`AWS Displacement Wins`)
-- FROM mv_customer_risk_reduction
-- WHERE `Fiscal Year` = 2026
-- GROUP BY ALL
-- ORDER BY `Microsoft Displacement Wins` DESC;
--
-- Risk Context Distribution:
-- SELECT `Risk Context`, MEASURE(`Total ASQs`), MEASURE(`Risk ASQs Resolved`),
--        MEASURE(`Risk Resolution Rate`)
-- FROM mv_customer_risk_reduction
-- GROUP BY ALL
-- ORDER BY `Total ASQs` DESC;
--
-- Pipeline Protection:
-- SELECT `Manager L2`, MEASURE(`Competitive Win ARR`), MEASURE(`Competitive Win DBUs`),
--        MEASURE(`At Risk Pipeline DBUs`)
-- FROM mv_customer_risk_reduction
-- WHERE `Business Unit` = 'AMER Enterprise & Emerging'
-- GROUP BY ALL;
