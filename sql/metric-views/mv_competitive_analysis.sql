-- ============================================================================
-- mv_competitive_analysis - Competitive Win/Loss Analysis
-- ============================================================================
-- Tracks competitive win rates for SSA-engaged opportunities
-- Charter Metric #2: Competitive Win Rate
-- Focus: Microsoft Fabric, Snowflake, AWS displacement tracking
-- Sources: GTM Silver (ASQ, use_case_detail, individual_hierarchy)
-- NO HARDCODED FILTERS - all BU/manager filtering at query time
-- ============================================================================

CREATE OR REPLACE METRIC VIEW ${catalog}.${schema}.mv_competitive_analysis
COMMENT 'Competitive win/loss analysis for SSA-engaged opportunities (Charter Metric #2). Track displacement of Microsoft Fabric, Snowflake, AWS, and other competitors.'
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
  -- COMPETITIVE DIMENSIONS
  -- ========================================================================

  uco.competitors
    COMMENT 'Competitor information from UCO (may contain multiple, semicolon-separated)' AS `Competitor`,
  -- Competitor Category (normalize competitor names based on actual values)
  CASE
    WHEN uco.competitors = 'No Competitor' THEN 'No Competitor'
    WHEN uco.competitors LIKE '%Microsoft Fabric%' THEN 'Microsoft Fabric'
    WHEN uco.competitors LIKE '%Microsoft Power BI%' THEN 'Microsoft Power BI'
    WHEN uco.competitors LIKE '%Azure Synapse%' THEN 'Microsoft Synapse'
    WHEN uco.competitors LIKE '%Azure%' THEN 'Microsoft Azure'
    WHEN uco.competitors LIKE '%Snowflake%' THEN 'Snowflake'
    WHEN uco.competitors LIKE '%AWS Redshift%' THEN 'AWS Redshift'
    WHEN uco.competitors LIKE '%AWS SageMaker%' OR uco.competitors LIKE '%AWS EMR%' OR uco.competitors LIKE '%AWS Glue%' THEN 'AWS'
    WHEN uco.competitors LIKE '%Google BigQuery%' THEN 'Google BigQuery'
    WHEN uco.competitors LIKE '%OpenAI%' OR uco.competitors LIKE '%Azure OpenAI%' THEN 'OpenAI/Azure OpenAI'
    WHEN uco.competitors LIKE '%Palantir%' THEN 'Palantir'
    WHEN uco.competitors = 'Other' THEN 'Other'
    WHEN uco.competitors IS NULL OR uco.competitors = '' THEN 'No Competitor'
    ELSE 'Other'
  END
    COMMENT 'Normalized competitor category' AS `Competitor Category`,
  -- Microsoft vs Snowflake vs AWS vs Other
  CASE
    WHEN uco.competitors LIKE '%Microsoft%' OR uco.competitors LIKE '%Azure%' OR uco.competitors LIKE '%Power BI%'
      THEN 'Microsoft'
    WHEN uco.competitors LIKE '%Snowflake%'
      THEN 'Snowflake'
    WHEN uco.competitors LIKE '%AWS%' OR uco.competitors LIKE '%Redshift%' OR uco.competitors LIKE '%SageMaker%'
      THEN 'AWS'
    WHEN uco.competitors LIKE '%Google%' OR uco.competitors LIKE '%BigQuery%'
      THEN 'Google Cloud'
    WHEN uco.competitors = 'No Competitor' OR uco.competitors IS NULL OR uco.competitors = ''
      THEN 'No Competitor'
    ELSE 'Other'
  END
    COMMENT 'Major competitor category: Microsoft, Snowflake, AWS, Google Cloud, Other' AS `Competitor Type`,
  -- Competitive Flag
  CASE
    WHEN uco.competitors IS NOT NULL AND uco.competitors != '' AND uco.competitors != 'No Competitor'
      THEN 'Competitive'
    ELSE 'Non-Competitive'
  END
    COMMENT 'Whether a competitor is involved' AS `Is Competitive`,

  -- ========================================================================
  -- UCO OUTCOME DIMENSIONS
  -- ========================================================================

  uco.stage
    COMMENT 'UCO stage: U1-U6, Lost, Disqualified' AS `UCO Stage`,
  uco.implementation_status
    COMMENT 'UCO implementation status: Green, Yellow, Red' AS `Implementation Status`,
  -- Outcome Category (based on stage, not status)
  CASE
    WHEN uco.stage = 'U6' THEN 'Won (Live)'
    WHEN uco.stage = 'Lost' THEN 'Lost'
    WHEN uco.stage = 'Disqualified' THEN 'Disqualified'
    WHEN uco.stage IN ('U5') THEN 'Production'
    WHEN uco.stage IN ('U3', 'U4') THEN 'Active (Tech Engaged)'
    WHEN uco.stage IN ('U1', 'U2') THEN 'Early Stage'
    ELSE 'Unknown'
  END
    COMMENT 'Outcome category: Won (Live), Production, Active, Early Stage, Lost, Disqualified' AS `Outcome`,

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
  -- UCO VOLUME MEASURES
  -- ========================================================================

  COUNT(DISTINCT uco.usecase_id)
    COMMENT 'Total unique UCOs' AS `Total UCOs`,
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.stage IN ('U6', 'Lost'))
    COMMENT 'Total resolved UCOs (live + lost)' AS `Total Resolved UCOs`,
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.competitors IS NOT NULL AND uco.competitors != '' AND uco.competitors != 'No Competitor')
    COMMENT 'UCOs with competitor involvement' AS `Competitive UCOs`,

  -- ========================================================================
  -- WIN/LOSS MEASURES (Win = U6/Live, Loss = Lost stage)
  -- ========================================================================

  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.stage = 'U6')
    COMMENT 'UCOs that reached Live (U6) - Wins' AS `Won UCOs`,
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.stage = 'Lost')
    COMMENT 'UCOs lost' AS `Lost UCOs`,

  -- Overall Win Rate (U6 vs Lost)
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.stage = 'U6') * 1.0
    / NULLIF(COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.stage IN ('U6', 'Lost')), 0)
    COMMENT 'Win rate (live / (live + lost))' AS `Win Rate`,

  -- Competitive Win/Loss
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.stage = 'U6'
    AND uco.competitors IS NOT NULL AND uco.competitors != '' AND uco.competitors != 'No Competitor')
    COMMENT 'Competitive wins (reached Live with competitor)' AS `Competitive Wins`,
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.stage = 'Lost'
    AND uco.competitors IS NOT NULL AND uco.competitors != '' AND uco.competitors != 'No Competitor')
    COMMENT 'Competitive losses' AS `Competitive Losses`,

  -- Competitive Win Rate
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.stage = 'U6'
    AND uco.competitors IS NOT NULL AND uco.competitors != '' AND uco.competitors != 'No Competitor') * 1.0
    / NULLIF(COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.stage IN ('U6', 'Lost')
    AND uco.competitors IS NOT NULL AND uco.competitors != '' AND uco.competitors != 'No Competitor'), 0)
    COMMENT 'Competitive win rate (competitive wins / competitive resolved)' AS `Competitive Win Rate`,

  -- ========================================================================
  -- PIPELINE VALUE MEASURES (using quarterly DBUs)
  -- ========================================================================

  SUM(uco.estimated_quarterly_dollar_dbus)
    COMMENT 'Total UCO estimated quarterly DBU dollars' AS `Total Quarterly DBUs`,
  SUM(uco.estimated_quarterly_dollar_dbus) FILTER (WHERE uco.stage = 'U6')
    COMMENT 'Quarterly DBUs for Live UCOs' AS `Won Quarterly DBUs`,
  SUM(uco.estimated_quarterly_dollar_dbus) FILTER (WHERE uco.stage = 'Lost')
    COMMENT 'Quarterly DBUs for Lost UCOs' AS `Lost Quarterly DBUs`,

  -- Competitive Pipeline
  SUM(uco.estimated_quarterly_dollar_dbus) FILTER (WHERE uco.competitors IS NOT NULL AND uco.competitors != '' AND uco.competitors != 'No Competitor')
    COMMENT 'Quarterly DBUs with competitor involvement' AS `Competitive Quarterly DBUs`,
  SUM(uco.estimated_quarterly_dollar_dbus) FILTER (WHERE uco.stage = 'U6'
    AND uco.competitors IS NOT NULL AND uco.competitors != '' AND uco.competitors != 'No Competitor')
    COMMENT 'Won competitive quarterly DBUs' AS `Won Competitive Quarterly DBUs`,
  SUM(uco.estimated_quarterly_dollar_dbus) FILTER (WHERE uco.stage = 'Lost'
    AND uco.competitors IS NOT NULL AND uco.competitors != '' AND uco.competitors != 'No Competitor')
    COMMENT 'Lost competitive quarterly DBUs' AS `Lost Competitive Quarterly DBUs`,

  -- ========================================================================
  -- DISPLACEMENT MEASURES (Microsoft Fabric, Snowflake, AWS)
  -- ========================================================================

  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.stage = 'U6'
    AND (uco.competitors LIKE '%Microsoft Fabric%'
      OR uco.competitors LIKE '%Azure Synapse%'
      OR uco.competitors LIKE '%Microsoft Power BI%'))
    COMMENT 'Microsoft displacement wins (Fabric/Synapse/Power BI)' AS `Microsoft Displacement Wins`,
  SUM(uco.estimated_quarterly_dollar_dbus) FILTER (WHERE uco.stage = 'U6'
    AND (uco.competitors LIKE '%Microsoft Fabric%'
      OR uco.competitors LIKE '%Azure Synapse%'
      OR uco.competitors LIKE '%Microsoft Power BI%'))
    COMMENT 'Microsoft displacement quarterly DBUs won' AS `Microsoft Displacement DBUs`,

  -- Snowflake Displacement
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.stage = 'U6'
    AND uco.competitors LIKE '%Snowflake%')
    COMMENT 'Snowflake displacement wins' AS `Snowflake Displacement Wins`,
  SUM(uco.estimated_quarterly_dollar_dbus) FILTER (WHERE uco.stage = 'U6'
    AND uco.competitors LIKE '%Snowflake%')
    COMMENT 'Snowflake displacement quarterly DBUs won' AS `Snowflake Displacement DBUs`,

  -- ========================================================================
  -- ASQ ENGAGEMENT MEASURES
  -- ========================================================================

  COUNT(DISTINCT asq.approval_request_id)
    COMMENT 'Total ASQs linked to UCOs' AS `Total ASQs`,
  SUM(asq.actual_effort_in_days)
    COMMENT 'Total SSA effort days' AS `Total Effort Days`,
  SUM(uco.estimated_quarterly_dollar_dbus) FILTER (WHERE uco.stage = 'U6')
    / NULLIF(SUM(asq.actual_effort_in_days), 0)
    COMMENT 'Won quarterly DBUs per effort day' AS `Won DBUs per Effort Day`,

  -- ========================================================================
  -- DISTRIBUTION MEASURES
  -- ========================================================================

  COUNT(DISTINCT asq.account_id)
    COMMENT 'Unique accounts with UCO linkage' AS `Accounts with UCOs`,
  COUNT(DISTINCT asq.account_id) FILTER (WHERE uco.competitors IS NOT NULL AND uco.competitors != '' AND uco.competitors != 'No Competitor')
    COMMENT 'Unique accounts in competitive deals' AS `Competitive Accounts`,
  COUNT(DISTINCT asq.owner_user_id)
    COMMENT 'Unique SSAs engaged' AS `SSAs Engaged`

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
WHERE asq.snapshot_date = (
  SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail
)
  AND uco.usecase_id IS NOT NULL;  -- Only include ASQs with UCO linkage

-- ============================================================================
-- Sample Queries
-- ============================================================================
--
-- Win Rate by SSA:
-- SELECT `Owner`, MEASURE(`Total Closed UCOs`), MEASURE(`Won UCOs`),
--        MEASURE(`Win Rate`), MEASURE(`Won Pipeline`)
-- FROM mv_competitive_analysis
-- WHERE `Manager L1` = 'Christopher Chalcraft'
-- GROUP BY ALL
-- ORDER BY `Win Rate` DESC;
--
-- Competitive Performance:
-- SELECT `Competitor Category`, MEASURE(`Competitive UCOs`), MEASURE(`Competitive Wins`),
--        MEASURE(`Competitive Win Rate`), MEASURE(`Won Competitive Pipeline`)
-- FROM mv_competitive_analysis
-- WHERE `Competitor Category` != 'No Competitor'
-- GROUP BY ALL
-- ORDER BY `Competitive UCOs` DESC;
--
-- Microsoft Displacement:
-- SELECT `Manager L1`, MEASURE(`Microsoft Displacement Wins`), MEASURE(`Microsoft Displacement Pipeline`)
-- FROM mv_competitive_analysis
-- WHERE `Business Unit` = 'AMER Enterprise & Emerging'
-- GROUP BY ALL
-- ORDER BY `Microsoft Displacement Pipeline` DESC;
--
-- Win Rate Trend:
-- SELECT `Created Year-Quarter`, MEASURE(`Total Closed UCOs`), MEASURE(`Win Rate`),
--        MEASURE(`Competitive Win Rate`)
-- FROM mv_competitive_analysis
-- GROUP BY ALL
-- ORDER BY `Created Year-Quarter`;
--
-- Win Rate by Vertical:
-- SELECT `Account Vertical`, MEASURE(`Total Closed UCOs`), MEASURE(`Win Rate`),
--        MEASURE(`Won Pipeline`)
-- FROM mv_competitive_analysis
-- WHERE `Fiscal Year` = 2026
-- GROUP BY ALL
-- ORDER BY `Won Pipeline` DESC;
