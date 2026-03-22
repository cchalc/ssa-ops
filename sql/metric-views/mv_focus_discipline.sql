-- ============================================================================
-- mv_focus_discipline - Focus & Discipline Metrics (80% Priority Target)
-- ============================================================================
-- Tracks SSA effort allocation against strategic account tiers
-- Charter Metric #9: Focus & Discipline (80% of effort on priority accounts)
-- Priority = A+/A tier, Focus Account, OR Strategic Account
-- Sources: GTM Silver (ASQ, individual_hierarchy) + GTM Gold (rpt_account_dim, account_obt)
-- NO HARDCODED FILTERS - all BU/manager filtering at query time
-- ============================================================================

CREATE OR REPLACE METRIC VIEW ${catalog}.${schema}.mv_focus_discipline
COMMENT 'Focus & Discipline metrics. Track 80% priority effort target (Charter Metric #9). Priority = A+/A tier, Focus Account, or Strategic. Filter by business_unit, manager hierarchy, or time period.'
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
  hier.Region_Level_1
    COMMENT 'Hierarchy region level 1' AS `Hierarchy Region`,

  -- ========================================================================
  -- CUSTOMER DIMENSIONS
  -- ========================================================================

  asq.account_id
    COMMENT 'Account ID' AS `Account ID`,
  asq.account_name
    COMMENT 'Customer account name' AS `Account`,
  ao.account_segment
    COMMENT 'Customer segment: Strategic, Named, Geo, Emerging Enterprise, Startup' AS `Account Segment`,
  ao.vertical_segment
    COMMENT 'Industry vertical' AS `Account Vertical`,
  ao.spend_tier
    COMMENT 'Spend tier: Scaling, Ramping, Greenfield Prospect, Greenfield PAYG' AS `Spend Tier`,

  -- ========================================================================
  -- ACCOUNT SEGMENTATION DIMENSIONS (from rpt_account_dim)
  -- ========================================================================

  acct.account_tier
    COMMENT 'Account tier: A+, A, B, C, Focus Account' AS `Account Tier`,
  CASE WHEN acct.is_strategic_account_ind THEN 'Yes' ELSE 'No' END
    COMMENT 'Is strategic account' AS `Is Strategic`,
  CASE WHEN acct.is_focus_account_ind THEN 'Yes' ELSE 'No' END
    COMMENT 'Is focus account' AS `Is Focus Account`,
  -- Priority Status (A+/A tier, Focus Account, OR Strategic Account)
  CASE
    WHEN acct.account_tier IN ('A+', 'A')
      OR acct.account_tier LIKE 'Focus Account%'
      OR acct.is_strategic_account_ind = TRUE
    THEN 'Priority'
    ELSE 'Non-Priority'
  END
    COMMENT 'Priority status based on A+/A tier, Focus Account, or Strategic' AS `Priority Status`,
  -- Strategic tier grouping
  CASE
    WHEN acct.account_tier = 'A+' THEN 'A+ (Top Strategic)'
    WHEN acct.account_tier = 'A' THEN 'A (Strategic)'
    WHEN acct.account_tier LIKE 'Focus Account%' THEN 'Focus Account'
    WHEN acct.is_strategic_account_ind = TRUE THEN 'Strategic (Flagged)'
    WHEN acct.account_tier = 'B' THEN 'B (Important)'
    WHEN acct.account_tier = 'C' THEN 'C (Standard)'
    ELSE 'Untiered'
  END
    COMMENT 'Strategic account tier grouping' AS `Strategic Tier`,

  -- ========================================================================
  -- WORK CLASSIFICATION DIMENSIONS
  -- ========================================================================

  asq.technical_specialization
    COMMENT 'Technical focus: Data Science, Data Engineering, Platform, etc.' AS `Specialization`,
  asq.support_type
    COMMENT 'Support type' AS `Support Type`,
  asq.status
    COMMENT 'ASQ status' AS `Status`,

  -- ========================================================================
  -- TIME DIMENSIONS
  -- ========================================================================

  YEAR(asq.created_date)
    COMMENT 'Calendar year' AS `Created Year`,
  CONCAT(YEAR(asq.created_date), '-Q', QUARTER(asq.created_date))
    COMMENT 'Calendar year-quarter' AS `Created Year-Quarter`,
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
  -- VOLUME MEASURES
  -- ========================================================================

  COUNT(1)
    COMMENT 'Total number of ASQs' AS `Total ASQs`,
  COUNT(1) FILTER (WHERE acct.account_tier IN ('A+', 'A') OR acct.account_tier LIKE 'Focus Account%' OR acct.is_strategic_account_ind = TRUE)
    COMMENT 'ASQs on priority accounts (A+/A/Focus/Strategic)' AS `Priority ASQs`,
  COUNT(1) FILTER (WHERE NOT (acct.account_tier IN ('A+', 'A') OR acct.account_tier LIKE 'Focus Account%' OR acct.is_strategic_account_ind = TRUE))
    COMMENT 'ASQs on non-priority accounts' AS `Non-Priority ASQs`,
  COUNT(1) FILTER (WHERE acct.account_tier = 'A+')
    COMMENT 'ASQs on A+ strategic accounts' AS `A+ ASQs`,
  COUNT(1) FILTER (WHERE acct.account_tier = 'A')
    COMMENT 'ASQs on A tier accounts' AS `A Tier ASQs`,
  COUNT(1) FILTER (WHERE acct.account_tier LIKE 'Focus Account%')
    COMMENT 'ASQs on Focus Accounts' AS `Focus Account ASQs`,
  COUNT(1) FILTER (WHERE acct.is_strategic_account_ind = TRUE)
    COMMENT 'ASQs on Strategic Accounts' AS `Strategic ASQs`,

  -- ========================================================================
  -- EFFORT MEASURES
  -- ========================================================================

  SUM(COALESCE(asq.actual_effort_in_days, asq.estimated_effort_in_days, 5))
    COMMENT 'Total effort days (actual or estimated, default 5)' AS `Total Effort Days`,
  SUM(COALESCE(asq.actual_effort_in_days, asq.estimated_effort_in_days, 5))
    FILTER (WHERE acct.account_tier IN ('A+', 'A') OR acct.account_tier LIKE 'Focus Account%' OR acct.is_strategic_account_ind = TRUE)
    COMMENT 'Effort days on priority accounts (A+/A/Focus/Strategic)' AS `Priority Effort Days`,
  SUM(COALESCE(asq.actual_effort_in_days, asq.estimated_effort_in_days, 5))
    FILTER (WHERE NOT (acct.account_tier IN ('A+', 'A') OR acct.account_tier LIKE 'Focus Account%' OR acct.is_strategic_account_ind = TRUE))
    COMMENT 'Effort days on non-priority accounts' AS `Non-Priority Effort Days`,

  -- By Tier
  SUM(COALESCE(asq.actual_effort_in_days, asq.estimated_effort_in_days, 5))
    FILTER (WHERE acct.account_tier = 'A+')
    COMMENT 'Effort days on A+ accounts' AS `A+ Effort Days`,
  SUM(COALESCE(asq.actual_effort_in_days, asq.estimated_effort_in_days, 5))
    FILTER (WHERE acct.account_tier = 'A')
    COMMENT 'Effort days on A tier accounts' AS `A Tier Effort Days`,
  SUM(COALESCE(asq.actual_effort_in_days, asq.estimated_effort_in_days, 5))
    FILTER (WHERE acct.account_tier LIKE 'Focus Account%')
    COMMENT 'Effort days on Focus Accounts' AS `Focus Account Effort Days`,
  SUM(COALESCE(asq.actual_effort_in_days, asq.estimated_effort_in_days, 5))
    FILTER (WHERE acct.is_strategic_account_ind = TRUE)
    COMMENT 'Effort days on Strategic Accounts' AS `Strategic Effort Days`,

  -- ========================================================================
  -- FOCUS RATE MEASURES (80% Target)
  -- ========================================================================

  -- Priority Effort Rate (primary KPI)
  SUM(COALESCE(asq.actual_effort_in_days, asq.estimated_effort_in_days, 5))
    FILTER (WHERE acct.account_tier IN ('A+', 'A') OR acct.account_tier LIKE 'Focus Account%' OR acct.is_strategic_account_ind = TRUE) * 1.0
    / NULLIF(SUM(COALESCE(asq.actual_effort_in_days, asq.estimated_effort_in_days, 5)), 0)
    COMMENT 'Percentage of effort on priority accounts (target: >= 80%)' AS `Priority Effort Rate`,

  -- Meeting 80% Goal flag
  CASE
    WHEN SUM(COALESCE(asq.actual_effort_in_days, asq.estimated_effort_in_days, 5))
           FILTER (WHERE acct.account_tier IN ('A+', 'A') OR acct.account_tier LIKE 'Focus Account%' OR acct.is_strategic_account_ind = TRUE) * 1.0
           / NULLIF(SUM(COALESCE(asq.actual_effort_in_days, asq.estimated_effort_in_days, 5)), 0) >= 0.8
    THEN 1 ELSE 0
  END
    COMMENT '1 if meeting 80% priority focus target, 0 otherwise' AS `Meeting 80% Goal`,

  -- Priority ASQ Rate
  COUNT(1) FILTER (WHERE acct.account_tier IN ('A+', 'A') OR acct.account_tier LIKE 'Focus Account%' OR acct.is_strategic_account_ind = TRUE) * 1.0
    / NULLIF(COUNT(1), 0)
    COMMENT 'Percentage of ASQs on priority accounts' AS `Priority ASQ Rate`,

  -- ========================================================================
  -- DISTRIBUTION MEASURES
  -- ========================================================================

  COUNT(DISTINCT asq.account_id)
    COMMENT 'Unique accounts with ASQ engagement' AS `Unique Accounts`,
  COUNT(DISTINCT asq.account_id) FILTER (WHERE acct.account_tier IN ('A+', 'A') OR acct.account_tier LIKE 'Focus Account%' OR acct.is_strategic_account_ind = TRUE)
    COMMENT 'Unique priority accounts engaged' AS `Priority Accounts Engaged`,
  COUNT(DISTINCT asq.owner_user_id)
    COMMENT 'Unique SSAs' AS `Unique SSAs`,

  -- ========================================================================
  -- CONSUMPTION LINKAGE (Impact of Focus)
  -- ========================================================================

  SUM(ao.dbu_dollars_qtd)
    COMMENT 'Total DBU consumption (QTD) for engaged accounts' AS `Engaged Account DBU QTD`,
  SUM(ao.dbu_dollars_qtd) FILTER (WHERE acct.account_tier IN ('A+', 'A') OR acct.account_tier LIKE 'Focus Account%' OR acct.is_strategic_account_ind = TRUE)
    COMMENT 'DBU consumption (QTD) for priority accounts' AS `Priority Account DBU QTD`,
  AVG(ao.dbu_dollars_qtd) FILTER (WHERE acct.account_tier IN ('A+', 'A') OR acct.account_tier LIKE 'Focus Account%' OR acct.is_strategic_account_ind = TRUE)
    COMMENT 'Avg DBU consumption for priority accounts' AS `Avg Priority Account DBU`

FROM main.gtm_silver.approval_request_detail asq
LEFT JOIN main.gtm_silver.individual_hierarchy_salesforce hier
  ON asq.owner_user_id = hier.user_id
LEFT JOIN main.gtm_gold.account_obt ao
  ON asq.account_id = ao.account_id
  AND ao.fiscal_year_quarter = (
    SELECT MAX(fiscal_year_quarter) FROM main.gtm_gold.account_obt
  )
LEFT JOIN main.gtm_gold.rpt_account_dim acct
  ON asq.account_id = acct.account_id
WHERE asq.snapshot_date = (
  SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail
)
  AND asq.status IN ('In Progress', 'Complete', 'Closed', 'Completed', '3. Delivered', 'Delivered');

-- ============================================================================
-- Sample Queries
-- ============================================================================
--
-- Focus by SSA (for manager review):
-- SELECT `Owner`, MEASURE(`Total Effort Days`), MEASURE(`Priority Effort Days`),
--        MEASURE(`Priority Effort Rate`), MEASURE(`Meeting 80% Goal`)
-- FROM mv_focus_discipline
-- WHERE `Manager L1` = 'Christopher Chalcraft'
--   AND `Fiscal Year` = 2026 AND `Fiscal Quarter` = 4
-- GROUP BY ALL
-- ORDER BY `Priority Effort Rate` DESC;
--
-- Team Summary:
-- SELECT `Manager L1`, MEASURE(`Total Effort Days`), MEASURE(`Priority Effort Rate`),
--        MEASURE(`Priority Accounts Engaged`), MEASURE(`Priority Account DBU QTD`)
-- FROM mv_focus_discipline
-- WHERE `Business Unit` = 'AMER Enterprise & Emerging'
-- GROUP BY ALL;
--
-- Account Tier Breakdown:
-- SELECT `Account Tier`, MEASURE(`Total ASQs`), MEASURE(`Total Effort Days`)
-- FROM mv_focus_discipline
-- WHERE `Manager L1` = 'Christopher Chalcraft'
-- GROUP BY ALL;
--
-- Trend Analysis:
-- SELECT `Created Year-Quarter`, MEASURE(`Priority Effort Rate`), MEASURE(`Meeting 80% Goal`)
-- FROM mv_focus_discipline
-- WHERE `Business Unit` = 'AMER Enterprise & Emerging'
-- GROUP BY ALL
-- ORDER BY `Created Year-Quarter`;
