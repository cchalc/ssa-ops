-- ============================================================================
-- mv_asset_reuse - Pattern Application & Asset Reuse Tracking
-- ============================================================================
-- Charter Metric #5: Asset Reuse Rate
-- Tracks SSA pattern application across multiple accounts
-- Measures reuse of technical approaches within specializations
-- Future: Integration with FE-IP project registry
-- Sources: GTM Silver (approval_request_detail, individual_hierarchy)
-- NO HARDCODED FILTERS - all BU/manager filtering at query time
-- ============================================================================

CREATE OR REPLACE METRIC VIEW home_christopher_chalcraft.cjc_views.mv_asset_reuse
COMMENT 'Asset reuse and pattern application metrics. Charter Metric #5. Tracks SSA pattern application across accounts within specializations.'
AS
WITH pattern_analysis AS (
  -- Identify patterns: SSA + Specialization + Support Type combinations
  SELECT
    asq.owner_user_name,
    asq.owner_user_id,
    asq.business_unit,
    asq.region_level_1,
    asq.technical_specialization,
    asq.support_type,
    asq.account_id,
    asq.account_name,
    asq.approval_request_id,
    asq.status,
    asq.created_date,
    -- Count how many accounts this SSA has applied this pattern to
    COUNT(DISTINCT asq.account_id) OVER (
      PARTITION BY asq.owner_user_id, asq.technical_specialization
    ) AS accounts_with_pattern,
    -- Count total ASQs for this pattern
    COUNT(*) OVER (
      PARTITION BY asq.owner_user_id, asq.technical_specialization
    ) AS asqs_with_pattern
  FROM main.gtm_silver.approval_request_detail asq
  WHERE asq.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
    AND asq.status IN ('Complete', 'Completed', 'Closed', 'In Progress')
    AND asq.technical_specialization IS NOT NULL
)
SELECT
  -- ========================================================================
  -- ORGANIZATIONAL DIMENSIONS
  -- ========================================================================

  pa.business_unit
    COMMENT 'BU: AMER Enterprise & Emerging, AMER Industries, EMEA, APJ' AS `Business Unit`,
  pa.region_level_1
    COMMENT 'Region: CAN, RCT, FINS, etc.' AS `Region`,
  pa.owner_user_name
    COMMENT 'SSA owner name' AS `Owner`,

  -- Manager Hierarchy
  hier.line_manager_name
    COMMENT 'Direct manager (L1)' AS `Manager L1`,
  hier.`2nd_line_manager_name`
    COMMENT 'Second-level manager (L2)' AS `Manager L2`,

  -- ========================================================================
  -- PATTERN CLASSIFICATION DIMENSIONS
  -- ========================================================================

  pa.technical_specialization
    COMMENT 'Technical specialization (pattern category)' AS `Specialization`,
  pa.support_type
    COMMENT 'Support type' AS `Support Type`,

  -- Pattern Reuse Tier
  CASE
    WHEN pa.accounts_with_pattern >= 5 THEN 'High Reuse (5+ accounts)'
    WHEN pa.accounts_with_pattern >= 3 THEN 'Moderate Reuse (3-4 accounts)'
    WHEN pa.accounts_with_pattern = 2 THEN 'Initial Reuse (2 accounts)'
    ELSE 'Single Use'
  END
    COMMENT 'Pattern reuse tier' AS `Reuse Tier`,

  -- ========================================================================
  -- CUSTOMER DIMENSIONS
  -- ========================================================================

  pa.account_name
    COMMENT 'Customer account name' AS `Account`,

  -- ========================================================================
  -- TIME DIMENSIONS
  -- ========================================================================

  YEAR(pa.created_date)
    COMMENT 'Calendar year' AS `Created Year`,
  CONCAT(YEAR(pa.created_date), '-Q', QUARTER(pa.created_date))
    COMMENT 'Calendar year-quarter' AS `Created Year-Quarter`,

  -- Fiscal
  CASE WHEN MONTH(pa.created_date) = 1 THEN YEAR(pa.created_date)
       ELSE YEAR(pa.created_date) + 1 END
    COMMENT 'Fiscal year (ends Jan 31)' AS `Fiscal Year`,

  -- ========================================================================
  -- PATTERN VOLUME MEASURES
  -- ========================================================================

  COUNT(DISTINCT pa.approval_request_id)
    COMMENT 'Total ASQs' AS `Total ASQs`,

  COUNT(DISTINCT pa.account_id)
    COMMENT 'Unique accounts' AS `Unique Accounts`,

  COUNT(DISTINCT pa.technical_specialization)
    COMMENT 'Unique specializations applied' AS `Unique Specializations`,

  -- ========================================================================
  -- PATTERN APPLICATION MEASURES
  -- ========================================================================

  -- Patterns applied to multiple accounts
  COUNT(DISTINCT CONCAT(pa.owner_user_id, '|', pa.technical_specialization))
    COMMENT 'Total pattern applications (SSA + Specialization combinations)' AS `Total Patterns`,

  COUNT(DISTINCT CONCAT(pa.owner_user_id, '|', pa.technical_specialization))
    FILTER (WHERE pa.accounts_with_pattern >= 2)
    COMMENT 'Patterns applied to 2+ accounts (reused patterns)' AS `Reused Patterns`,

  COUNT(DISTINCT CONCAT(pa.owner_user_id, '|', pa.technical_specialization))
    FILTER (WHERE pa.accounts_with_pattern >= 3)
    COMMENT 'Patterns applied to 3+ accounts (high reuse)' AS `High Reuse Patterns`,

  -- ========================================================================
  -- REUSE RATE MEASURES
  -- ========================================================================

  COUNT(DISTINCT CONCAT(pa.owner_user_id, '|', pa.technical_specialization))
    FILTER (WHERE pa.accounts_with_pattern >= 2) * 1.0
    / NULLIF(COUNT(DISTINCT CONCAT(pa.owner_user_id, '|', pa.technical_specialization)), 0)
    COMMENT 'Percentage of patterns reused (2+ accounts)' AS `Pattern Reuse Rate`,

  COUNT(DISTINCT pa.approval_request_id) FILTER (WHERE pa.accounts_with_pattern >= 2) * 1.0
    / NULLIF(COUNT(DISTINCT pa.approval_request_id), 0)
    COMMENT 'Percentage of ASQs that are pattern applications' AS `ASQ Reuse Rate`,

  -- ========================================================================
  -- CROSS-ACCOUNT REACH MEASURES
  -- ========================================================================

  AVG(pa.accounts_with_pattern)
    COMMENT 'Average accounts per pattern' AS `Avg Accounts per Pattern`,

  MAX(pa.accounts_with_pattern)
    COMMENT 'Maximum accounts for a single pattern (most reused)' AS `Max Pattern Reach`,

  -- Average specializations per SSA (breadth)
  COUNT(DISTINCT pa.technical_specialization) * 1.0
    / NULLIF(COUNT(DISTINCT pa.owner_user_id), 0)
    COMMENT 'Average specializations per SSA' AS `Avg Specializations per SSA`,

  -- ========================================================================
  -- PATTERN EFFICIENCY MEASURES
  -- ========================================================================

  -- ASQs per pattern (leverage)
  COUNT(DISTINCT pa.approval_request_id) * 1.0
    / NULLIF(COUNT(DISTINCT CONCAT(pa.owner_user_id, '|', pa.technical_specialization)), 0)
    COMMENT 'ASQs per pattern (leverage factor)' AS `ASQs per Pattern`,

  -- Accounts per SSA
  COUNT(DISTINCT pa.account_id) * 1.0
    / NULLIF(COUNT(DISTINCT pa.owner_user_id), 0)
    COMMENT 'Accounts per SSA' AS `Accounts per SSA`,

  -- ========================================================================
  -- SPECIALIZATION DISTRIBUTION
  -- ========================================================================

  COUNT(DISTINCT pa.approval_request_id) FILTER (WHERE
    pa.technical_specialization LIKE '%AI%' OR pa.technical_specialization LIKE '%ML%')
    COMMENT 'AI/ML ASQs' AS `AI ML ASQs`,

  COUNT(DISTINCT pa.approval_request_id) FILTER (WHERE
    pa.technical_specialization LIKE '%Data Eng%' OR pa.technical_specialization LIKE '%ETL%')
    COMMENT 'Data Engineering ASQs' AS `Data Engineering ASQs`,

  COUNT(DISTINCT pa.approval_request_id) FILTER (WHERE
    pa.technical_specialization LIKE '%Platform%' OR pa.technical_specialization LIKE '%Admin%')
    COMMENT 'Platform ASQs' AS `Platform ASQs`,

  COUNT(DISTINCT pa.approval_request_id) FILTER (WHERE
    pa.technical_specialization LIKE '%SQL%' OR pa.technical_specialization LIKE '%Analytics%')
    COMMENT 'SQL/Analytics ASQs' AS `SQL Analytics ASQs`

FROM pattern_analysis pa
LEFT JOIN main.gtm_silver.individual_hierarchy_salesforce hier
  ON pa.owner_user_id = hier.user_id;

-- ============================================================================
-- Sample Queries
-- ============================================================================
--
-- Pattern Reuse by SSA:
-- SELECT `Owner`, MEASURE(`Total Patterns`), MEASURE(`Reused Patterns`),
--        MEASURE(`Pattern Reuse Rate`), MEASURE(`Max Pattern Reach`)
-- FROM mv_asset_reuse
-- WHERE `Manager L1` = 'Christopher Chalcraft'
-- GROUP BY ALL
-- ORDER BY `Pattern Reuse Rate` DESC;
--
-- Specialization Reuse Analysis:
-- SELECT `Specialization`, MEASURE(`Total ASQs`), MEASURE(`Unique Accounts`),
--        MEASURE(`Reused Patterns`), MEASURE(`ASQs per Pattern`)
-- FROM mv_asset_reuse
-- WHERE `Business Unit` = 'AMER Enterprise & Emerging'
-- GROUP BY ALL
-- ORDER BY `Reused Patterns` DESC;
--
-- Team Reuse Summary:
-- SELECT `Manager L1`, MEASURE(`Total ASQs`), MEASURE(`Total Patterns`),
--        MEASURE(`Pattern Reuse Rate`), MEASURE(`Avg Accounts per Pattern`)
-- FROM mv_asset_reuse
-- WHERE `Fiscal Year` = 2026
-- GROUP BY ALL;
--
-- Reuse Tier Distribution:
-- SELECT `Reuse Tier`, MEASURE(`Total ASQs`), MEASURE(`Unique Accounts`)
-- FROM mv_asset_reuse
-- GROUP BY ALL
-- ORDER BY `Total ASQs` DESC;
