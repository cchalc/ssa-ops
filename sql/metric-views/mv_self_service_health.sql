-- ============================================================================
-- mv_self_service_health - ASQ Deflection Proxy (Self-Service Enablement)
-- ============================================================================
-- Charter Metric #6: ASQ Deflection Rate (PROXY)
-- Since "potential ASQs" cannot be measured, this tracks self-service health:
-- - Accounts with longer gaps between ASQs = more self-sufficient
-- - One-time accounts = potentially enabled after single engagement
-- - Decreasing ASQ frequency over time = successful enablement
-- Sources: GTM Silver (approval_request_detail, individual_hierarchy)
-- NO HARDCODED FILTERS - all BU/manager filtering at query time
-- ============================================================================

CREATE OR REPLACE METRIC VIEW home_christopher_chalcraft.cjc_views.mv_self_service_health
COMMENT 'Self-service health proxy for ASQ deflection. Charter Metric #6 (proxy). Tracks account engagement patterns indicating self-sufficiency.'
AS
WITH account_engagement AS (
  -- Calculate engagement patterns per account
  SELECT
    asq.account_id,
    asq.account_name,
    asq.business_unit,
    asq.region_level_1,
    MIN(asq.created_date) AS first_asq_date,
    MAX(asq.created_date) AS last_asq_date,
    COUNT(*) AS total_asqs,
    COUNT(DISTINCT YEAR(asq.created_date)) AS years_engaged,
    COUNT(DISTINCT CONCAT(YEAR(asq.created_date), '-Q', QUARTER(asq.created_date))) AS quarters_engaged,
    DATEDIFF(MAX(asq.created_date), MIN(asq.created_date)) AS engagement_span_days,
    -- Latest SSA to work on account
    FIRST_VALUE(asq.owner_user_name) OVER (
      PARTITION BY asq.account_id ORDER BY asq.created_date DESC
    ) AS latest_ssa,
    FIRST_VALUE(asq.owner_user_id) OVER (
      PARTITION BY asq.account_id ORDER BY asq.created_date DESC
    ) AS latest_ssa_id
  FROM main.gtm_silver.approval_request_detail asq
  WHERE asq.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
    AND asq.status NOT IN ('Rejected', 'Cancelled')
  GROUP BY asq.account_id, asq.account_name, asq.business_unit, asq.region_level_1,
           asq.owner_user_name, asq.owner_user_id, asq.created_date
),
account_metrics AS (
  SELECT
    ae.*,
    -- Calculate average days between ASQs
    CASE
      WHEN ae.total_asqs > 1 THEN ae.engagement_span_days * 1.0 / (ae.total_asqs - 1)
      ELSE NULL
    END AS avg_days_between_asqs,
    -- Self-service tier
    CASE
      WHEN ae.total_asqs = 1 THEN 'One-Time (Enabled)'
      WHEN ae.total_asqs = 2 AND ae.engagement_span_days > 180 THEN 'Self-Sufficient'
      WHEN ae.engagement_span_days / NULLIF(ae.total_asqs - 1, 0) > 180 THEN 'Highly Self-Sufficient'
      WHEN ae.engagement_span_days / NULLIF(ae.total_asqs - 1, 0) > 90 THEN 'Self-Sufficient'
      WHEN ae.engagement_span_days / NULLIF(ae.total_asqs - 1, 0) > 30 THEN 'Regular Engagement'
      WHEN ae.engagement_span_days / NULLIF(ae.total_asqs - 1, 0) <= 30 THEN 'Frequent Dependency'
      ELSE 'Unknown'
    END AS self_service_tier
  FROM (
    SELECT
      account_id,
      account_name,
      business_unit,
      region_level_1,
      MIN(first_asq_date) AS first_asq_date,
      MAX(last_asq_date) AS last_asq_date,
      SUM(total_asqs) AS total_asqs,
      MAX(years_engaged) AS years_engaged,
      MAX(quarters_engaged) AS quarters_engaged,
      MAX(engagement_span_days) AS engagement_span_days,
      MAX(latest_ssa) AS latest_ssa,
      MAX(latest_ssa_id) AS latest_ssa_id
    FROM account_engagement
    GROUP BY account_id, account_name, business_unit, region_level_1
  ) ae
)
SELECT
  -- ========================================================================
  -- ORGANIZATIONAL DIMENSIONS
  -- ========================================================================

  am.business_unit
    COMMENT 'BU: AMER Enterprise & Emerging, AMER Industries, EMEA, APJ' AS `Business Unit`,
  am.region_level_1
    COMMENT 'Region: CAN, RCT, FINS, etc.' AS `Region`,
  am.latest_ssa
    COMMENT 'Most recent SSA on account' AS `Latest SSA`,

  -- Manager Hierarchy (for latest SSA)
  hier.line_manager_name
    COMMENT 'Direct manager (L1)' AS `Manager L1`,
  hier.`2nd_line_manager_name`
    COMMENT 'Second-level manager (L2)' AS `Manager L2`,

  -- ========================================================================
  -- SELF-SERVICE CLASSIFICATION DIMENSIONS
  -- ========================================================================

  am.self_service_tier
    COMMENT 'Self-service tier based on engagement frequency' AS `Self-Service Tier`,

  -- Engagement Frequency Band
  CASE
    WHEN am.avg_days_between_asqs IS NULL THEN 'Single ASQ'
    WHEN am.avg_days_between_asqs <= 30 THEN 'Very Frequent (<30 days)'
    WHEN am.avg_days_between_asqs <= 60 THEN 'Frequent (30-60 days)'
    WHEN am.avg_days_between_asqs <= 90 THEN 'Regular (60-90 days)'
    WHEN am.avg_days_between_asqs <= 180 THEN 'Occasional (90-180 days)'
    ELSE 'Rare (>180 days)'
  END
    COMMENT 'ASQ frequency band' AS `Engagement Frequency`,

  -- ========================================================================
  -- CUSTOMER DIMENSIONS
  -- ========================================================================

  am.account_name
    COMMENT 'Customer account name' AS `Account`,

  -- ========================================================================
  -- TIME DIMENSIONS
  -- ========================================================================

  YEAR(am.first_asq_date)
    COMMENT 'Year of first ASQ engagement' AS `First Engaged Year`,
  YEAR(am.last_asq_date)
    COMMENT 'Year of most recent ASQ' AS `Last Engaged Year`,

  -- ========================================================================
  -- ACCOUNT VOLUME MEASURES
  -- ========================================================================

  COUNT(DISTINCT am.account_id)
    COMMENT 'Total accounts' AS `Total Accounts`,

  -- Self-Service Tier Distribution
  COUNT(DISTINCT am.account_id) FILTER (WHERE am.self_service_tier = 'One-Time (Enabled)')
    COMMENT 'Accounts with single ASQ (enabled after one engagement)' AS `One-Time Accounts`,

  COUNT(DISTINCT am.account_id) FILTER (WHERE am.self_service_tier IN ('Self-Sufficient', 'Highly Self-Sufficient'))
    COMMENT 'Self-sufficient accounts (>90 days between ASQs)' AS `Self-Sufficient Accounts`,

  COUNT(DISTINCT am.account_id) FILTER (WHERE am.self_service_tier = 'Regular Engagement')
    COMMENT 'Regular engagement accounts (30-90 days)' AS `Regular Engagement Accounts`,

  COUNT(DISTINCT am.account_id) FILTER (WHERE am.self_service_tier = 'Frequent Dependency')
    COMMENT 'Frequent dependency accounts (<30 days)' AS `Frequent Dependency Accounts`,

  -- ========================================================================
  -- SELF-SERVICE RATE MEASURES
  -- ========================================================================

  -- Deflection Proxy: One-Time + Self-Sufficient / Total
  (COUNT(DISTINCT am.account_id) FILTER (WHERE am.self_service_tier = 'One-Time (Enabled)')
   + COUNT(DISTINCT am.account_id) FILTER (WHERE am.self_service_tier IN ('Self-Sufficient', 'Highly Self-Sufficient'))) * 1.0
    / NULLIF(COUNT(DISTINCT am.account_id), 0)
    COMMENT 'Self-service rate (one-time + self-sufficient / total)' AS `Self-Service Rate`,

  -- One-Time Rate
  COUNT(DISTINCT am.account_id) FILTER (WHERE am.self_service_tier = 'One-Time (Enabled)') * 1.0
    / NULLIF(COUNT(DISTINCT am.account_id), 0)
    COMMENT 'One-time enablement rate' AS `One-Time Rate`,

  -- Dependency Rate (inverse of deflection)
  COUNT(DISTINCT am.account_id) FILTER (WHERE am.self_service_tier = 'Frequent Dependency') * 1.0
    / NULLIF(COUNT(DISTINCT am.account_id), 0)
    COMMENT 'Frequent dependency rate (inverse of self-service)' AS `Dependency Rate`,

  -- ========================================================================
  -- ENGAGEMENT GAP MEASURES
  -- ========================================================================

  AVG(am.avg_days_between_asqs)
    COMMENT 'Average days between ASQs across accounts' AS `Avg Days Between ASQs`,

  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY am.avg_days_between_asqs)
    COMMENT 'Median days between ASQs' AS `Median Days Between ASQs`,

  MAX(am.avg_days_between_asqs)
    COMMENT 'Maximum average gap (most self-sufficient)' AS `Max Avg Gap`,

  MIN(am.avg_days_between_asqs) FILTER (WHERE am.avg_days_between_asqs > 0)
    COMMENT 'Minimum average gap (most dependent)' AS `Min Avg Gap`,

  -- ========================================================================
  -- ASQ VOLUME BY TIER
  -- ========================================================================

  SUM(am.total_asqs)
    COMMENT 'Total ASQs across all accounts' AS `Total ASQs`,

  SUM(am.total_asqs) FILTER (WHERE am.self_service_tier = 'One-Time (Enabled)')
    COMMENT 'ASQs from one-time accounts (single touch)' AS `One-Time ASQs`,

  SUM(am.total_asqs) FILTER (WHERE am.self_service_tier = 'Frequent Dependency')
    COMMENT 'ASQs from frequent dependency accounts' AS `Dependency ASQs`,

  -- ========================================================================
  -- ENABLEMENT EFFICIENCY
  -- ========================================================================

  -- ASQs per account (lower = better enablement)
  SUM(am.total_asqs) * 1.0 / NULLIF(COUNT(DISTINCT am.account_id), 0)
    COMMENT 'ASQs per account (lower = better enablement)' AS `ASQs per Account`,

  -- Engagement span efficiency
  AVG(am.engagement_span_days)
    COMMENT 'Average engagement span in days' AS `Avg Engagement Span Days`,

  AVG(am.years_engaged)
    COMMENT 'Average years of engagement per account' AS `Avg Years Engaged`

FROM account_metrics am
LEFT JOIN main.gtm_silver.individual_hierarchy_salesforce hier
  ON am.latest_ssa_id = hier.user_id;

-- ============================================================================
-- Sample Queries
-- ============================================================================
--
-- Self-Service Health by Team:
-- SELECT `Manager L1`, MEASURE(`Total Accounts`), MEASURE(`Self-Sufficient Accounts`),
--        MEASURE(`Self-Service Rate`), MEASURE(`Dependency Rate`)
-- FROM mv_self_service_health
-- WHERE `Business Unit` = 'AMER Enterprise & Emerging'
-- GROUP BY ALL;
--
-- Self-Service Tier Distribution:
-- SELECT `Self-Service Tier`, MEASURE(`Total Accounts`), MEASURE(`Total ASQs`),
--        MEASURE(`ASQs per Account`)
-- FROM mv_self_service_health
-- GROUP BY ALL
-- ORDER BY `Total Accounts` DESC;
--
-- Engagement Frequency Analysis:
-- SELECT `Engagement Frequency`, MEASURE(`Total Accounts`),
--        MEASURE(`Avg Days Between ASQs`)
-- FROM mv_self_service_health
-- GROUP BY ALL
-- ORDER BY `Avg Days Between ASQs`;
--
-- Enablement Efficiency by Region:
-- SELECT `Region`, MEASURE(`Total Accounts`), MEASURE(`One-Time Accounts`),
--        MEASURE(`One-Time Rate`), MEASURE(`ASQs per Account`)
-- FROM mv_self_service_health
-- GROUP BY ALL
-- ORDER BY `One-Time Rate` DESC;
