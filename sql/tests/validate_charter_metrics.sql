-- ============================================================================
-- validate_charter_metrics.sql - Validation Tests for Charter Metrics 4-8
-- ============================================================================
-- Run these queries after deploying the metric views to validate data quality
-- ============================================================================

-- ============================================================================
-- TEST 1: mv_time_to_adopt - Charter Metric #4
-- ============================================================================

-- 1.1 Check that we have UCOs with transition data
SELECT 'mv_time_to_adopt: UCO count check' AS test_name,
  CASE WHEN MEASURE(`Total UCOs`) > 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  MEASURE(`Total UCOs`) AS value
FROM ${catalog}.${schema}.mv_time_to_adopt;

-- 1.2 Check that adopted UCOs exist
SELECT 'mv_time_to_adopt: Adopted UCOs exist' AS test_name,
  CASE WHEN MEASURE(`Adopted UCOs`) > 0 THEN 'PASS' ELSE 'WARN' END AS status,
  MEASURE(`Adopted UCOs`) AS value
FROM ${catalog}.${schema}.mv_time_to_adopt;

-- 1.3 Check adoption time is reasonable (1-365 days)
SELECT 'mv_time_to_adopt: Avg days reasonable' AS test_name,
  CASE
    WHEN MEASURE(`Avg Days to Adopt`) BETWEEN 1 AND 365 THEN 'PASS'
    WHEN MEASURE(`Avg Days to Adopt`) IS NULL THEN 'WARN - No data'
    ELSE 'FAIL'
  END AS status,
  MEASURE(`Avg Days to Adopt`) AS value
FROM ${catalog}.${schema}.mv_time_to_adopt;

-- 1.4 Sample by BU
SELECT
  `Business Unit`,
  MEASURE(`Total UCOs`) AS total_ucos,
  MEASURE(`Adopted UCOs`) AS adopted,
  MEASURE(`Avg Days to Adopt`) AS avg_days,
  MEASURE(`Fast Adoption Rate`) AS fast_rate
FROM ${catalog}.${schema}.mv_time_to_adopt
GROUP BY ALL
ORDER BY total_ucos DESC
LIMIT 10;

-- ============================================================================
-- TEST 2: mv_asset_reuse - Charter Metric #5
-- ============================================================================

-- 2.1 Check pattern data exists
SELECT 'mv_asset_reuse: Pattern count check' AS test_name,
  CASE WHEN MEASURE(`Total Patterns`) > 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  MEASURE(`Total Patterns`) AS value
FROM ${catalog}.${schema}.mv_asset_reuse;

-- 2.2 Check reuse rate is between 0-1
SELECT 'mv_asset_reuse: Reuse rate valid' AS test_name,
  CASE
    WHEN MEASURE(`Pattern Reuse Rate`) BETWEEN 0 AND 1 THEN 'PASS'
    WHEN MEASURE(`Pattern Reuse Rate`) IS NULL THEN 'WARN - No data'
    ELSE 'FAIL'
  END AS status,
  MEASURE(`Pattern Reuse Rate`) AS value
FROM ${catalog}.${schema}.mv_asset_reuse;

-- 2.3 Sample by specialization
SELECT
  `Specialization`,
  MEASURE(`Total ASQs`) AS total_asqs,
  MEASURE(`Unique Accounts`) AS accounts,
  MEASURE(`Reused Patterns`) AS reused,
  MEASURE(`Pattern Reuse Rate`) AS reuse_rate
FROM ${catalog}.${schema}.mv_asset_reuse
GROUP BY ALL
ORDER BY total_asqs DESC
LIMIT 10;

-- ============================================================================
-- TEST 3: mv_self_service_health - Charter Metric #6 (Proxy)
-- ============================================================================

-- 3.1 Check account data exists
SELECT 'mv_self_service_health: Account count check' AS test_name,
  CASE WHEN MEASURE(`Total Accounts`) > 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  MEASURE(`Total Accounts`) AS value
FROM ${catalog}.${schema}.mv_self_service_health;

-- 3.2 Check self-service tier distribution adds up
SELECT 'mv_self_service_health: Tier distribution check' AS test_name,
  CASE
    WHEN (MEASURE(`One-Time Accounts`) + MEASURE(`Self-Sufficient Accounts`) +
          MEASURE(`Regular Engagement Accounts`) + MEASURE(`Frequent Dependency Accounts`)) > 0
    THEN 'PASS'
    ELSE 'WARN'
  END AS status,
  MEASURE(`Total Accounts`) AS total
FROM ${catalog}.${schema}.mv_self_service_health;

-- 3.3 Sample by self-service tier
SELECT
  `Self-Service Tier`,
  MEASURE(`Total Accounts`) AS accounts,
  MEASURE(`Total ASQs`) AS asqs,
  MEASURE(`ASQs per Account`) AS asqs_per_account
FROM ${catalog}.${schema}.mv_self_service_health
GROUP BY ALL
ORDER BY accounts DESC;

-- ============================================================================
-- TEST 4: mv_product_impact - Charter Metric #7
-- ============================================================================

-- 4.1 Check engagement data exists
SELECT 'mv_product_impact: Engaged accounts check' AS test_name,
  CASE WHEN MEASURE(`Engaged Accounts`) > 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  MEASURE(`Engaged Accounts`) AS value
FROM ${catalog}.${schema}.mv_product_impact;

-- 4.2 Check product adoption rates are valid (0-1)
SELECT 'mv_product_impact: Adoption rates valid' AS test_name,
  CASE
    WHEN MEASURE(`Lakeflow Adoption Rate`) BETWEEN 0 AND 1
     AND MEASURE(`Serverless Adoption Rate`) BETWEEN 0 AND 1
    THEN 'PASS'
    ELSE 'WARN'
  END AS status,
  MEASURE(`Lakeflow Adoption Rate`) AS lakeflow,
  MEASURE(`Serverless Adoption Rate`) AS serverless
FROM ${catalog}.${schema}.mv_product_impact;

-- 4.3 Sample by product group
SELECT
  `Product Group`,
  MEASURE(`Total UCOs`) AS total_ucos,
  MEASURE(`Production UCOs`) AS production,
  MEASURE(`Engaged Accounts`) AS accounts
FROM ${catalog}.${schema}.mv_product_impact
GROUP BY ALL
ORDER BY total_ucos DESC;

-- 4.4 Product adoption by team
SELECT
  `Manager L1`,
  MEASURE(`Engaged Accounts`) AS accounts,
  MEASURE(`Lakeflow Influenced Accounts`) AS lakeflow,
  MEASURE(`Serverless Influenced Accounts`) AS serverless,
  MEASURE(`Model Serving Influenced Accounts`) AS model_serving
FROM ${catalog}.${schema}.mv_product_impact
WHERE `Business Unit` = 'AMER Enterprise & Emerging'
GROUP BY ALL
ORDER BY accounts DESC
LIMIT 10;

-- ============================================================================
-- TEST 5: mv_customer_risk_reduction - Charter Metric #8
-- ============================================================================

-- 5.1 Check ASQ data exists
SELECT 'mv_customer_risk_reduction: ASQ count check' AS test_name,
  CASE WHEN MEASURE(`Total ASQs`) > 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  MEASURE(`Total ASQs`) AS value
FROM ${catalog}.${schema}.mv_customer_risk_reduction;

-- 5.2 Check competitive data exists
SELECT 'mv_customer_risk_reduction: Competitive UCOs exist' AS test_name,
  CASE WHEN MEASURE(`Competitive UCOs`) > 0 THEN 'PASS' ELSE 'WARN' END AS status,
  MEASURE(`Competitive UCOs`) AS value
FROM ${catalog}.${schema}.mv_customer_risk_reduction;

-- 5.3 Check win rate is valid (0-1)
SELECT 'mv_customer_risk_reduction: Win rate valid' AS test_name,
  CASE
    WHEN MEASURE(`Competitive Win Rate`) BETWEEN 0 AND 1 THEN 'PASS'
    WHEN MEASURE(`Competitive Win Rate`) IS NULL THEN 'WARN - No competitive data'
    ELSE 'FAIL'
  END AS status,
  MEASURE(`Competitive Win Rate`) AS value
FROM ${catalog}.${schema}.mv_customer_risk_reduction;

-- 5.4 Sample by competitor category
SELECT
  `Competitor Category`,
  MEASURE(`Competitive UCOs`) AS ucos,
  MEASURE(`Competitive Wins`) AS wins,
  MEASURE(`Competitive Losses`) AS losses,
  MEASURE(`Competitive Win Rate`) AS win_rate
FROM ${catalog}.${schema}.mv_customer_risk_reduction
WHERE `Competitor Category` != 'No Competitor'
GROUP BY ALL
ORDER BY ucos DESC;

-- 5.5 Risk context distribution
SELECT
  `Risk Context`,
  MEASURE(`Total ASQs`) AS asqs,
  MEASURE(`Risk ASQs Resolved`) AS resolved,
  MEASURE(`Risk Resolution Rate`) AS resolution_rate
FROM ${catalog}.${schema}.mv_customer_risk_reduction
WHERE `Risk Context` != 'Standard'
GROUP BY ALL
ORDER BY asqs DESC;

-- ============================================================================
-- SUMMARY: Charter Metrics Coverage
-- ============================================================================

SELECT 'CHARTER METRICS SUMMARY' AS section;

SELECT
  'Metric #4: Time-to-Adopt' AS metric,
  'mv_time_to_adopt' AS metric_view,
  CASE WHEN MEASURE(`Total UCOs`) > 0 THEN 'DATA AVAILABLE' ELSE 'NO DATA' END AS status
FROM ${catalog}.${schema}.mv_time_to_adopt
UNION ALL
SELECT
  'Metric #5: Asset Reuse' AS metric,
  'mv_asset_reuse' AS metric_view,
  CASE WHEN MEASURE(`Total Patterns`) > 0 THEN 'DATA AVAILABLE' ELSE 'NO DATA' END AS status
FROM ${catalog}.${schema}.mv_asset_reuse
UNION ALL
SELECT
  'Metric #6: Self-Service (Proxy)' AS metric,
  'mv_self_service_health' AS metric_view,
  CASE WHEN MEASURE(`Total Accounts`) > 0 THEN 'DATA AVAILABLE' ELSE 'NO DATA' END AS status
FROM ${catalog}.${schema}.mv_self_service_health
UNION ALL
SELECT
  'Metric #7: Product Impact' AS metric,
  'mv_product_impact' AS metric_view,
  CASE WHEN MEASURE(`Engaged Accounts`) > 0 THEN 'DATA AVAILABLE' ELSE 'NO DATA' END AS status
FROM ${catalog}.${schema}.mv_product_impact
UNION ALL
SELECT
  'Metric #8: Customer Risk' AS metric,
  'mv_customer_risk_reduction' AS metric_view,
  CASE WHEN MEASURE(`Competitive UCOs`) > 0 THEN 'DATA AVAILABLE' ELSE 'NO DATA' END AS status
FROM ${catalog}.${schema}.mv_customer_risk_reduction;
