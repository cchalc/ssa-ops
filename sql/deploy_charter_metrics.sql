-- ============================================================================
-- DEPLOY CHARTER METRICS 4-8
-- ============================================================================
-- Run this script in Databricks SQL Editor on logfood workspace:
-- https://adb-2548836972759138.18.azuredatabricks.net/sql/editor
-- Warehouse: Shared SQL Endpoint - Stable (927ac096f9833442)
-- ============================================================================

-- ============================================================================
-- METRIC #4: TIME-TO-ADOPT (U3→U4 Transition Time)
-- ============================================================================
CREATE OR REPLACE METRIC VIEW home_christopher_chalcraft.cjc_views.mv_time_to_adopt
COMMENT 'UCO stage acceleration metrics. Charter Metric #4: Time-to-Adopt. Measures U3 to U4 transition time for SSA-engaged use cases.'
AS
SELECT
  asq.business_unit COMMENT 'BU: AMER Enterprise and Emerging, AMER Industries, EMEA, APJ' AS `Business Unit`,
  asq.region_level_1 COMMENT 'Region: CAN, RCT, FINS, etc.' AS `Region`,
  asq.owner_user_name COMMENT 'SSA owner name' AS `Owner`,
  hier.line_manager_name COMMENT 'Direct manager (L1)' AS `Manager L1`,
  hier.`2nd_line_manager_name` COMMENT 'Second-level manager (L2)' AS `Manager L2`,
  asq.account_name COMMENT 'Customer account name' AS `Account`,
  uco.stage COMMENT 'Current UCO stage' AS `Current Stage`,
  uco.use_case_product COMMENT 'Product category' AS `Product Category`,
  CASE
    WHEN uco.u4_date_sfdc_original IS NULL THEN 'Not Yet Adopted'
    WHEN uco.u3_date_sfdc_original IS NULL THEN 'No U3 Date'
    WHEN DATEDIFF(uco.u4_date_sfdc_original, uco.u3_date_sfdc_original) <= 14 THEN 'Fast (14 days or less)'
    WHEN DATEDIFF(uco.u4_date_sfdc_original, uco.u3_date_sfdc_original) <= 30 THEN 'Normal (15-30 days)'
    WHEN DATEDIFF(uco.u4_date_sfdc_original, uco.u3_date_sfdc_original) <= 60 THEN 'Slow (31-60 days)'
    ELSE 'Very Slow (over 60 days)'
  END COMMENT 'Adoption speed tier based on U3 to U4 days' AS `Adoption Speed`,
  YEAR(asq.created_date) COMMENT 'Calendar year' AS `Created Year`,
  CASE WHEN MONTH(asq.created_date) = 1 THEN YEAR(asq.created_date) ELSE YEAR(asq.created_date) + 1 END
    COMMENT 'Fiscal year (ends Jan 31)' AS `Fiscal Year`,
  COUNT(DISTINCT uco.usecase_id) COMMENT 'Total UCOs linked to ASQs' AS `Total UCOs`,
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.u4_date_sfdc_original IS NOT NULL)
    COMMENT 'UCOs that reached U4 (tech win)' AS `Adopted UCOs`,
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.stage IN ('U5', 'U6'))
    COMMENT 'UCOs at production (U5+)' AS `Production UCOs`,
  AVG(DATEDIFF(uco.u4_date_sfdc_original, uco.u3_date_sfdc_original))
    COMMENT 'Average days from U3 to U4' AS `Avg Days to Adopt`,
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.u4_date_sfdc_original IS NOT NULL) * 1.0
    / NULLIF(COUNT(DISTINCT uco.usecase_id), 0)
    COMMENT 'Percentage of UCOs that reached tech win (U4)' AS `Adoption Rate`,
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE
    uco.u3_date_sfdc_original IS NOT NULL AND uco.u4_date_sfdc_original IS NOT NULL
    AND DATEDIFF(uco.u4_date_sfdc_original, uco.u3_date_sfdc_original) <= 14) * 1.0
    / NULLIF(COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.u4_date_sfdc_original IS NOT NULL), 0)
    COMMENT 'Percentage of adoptions that were fast' AS `Fast Adoption Rate`
FROM main.gtm_silver.approval_request_detail asq
LEFT JOIN main.gtm_silver.individual_hierarchy_salesforce hier ON asq.owner_user_id = hier.user_id
LEFT JOIN main.gtm_silver.use_case_detail uco
  ON asq.account_id = uco.account_id
  AND uco.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.use_case_detail)
  AND uco.is_active_ind = true
WHERE asq.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
  AND uco.usecase_id IS NOT NULL;

-- ============================================================================
-- METRIC #5: ASSET REUSE RATE (Pattern Application)
-- ============================================================================
CREATE OR REPLACE METRIC VIEW home_christopher_chalcraft.cjc_views.mv_asset_reuse
COMMENT 'Asset reuse and pattern application metrics. Charter Metric #5. Tracks SSA pattern application across accounts.'
AS
WITH pattern_analysis AS (
  SELECT
    asq.owner_user_name,
    asq.owner_user_id,
    asq.business_unit,
    asq.region_level_1,
    asq.technical_specialization,
    asq.account_id,
    asq.account_name,
    asq.approval_request_id,
    asq.created_date,
    COUNT(DISTINCT asq.account_id) OVER (PARTITION BY asq.owner_user_id, asq.technical_specialization) AS accounts_with_pattern
  FROM main.gtm_silver.approval_request_detail asq
  WHERE asq.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
    AND asq.status IN ('Complete', 'Completed', 'Closed', 'In Progress')
    AND asq.technical_specialization IS NOT NULL
)
SELECT
  pa.business_unit COMMENT 'BU' AS `Business Unit`,
  pa.region_level_1 COMMENT 'Region' AS `Region`,
  pa.owner_user_name COMMENT 'SSA owner name' AS `Owner`,
  hier.line_manager_name COMMENT 'Direct manager (L1)' AS `Manager L1`,
  pa.technical_specialization COMMENT 'Technical specialization' AS `Specialization`,
  CASE
    WHEN pa.accounts_with_pattern >= 5 THEN 'High Reuse (5+ accounts)'
    WHEN pa.accounts_with_pattern >= 3 THEN 'Moderate Reuse (3-4 accounts)'
    WHEN pa.accounts_with_pattern = 2 THEN 'Initial Reuse (2 accounts)'
    ELSE 'Single Use'
  END COMMENT 'Pattern reuse tier' AS `Reuse Tier`,
  pa.account_name COMMENT 'Customer account name' AS `Account`,
  YEAR(pa.created_date) COMMENT 'Calendar year' AS `Created Year`,
  CASE WHEN MONTH(pa.created_date) = 1 THEN YEAR(pa.created_date) ELSE YEAR(pa.created_date) + 1 END
    COMMENT 'Fiscal year' AS `Fiscal Year`,
  COUNT(DISTINCT pa.approval_request_id) COMMENT 'Total ASQs' AS `Total ASQs`,
  COUNT(DISTINCT pa.account_id) COMMENT 'Unique accounts' AS `Unique Accounts`,
  COUNT(DISTINCT CONCAT(pa.owner_user_id, '|', pa.technical_specialization))
    COMMENT 'Total patterns' AS `Total Patterns`,
  COUNT(DISTINCT CONCAT(pa.owner_user_id, '|', pa.technical_specialization)) FILTER (WHERE pa.accounts_with_pattern >= 2)
    COMMENT 'Reused patterns (2+ accounts)' AS `Reused Patterns`,
  COUNT(DISTINCT CONCAT(pa.owner_user_id, '|', pa.technical_specialization)) FILTER (WHERE pa.accounts_with_pattern >= 2) * 1.0
    / NULLIF(COUNT(DISTINCT CONCAT(pa.owner_user_id, '|', pa.technical_specialization)), 0)
    COMMENT 'Reuse rate' AS `Pattern Reuse Rate`,
  AVG(pa.accounts_with_pattern) COMMENT 'Avg accounts per pattern' AS `Avg Accounts per Pattern`,
  MAX(pa.accounts_with_pattern) COMMENT 'Max pattern reach' AS `Max Pattern Reach`
FROM pattern_analysis pa
LEFT JOIN main.gtm_silver.individual_hierarchy_salesforce hier ON pa.owner_user_id = hier.user_id;

-- ============================================================================
-- METRIC #6: SELF-SERVICE HEALTH (ASQ Deflection Proxy)
-- ============================================================================
CREATE OR REPLACE METRIC VIEW home_christopher_chalcraft.cjc_views.mv_self_service_health
COMMENT 'Self-service health proxy for ASQ deflection. Charter Metric #6 (proxy). Tracks account engagement patterns.'
AS
WITH account_metrics AS (
  SELECT
    asq.account_id,
    asq.account_name,
    asq.business_unit,
    asq.region_level_1,
    COUNT(*) AS total_asqs,
    DATEDIFF(MAX(asq.created_date), MIN(asq.created_date)) AS engagement_span_days,
    FIRST_VALUE(asq.owner_user_name) OVER (PARTITION BY asq.account_id ORDER BY asq.created_date DESC) AS latest_ssa,
    FIRST_VALUE(asq.owner_user_id) OVER (PARTITION BY asq.account_id ORDER BY asq.created_date DESC) AS latest_ssa_id
  FROM main.gtm_silver.approval_request_detail asq
  WHERE asq.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
    AND asq.status NOT IN ('Rejected', 'Cancelled')
  GROUP BY asq.account_id, asq.account_name, asq.business_unit, asq.region_level_1,
           asq.owner_user_name, asq.owner_user_id, asq.created_date
),
account_summary AS (
  SELECT
    account_id, account_name, business_unit, region_level_1,
    MAX(latest_ssa) AS latest_ssa, MAX(latest_ssa_id) AS latest_ssa_id,
    SUM(total_asqs) AS total_asqs, MAX(engagement_span_days) AS engagement_span_days,
    CASE
      WHEN SUM(total_asqs) = 1 THEN 'One-Time (Enabled)'
      WHEN MAX(engagement_span_days) / NULLIF(SUM(total_asqs) - 1, 0) > 90 THEN 'Self-Sufficient'
      WHEN MAX(engagement_span_days) / NULLIF(SUM(total_asqs) - 1, 0) > 30 THEN 'Regular Engagement'
      ELSE 'Frequent Dependency'
    END AS self_service_tier
  FROM account_metrics
  GROUP BY account_id, account_name, business_unit, region_level_1
)
SELECT
  am.business_unit COMMENT 'BU' AS `Business Unit`,
  am.region_level_1 COMMENT 'Region' AS `Region`,
  am.latest_ssa COMMENT 'Most recent SSA on account' AS `Latest SSA`,
  hier.line_manager_name COMMENT 'Direct manager (L1)' AS `Manager L1`,
  am.self_service_tier COMMENT 'Self-service tier' AS `Self-Service Tier`,
  am.account_name COMMENT 'Customer account name' AS `Account`,
  COUNT(DISTINCT am.account_id) COMMENT 'Total accounts' AS `Total Accounts`,
  COUNT(DISTINCT am.account_id) FILTER (WHERE am.self_service_tier = 'One-Time (Enabled)')
    COMMENT 'One-time accounts' AS `One-Time Accounts`,
  COUNT(DISTINCT am.account_id) FILTER (WHERE am.self_service_tier = 'Self-Sufficient')
    COMMENT 'Self-sufficient accounts' AS `Self-Sufficient Accounts`,
  COUNT(DISTINCT am.account_id) FILTER (WHERE am.self_service_tier = 'Frequent Dependency')
    COMMENT 'Frequent dependency accounts' AS `Frequent Dependency Accounts`,
  (COUNT(DISTINCT am.account_id) FILTER (WHERE am.self_service_tier IN ('One-Time (Enabled)', 'Self-Sufficient'))) * 1.0
    / NULLIF(COUNT(DISTINCT am.account_id), 0)
    COMMENT 'Self-service rate' AS `Self-Service Rate`,
  SUM(am.total_asqs) COMMENT 'Total ASQs' AS `Total ASQs`,
  SUM(am.total_asqs) * 1.0 / NULLIF(COUNT(DISTINCT am.account_id), 0)
    COMMENT 'ASQs per account' AS `ASQs per Account`
FROM account_summary am
LEFT JOIN main.gtm_silver.individual_hierarchy_salesforce hier ON am.latest_ssa_id = hier.user_id;

-- ============================================================================
-- METRIC #7: PRODUCT IMPACT (Compensation-Tied Product Adoption)
-- ============================================================================
CREATE OR REPLACE METRIC VIEW home_christopher_chalcraft.cjc_views.mv_product_impact
COMMENT 'Product adoption impact from SSA engagements. Charter Metric #7. Tracks Lakeflow, Serverless, Model Serving adoption.'
AS
SELECT
  asq.business_unit COMMENT 'BU' AS `Business Unit`,
  asq.region_level_1 COMMENT 'Region' AS `Region`,
  asq.owner_user_name COMMENT 'SSA owner name' AS `Owner`,
  hier.line_manager_name COMMENT 'Direct manager (L1)' AS `Manager L1`,
  asq.account_name COMMENT 'Customer account name' AS `Account`,
  uco.use_case_product COMMENT 'UCO product category' AS `UCO Product`,
  CASE
    WHEN uco.use_case_product LIKE '%Lakeflow%' OR uco.use_case_product LIKE '%DLT%' THEN 'Lakeflow'
    WHEN uco.use_case_product LIKE '%Serverless%' OR uco.use_case_product LIKE '%SQL Warehouse%' THEN 'Serverless SQL'
    WHEN uco.use_case_product LIKE '%Model Serving%' OR uco.use_case_product LIKE '%ML%' THEN 'AI/ML'
    ELSE 'Other'
  END COMMENT 'Product grouping' AS `Product Group`,
  CASE WHEN COALESCE(ao.dlt_dbu_dollars_qtd, 0) > 0 THEN 'Yes' ELSE 'No' END
    COMMENT 'Has Lakeflow consumption' AS `Has Lakeflow`,
  CASE WHEN COALESCE(ao.dbsql_serverless_dbu_dollars_qtd, 0) > 0 THEN 'Yes' ELSE 'No' END
    COMMENT 'Has Serverless SQL consumption' AS `Has Serverless SQL`,
  YEAR(asq.created_date) COMMENT 'Calendar year' AS `Created Year`,
  CASE WHEN MONTH(asq.created_date) = 1 THEN YEAR(asq.created_date) ELSE YEAR(asq.created_date) + 1 END
    COMMENT 'Fiscal year' AS `Fiscal Year`,
  COUNT(DISTINCT asq.approval_request_id) COMMENT 'Total ASQs' AS `Total ASQs`,
  COUNT(DISTINCT asq.account_id) COMMENT 'Engaged accounts' AS `Engaged Accounts`,
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.stage IN ('U5', 'U6'))
    COMMENT 'UCOs at production' AS `Production UCOs`,
  COUNT(DISTINCT asq.account_id) FILTER (WHERE COALESCE(ao.dlt_dbu_dollars_qtd, 0) > 0)
    COMMENT 'Accounts with Lakeflow' AS `Lakeflow Influenced Accounts`,
  COUNT(DISTINCT asq.account_id) FILTER (WHERE COALESCE(ao.dbsql_serverless_dbu_dollars_qtd, 0) > 0)
    COMMENT 'Accounts with Serverless' AS `Serverless Influenced Accounts`,
  COUNT(DISTINCT asq.account_id) FILTER (WHERE COALESCE(ao.dlt_dbu_dollars_qtd, 0) > 0) * 1.0
    / NULLIF(COUNT(DISTINCT asq.account_id), 0)
    COMMENT 'Lakeflow adoption rate' AS `Lakeflow Adoption Rate`,
  COUNT(DISTINCT asq.account_id) FILTER (WHERE COALESCE(ao.dbsql_serverless_dbu_dollars_qtd, 0) > 0) * 1.0
    / NULLIF(COUNT(DISTINCT asq.account_id), 0)
    COMMENT 'Serverless adoption rate' AS `Serverless Adoption Rate`,
  SUM(ao.dlt_dbu_dollars_qtd) COMMENT 'Lakeflow DBU QTD' AS `Lakeflow DBU QTD`,
  SUM(ao.dbsql_serverless_dbu_dollars_qtd) COMMENT 'Serverless DBU QTD' AS `Serverless DBU QTD`,
  SUM(COALESCE(ao.dlt_dbu_dollars_qtd, 0) + COALESCE(ao.dbsql_serverless_dbu_dollars_qtd, 0))
    COMMENT 'Total product DBUs' AS `Total Product DBUs`
FROM main.gtm_silver.approval_request_detail asq
LEFT JOIN main.gtm_silver.individual_hierarchy_salesforce hier ON asq.owner_user_id = hier.user_id
LEFT JOIN main.gtm_gold.account_obt ao
  ON asq.account_id = ao.account_id
  AND ao.fiscal_year_quarter = "FY'26 Q4"
LEFT JOIN main.gtm_silver.use_case_detail uco
  ON asq.account_id = uco.account_id
  AND uco.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.use_case_detail)
  AND uco.is_active_ind = true
WHERE asq.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail);

-- ============================================================================
-- METRIC #8: CUSTOMER RISK REDUCTION (Competitive Wins & Mitigation)
-- ============================================================================
CREATE OR REPLACE METRIC VIEW home_christopher_chalcraft.cjc_views.mv_customer_risk_reduction
COMMENT 'Customer risk reduction metrics. Charter Metric #8. Tracks competitive wins and risk mitigation ASQs.'
AS
SELECT
  asq.business_unit COMMENT 'BU' AS `Business Unit`,
  asq.region_level_1 COMMENT 'Region' AS `Region`,
  asq.owner_user_name COMMENT 'SSA owner name' AS `Owner`,
  hier.line_manager_name COMMENT 'Direct manager (L1)' AS `Manager L1`,
  asq.account_name COMMENT 'Customer account name' AS `Account`,
  CASE
    WHEN LOWER(asq.support_type) LIKE '%migration%' THEN 'Migration'
    WHEN LOWER(asq.request_description) LIKE '%churn%' THEN 'Churn Risk'
    WHEN LOWER(asq.request_description) LIKE '%mitigation%' THEN 'Mitigation'
    WHEN uco.competitor_status = 'Active' THEN 'Active Compete'
    WHEN uco.primary_competitor IS NOT NULL THEN 'Has Competitor'
    ELSE 'Standard'
  END COMMENT 'Risk context' AS `Risk Context`,
  CASE
    WHEN uco.primary_competitor LIKE '%Microsoft%' OR uco.primary_competitor LIKE '%Fabric%' THEN 'Microsoft'
    WHEN uco.primary_competitor LIKE '%Snowflake%' THEN 'Snowflake'
    WHEN uco.primary_competitor LIKE '%AWS%' OR uco.primary_competitor LIKE '%Redshift%' THEN 'AWS'
    WHEN uco.primary_competitor IS NOT NULL THEN 'Other Competitor'
    ELSE 'No Competitor'
  END COMMENT 'Competitor category' AS `Competitor Category`,
  uco.primary_competitor COMMENT 'Primary competitor' AS `Primary Competitor`,
  YEAR(asq.created_date) COMMENT 'Calendar year' AS `Created Year`,
  CASE WHEN MONTH(asq.created_date) = 1 THEN YEAR(asq.created_date) ELSE YEAR(asq.created_date) + 1 END
    COMMENT 'Fiscal year' AS `Fiscal Year`,
  COUNT(DISTINCT asq.approval_request_id) COMMENT 'Total ASQs' AS `Total ASQs`,
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.primary_competitor IS NOT NULL)
    COMMENT 'Competitive UCOs' AS `Competitive UCOs`,
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.primary_competitor IS NOT NULL AND uco.stage IN ('U5', 'U6'))
    COMMENT 'Competitive wins' AS `Competitive Wins`,
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.primary_competitor IS NOT NULL AND uco.stage = 'Lost')
    COMMENT 'Competitive losses' AS `Competitive Losses`,
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.primary_competitor IS NOT NULL AND uco.stage IN ('U5', 'U6')) * 1.0
    / NULLIF(COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.primary_competitor IS NOT NULL AND uco.stage IN ('U5', 'U6', 'Lost')), 0)
    COMMENT 'Competitive win rate' AS `Competitive Win Rate`,
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.primary_competitor LIKE '%Microsoft%' AND uco.stage IN ('U5', 'U6'))
    COMMENT 'Microsoft wins' AS `Microsoft Displacement Wins`,
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.primary_competitor LIKE '%Snowflake%' AND uco.stage IN ('U5', 'U6'))
    COMMENT 'Snowflake wins' AS `Snowflake Displacement Wins`
FROM main.gtm_silver.approval_request_detail asq
LEFT JOIN main.gtm_silver.individual_hierarchy_salesforce hier ON asq.owner_user_id = hier.user_id
LEFT JOIN main.gtm_silver.use_case_detail uco
  ON asq.account_id = uco.account_id
  AND uco.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.use_case_detail)
  AND uco.is_active_ind = true
WHERE asq.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail);

-- ============================================================================
-- VALIDATION QUERIES
-- ============================================================================

-- Verify all metric views exist
SHOW TABLES IN home_christopher_chalcraft.cjc_views LIKE 'mv_*';

-- Test each metric view
SELECT 'mv_time_to_adopt' AS view_name, COUNT(*) AS row_count FROM home_christopher_chalcraft.cjc_views.mv_time_to_adopt LIMIT 1;
SELECT 'mv_asset_reuse' AS view_name, COUNT(*) AS row_count FROM home_christopher_chalcraft.cjc_views.mv_asset_reuse LIMIT 1;
SELECT 'mv_self_service_health' AS view_name, COUNT(*) AS row_count FROM home_christopher_chalcraft.cjc_views.mv_self_service_health LIMIT 1;
SELECT 'mv_product_impact' AS view_name, COUNT(*) AS row_count FROM home_christopher_chalcraft.cjc_views.mv_product_impact LIMIT 1;
SELECT 'mv_customer_risk_reduction' AS view_name, COUNT(*) AS row_count FROM home_christopher_chalcraft.cjc_views.mv_customer_risk_reduction LIMIT 1;
