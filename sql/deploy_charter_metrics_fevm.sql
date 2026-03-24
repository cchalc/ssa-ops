-- ============================================================================
-- DEPLOY CHARTER METRICS 4-8 TO FEVM-CJC
-- ============================================================================
-- Run this script in Databricks SQL Editor on fevm-cjc workspace:
-- https://fevm-cjc-aws-workspace.cloud.databricks.com/sql/editor
--
-- PREREQUISITES:
-- 1. Run sql/sync/01_export_to_delta.sql on logfood first to replicate source tables
-- 2. Verify source tables exist: cjc_aws_workspace_catalog.ssa_ops_dev.*
--
-- These metric views reference replicated Delta tables, not logfood directly
-- ============================================================================

-- Create schema for metric views
CREATE SCHEMA IF NOT EXISTS cjc_aws_workspace_catalog.ssa_ops_metric_views;

-- ============================================================================
-- METRIC #4: TIME-TO-ADOPT (U3→U4 Transition Time)
-- ============================================================================
CREATE OR REPLACE METRIC VIEW cjc_aws_workspace_catalog.ssa_ops_metric_views.mv_time_to_adopt
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
FROM cjc_aws_workspace_catalog.ssa_ops_dev.approval_request_detail asq
LEFT JOIN cjc_aws_workspace_catalog.ssa_ops_dev.individual_hierarchy_salesforce hier
  ON asq.owner_user_id = hier.user_id
LEFT JOIN cjc_aws_workspace_catalog.ssa_ops_dev.use_case_detail uco
  ON asq.account_id = uco.account_id
  AND uco.is_active_ind = true;

-- ============================================================================
-- METRIC #5: ASSET REUSE RATE
-- ============================================================================
CREATE OR REPLACE METRIC VIEW cjc_aws_workspace_catalog.ssa_ops_metric_views.mv_asset_reuse
COMMENT 'Asset reuse and pattern application metrics. Charter Metric #5. Tracks SSA pattern reuse across accounts.'
AS
SELECT
  asq.business_unit COMMENT 'BU: AMER Enterprise and Emerging, AMER Industries, EMEA, APJ' AS `Business Unit`,
  asq.region_level_1 COMMENT 'Region: CAN, RCT, FINS, etc.' AS `Region`,
  asq.owner_user_name COMMENT 'SSA owner name' AS `Owner`,
  asq.owner_user_id COMMENT 'SSA owner ID' AS `Owner ID`,
  hier.line_manager_name COMMENT 'Direct manager (L1)' AS `Manager L1`,
  hier.`2nd_line_manager_name` COMMENT 'Second-level manager (L2)' AS `Manager L2`,
  asq.account_name COMMENT 'Customer account name' AS `Account`,
  asq.technical_specialization COMMENT 'Technical focus area' AS `Specialization`,
  asq.support_type COMMENT 'Support type' AS `Support Type`,
  YEAR(asq.created_date) COMMENT 'Calendar year' AS `Created Year`,
  CONCAT(YEAR(asq.created_date), '-Q', QUARTER(asq.created_date))
    COMMENT 'Calendar year-quarter' AS `Created Year-Quarter`,
  CASE WHEN MONTH(asq.created_date) = 1 THEN YEAR(asq.created_date) ELSE YEAR(asq.created_date) + 1 END
    COMMENT 'Fiscal year (ends Jan 31)' AS `Fiscal Year`,
  COUNT(DISTINCT asq.approval_request_id) COMMENT 'Total ASQs' AS `Total ASQs`,
  COUNT(DISTINCT asq.account_id) COMMENT 'Unique accounts engaged' AS `Unique Accounts`,
  COUNT(DISTINCT CONCAT(asq.owner_user_id, '|', asq.technical_specialization))
    COMMENT 'Unique SSA + specialization patterns' AS `Pattern Count`,
  COUNT(DISTINCT asq.approval_request_id) * 1.0
    / NULLIF(COUNT(DISTINCT CONCAT(asq.owner_user_id, '|', asq.technical_specialization)), 0)
    COMMENT 'Average ASQs per pattern (higher = more reuse)' AS `Avg ASQs per Pattern`,
  COUNT(DISTINCT asq.account_id) * 1.0
    / NULLIF(COUNT(DISTINCT CONCAT(asq.owner_user_id, '|', asq.technical_specialization)), 0)
    COMMENT 'Average accounts per pattern' AS `Avg Accounts per Pattern`
FROM cjc_aws_workspace_catalog.ssa_ops_dev.approval_request_detail asq
LEFT JOIN cjc_aws_workspace_catalog.ssa_ops_dev.individual_hierarchy_salesforce hier
  ON asq.owner_user_id = hier.user_id
WHERE asq.status NOT IN ('Rejected', 'Cancelled');

-- ============================================================================
-- METRIC #6: SELF-SERVICE HEALTH (ASQ Deflection Proxy)
-- ============================================================================
CREATE OR REPLACE METRIC VIEW cjc_aws_workspace_catalog.ssa_ops_metric_views.mv_self_service_health
COMMENT 'Self-service health proxy for ASQ deflection. Charter Metric #6 (proxy). Tracks account engagement patterns indicating self-sufficiency.'
AS
WITH account_engagement AS (
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
    FIRST_VALUE(asq.owner_user_name) OVER (
      PARTITION BY asq.account_id ORDER BY asq.created_date DESC
    ) AS latest_ssa,
    FIRST_VALUE(asq.owner_user_id) OVER (
      PARTITION BY asq.account_id ORDER BY asq.created_date DESC
    ) AS latest_ssa_id
  FROM cjc_aws_workspace_catalog.ssa_ops_dev.approval_request_detail asq
  WHERE asq.status NOT IN ('Rejected', 'Cancelled')
  GROUP BY asq.account_id, asq.account_name, asq.business_unit, asq.region_level_1,
           asq.owner_user_name, asq.owner_user_id, asq.created_date
),
account_metrics AS (
  SELECT
    ae.*,
    CASE
      WHEN ae.total_asqs > 1 THEN ae.engagement_span_days * 1.0 / (ae.total_asqs - 1)
      ELSE NULL
    END AS avg_days_between_asqs,
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
      account_id, account_name, business_unit, region_level_1,
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
  am.business_unit COMMENT 'BU: AMER Enterprise and Emerging, AMER Industries, EMEA, APJ' AS `Business Unit`,
  am.region_level_1 COMMENT 'Region: CAN, RCT, FINS, etc.' AS `Region`,
  am.latest_ssa COMMENT 'Most recent SSA on account' AS `Latest SSA`,
  hier.line_manager_name COMMENT 'Direct manager (L1)' AS `Manager L1`,
  hier.`2nd_line_manager_name` COMMENT 'Second-level manager (L2)' AS `Manager L2`,
  am.self_service_tier COMMENT 'Self-service tier based on engagement frequency' AS `Self-Service Tier`,
  CASE
    WHEN am.avg_days_between_asqs IS NULL THEN 'Single ASQ'
    WHEN am.avg_days_between_asqs <= 30 THEN 'Very Frequent (<30 days)'
    WHEN am.avg_days_between_asqs <= 60 THEN 'Frequent (30-60 days)'
    WHEN am.avg_days_between_asqs <= 90 THEN 'Regular (60-90 days)'
    WHEN am.avg_days_between_asqs <= 180 THEN 'Occasional (90-180 days)'
    ELSE 'Rare (>180 days)'
  END COMMENT 'ASQ frequency band' AS `Engagement Frequency`,
  am.account_name COMMENT 'Customer account name' AS `Account`,
  YEAR(am.first_asq_date) COMMENT 'Year of first ASQ engagement' AS `First Engaged Year`,
  YEAR(am.last_asq_date) COMMENT 'Year of most recent ASQ' AS `Last Engaged Year`,
  COUNT(DISTINCT am.account_id) COMMENT 'Total accounts' AS `Total Accounts`,
  COUNT(DISTINCT am.account_id) FILTER (WHERE am.self_service_tier = 'One-Time (Enabled)')
    COMMENT 'Accounts with single ASQ (enabled after one engagement)' AS `One-Time Accounts`,
  COUNT(DISTINCT am.account_id) FILTER (WHERE am.self_service_tier IN ('Self-Sufficient', 'Highly Self-Sufficient'))
    COMMENT 'Self-sufficient accounts (>90 days between ASQs)' AS `Self-Sufficient Accounts`,
  COUNT(DISTINCT am.account_id) FILTER (WHERE am.self_service_tier = 'Regular Engagement')
    COMMENT 'Regular engagement accounts (30-90 days)' AS `Regular Engagement Accounts`,
  COUNT(DISTINCT am.account_id) FILTER (WHERE am.self_service_tier = 'Frequent Dependency')
    COMMENT 'Frequent dependency accounts (<30 days)' AS `Frequent Dependency Accounts`,
  (COUNT(DISTINCT am.account_id) FILTER (WHERE am.self_service_tier = 'One-Time (Enabled)')
   + COUNT(DISTINCT am.account_id) FILTER (WHERE am.self_service_tier IN ('Self-Sufficient', 'Highly Self-Sufficient'))) * 1.0
    / NULLIF(COUNT(DISTINCT am.account_id), 0)
    COMMENT 'Self-service rate (one-time + self-sufficient / total)' AS `Self-Service Rate`,
  AVG(am.avg_days_between_asqs)
    COMMENT 'Average days between ASQs across accounts' AS `Avg Days Between ASQs`,
  SUM(am.total_asqs) COMMENT 'Total ASQs across all accounts' AS `Total ASQs`,
  SUM(am.total_asqs) * 1.0 / NULLIF(COUNT(DISTINCT am.account_id), 0)
    COMMENT 'ASQs per account (lower = better enablement)' AS `ASQs per Account`
FROM account_metrics am
LEFT JOIN cjc_aws_workspace_catalog.ssa_ops_dev.individual_hierarchy_salesforce hier
  ON am.latest_ssa_id = hier.user_id;

-- ============================================================================
-- METRIC #7: PRODUCT IMPACT
-- ============================================================================
CREATE OR REPLACE METRIC VIEW cjc_aws_workspace_catalog.ssa_ops_metric_views.mv_product_impact
COMMENT 'Product adoption impact from SSA engagements. Charter Metric #7: Product Impact. Tracks adoption of compensation-tied products (Lakeflow, Serverless, Model Serving).'
AS
SELECT
  asq.business_unit COMMENT 'BU: AMER Enterprise and Emerging, AMER Industries, EMEA, APJ' AS `Business Unit`,
  asq.region_level_1 COMMENT 'Region: CAN, RCT, FINS, etc.' AS `Region`,
  asq.owner_user_name COMMENT 'SSA owner name' AS `Owner`,
  asq.owner_user_id COMMENT 'SSA owner ID' AS `Owner ID`,
  hier.line_manager_name COMMENT 'Direct manager (L1)' AS `Manager L1`,
  hier.`2nd_line_manager_name` COMMENT 'Second-level manager (L2)' AS `Manager L2`,
  asq.account_id COMMENT 'Account ID' AS `Account ID`,
  asq.account_name COMMENT 'Customer account name' AS `Account`,
  ao.account_segment COMMENT 'Customer segment' AS `Account Segment`,
  ao.vertical_segment COMMENT 'Industry vertical' AS `Account Vertical`,
  uco.use_case_product COMMENT 'UCO product category' AS `UCO Product`,
  uco.stage COMMENT 'Current UCO stage' AS `UCO Stage`,
  CASE
    WHEN uco.use_case_product LIKE '%Lakeflow%' OR uco.use_case_product LIKE '%DLT%'
         OR uco.use_case_product LIKE '%Delta Live%' THEN 'Lakeflow'
    WHEN uco.use_case_product LIKE '%Serverless%' OR uco.use_case_product LIKE '%SQL Warehouse%' THEN 'Serverless SQL'
    WHEN uco.use_case_product LIKE '%Model Serving%' OR uco.use_case_product LIKE '%ML%'
         OR uco.use_case_product LIKE '%AI%' THEN 'AI/ML'
    WHEN uco.use_case_product LIKE '%Unity Catalog%' OR uco.use_case_product LIKE '%Governance%' THEN 'Unity Catalog'
    WHEN uco.use_case_product LIKE '%Mosaic%' OR uco.use_case_product LIKE '%GenAI%' THEN 'Mosaic AI'
    ELSE 'Other'
  END COMMENT 'Product grouping for compensation tracking' AS `Product Group`,
  asq.technical_specialization COMMENT 'Technical focus' AS `Specialization`,
  asq.support_type COMMENT 'Support type' AS `Support Type`,
  YEAR(asq.created_date) COMMENT 'Calendar year' AS `Created Year`,
  CASE WHEN MONTH(asq.created_date) = 1 THEN YEAR(asq.created_date) ELSE YEAR(asq.created_date) + 1 END
    COMMENT 'Fiscal year (ends Jan 31)' AS `Fiscal Year`,
  CASE WHEN COALESCE(ao.dlt_dbu_dollars_qtd, 0) > 0 THEN 'Yes' ELSE 'No' END
    COMMENT 'Account has Lakeflow/DLT consumption' AS `Has Lakeflow`,
  CASE WHEN COALESCE(ao.dbsql_serverless_dbu_dollars_qtd, 0) > 0 THEN 'Yes' ELSE 'No' END
    COMMENT 'Account has Serverless SQL consumption' AS `Has Serverless SQL`,
  CASE WHEN COALESCE(ao.genai_gpu_model_serving_dbu_dollars_qtd, 0) + COALESCE(ao.genai_cpu_model_serving_dbu_dollars_qtd, 0) > 0 THEN 'Yes' ELSE 'No' END
    COMMENT 'Account has Model Serving consumption' AS `Has Model Serving`,
  COUNT(DISTINCT asq.approval_request_id) COMMENT 'Total ASQs' AS `Total ASQs`,
  COUNT(DISTINCT asq.account_id) COMMENT 'Unique accounts engaged' AS `Engaged Accounts`,
  COUNT(DISTINCT uco.usecase_id) COMMENT 'Total UCOs linked' AS `Total UCOs`,
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.stage IN ('U4', 'U5', 'U6'))
    COMMENT 'UCOs at tech win or beyond (U4+)' AS `Confirmed UCOs`,
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.stage IN ('U5', 'U6'))
    COMMENT 'UCOs at production (U5+)' AS `Production UCOs`,
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE
    uco.use_case_product LIKE '%Lakeflow%' OR uco.use_case_product LIKE '%DLT%')
    COMMENT 'Lakeflow/DLT UCOs' AS `Lakeflow UCOs`,
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE
    uco.use_case_product LIKE '%Serverless%' OR uco.use_case_product LIKE '%SQL Warehouse%')
    COMMENT 'Serverless SQL UCOs' AS `Serverless UCOs`,
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE
    uco.use_case_product LIKE '%Model Serving%' OR uco.use_case_product LIKE '%ML%'
    OR uco.use_case_product LIKE '%AI%')
    COMMENT 'AI/ML UCOs' AS `AI ML UCOs`,
  COUNT(DISTINCT asq.account_id) FILTER (WHERE COALESCE(ao.dlt_dbu_dollars_qtd, 0) > 0)
    COMMENT 'Engaged accounts with Lakeflow consumption' AS `Lakeflow Influenced Accounts`,
  COUNT(DISTINCT asq.account_id) FILTER (WHERE COALESCE(ao.dbsql_serverless_dbu_dollars_qtd, 0) > 0)
    COMMENT 'Engaged accounts with Serverless consumption' AS `Serverless Influenced Accounts`,
  COUNT(DISTINCT asq.account_id) FILTER (WHERE COALESCE(ao.genai_gpu_model_serving_dbu_dollars_qtd, 0) + COALESCE(ao.genai_cpu_model_serving_dbu_dollars_qtd, 0) > 0)
    COMMENT 'Engaged accounts with Model Serving consumption' AS `Model Serving Influenced Accounts`,
  COUNT(DISTINCT asq.account_id) FILTER (WHERE COALESCE(ao.dlt_dbu_dollars_qtd, 0) > 0) * 1.0
    / NULLIF(COUNT(DISTINCT asq.account_id), 0)
    COMMENT 'Percentage of engaged accounts with Lakeflow' AS `Lakeflow Adoption Rate`,
  COUNT(DISTINCT asq.account_id) FILTER (WHERE COALESCE(ao.dbsql_serverless_dbu_dollars_qtd, 0) > 0) * 1.0
    / NULLIF(COUNT(DISTINCT asq.account_id), 0)
    COMMENT 'Percentage of engaged accounts with Serverless' AS `Serverless Adoption Rate`,
  SUM(ao.dbsql_serverless_dbu_dollars_qtd)
    COMMENT 'Total SQL Serverless DBUs QTD' AS `Serverless DBU QTD`,
  SUM(ao.dlt_dbu_dollars_qtd)
    COMMENT 'Total Lakeflow/DLT DBUs QTD' AS `Lakeflow DBU QTD`,
  SUM(COALESCE(ao.genai_gpu_model_serving_dbu_dollars_qtd, 0) + COALESCE(ao.genai_cpu_model_serving_dbu_dollars_qtd, 0))
    COMMENT 'Total Model Serving DBUs QTD' AS `Model Serving DBU QTD`,
  SUM(COALESCE(ao.dbsql_serverless_dbu_dollars_qtd, 0) +
      COALESCE(ao.dlt_dbu_dollars_qtd, 0) +
      COALESCE(ao.genai_gpu_model_serving_dbu_dollars_qtd, 0) +
      COALESCE(ao.genai_cpu_model_serving_dbu_dollars_qtd, 0))
    COMMENT 'Total compensation-aligned product DBUs' AS `Total Product DBUs`,
  SUM(asq.actual_effort_in_days)
    COMMENT 'Total SSA effort days' AS `Total Effort Days`
FROM cjc_aws_workspace_catalog.ssa_ops_dev.approval_request_detail asq
LEFT JOIN cjc_aws_workspace_catalog.ssa_ops_dev.individual_hierarchy_salesforce hier
  ON asq.owner_user_id = hier.user_id
LEFT JOIN cjc_aws_workspace_catalog.ssa_ops_dev.account_obt ao
  ON asq.account_id = ao.account_id
LEFT JOIN cjc_aws_workspace_catalog.ssa_ops_dev.use_case_detail uco
  ON asq.account_id = uco.account_id
  AND uco.is_active_ind = true;

-- ============================================================================
-- METRIC #8: CUSTOMER RISK REDUCTION
-- ============================================================================
CREATE OR REPLACE METRIC VIEW cjc_aws_workspace_catalog.ssa_ops_metric_views.mv_customer_risk_reduction
COMMENT 'Customer risk reduction metrics. Charter Metric #8. Tracks competitive wins, displacement scenarios, and risk mitigation ASQs.'
AS
SELECT
  asq.business_unit COMMENT 'BU: AMER Enterprise and Emerging, AMER Industries, EMEA, APJ' AS `Business Unit`,
  asq.region_level_1 COMMENT 'Region: CAN, RCT, FINS, etc.' AS `Region`,
  asq.owner_user_name COMMENT 'SSA owner name' AS `Owner`,
  asq.owner_user_id COMMENT 'SSA owner ID' AS `Owner ID`,
  hier.line_manager_name COMMENT 'Direct manager (L1)' AS `Manager L1`,
  hier.`2nd_line_manager_name` COMMENT 'Second-level manager (L2)' AS `Manager L2`,
  asq.account_id COMMENT 'Account ID' AS `Account ID`,
  asq.account_name COMMENT 'Customer account name' AS `Account`,
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
  END COMMENT 'Risk context classification' AS `Risk Context`,
  CASE
    WHEN uco.primary_competitor LIKE '%Microsoft%' OR uco.primary_competitor LIKE '%Fabric%'
         OR uco.primary_competitor LIKE '%Synapse%' OR uco.primary_competitor LIKE '%Power BI%' THEN 'Microsoft'
    WHEN uco.primary_competitor LIKE '%Snowflake%' THEN 'Snowflake'
    WHEN uco.primary_competitor LIKE '%AWS%' OR uco.primary_competitor LIKE '%Redshift%'
         OR uco.primary_competitor LIKE '%Glue%' THEN 'AWS'
    WHEN uco.primary_competitor LIKE '%Google%' OR uco.primary_competitor LIKE '%BigQuery%' THEN 'Google Cloud'
    WHEN uco.primary_competitor IS NOT NULL THEN 'Other Competitor'
    ELSE 'No Competitor'
  END COMMENT 'Primary competitor category' AS `Competitor Category`,
  uco.primary_competitor COMMENT 'Primary competitor name' AS `Primary Competitor`,
  uco.competitor_status COMMENT 'Competitor status: Active, Won, Lost' AS `Competitor Status`,
  uco.stage COMMENT 'UCO stage' AS `UCO Stage`,
  asq.technical_specialization COMMENT 'Technical focus' AS `Specialization`,
  asq.support_type COMMENT 'Support type' AS `Support Type`,
  YEAR(asq.created_date) COMMENT 'Calendar year' AS `Created Year`,
  CASE WHEN MONTH(asq.created_date) = 1 THEN YEAR(asq.created_date) ELSE YEAR(asq.created_date) + 1 END
    COMMENT 'Fiscal year (ends Jan 31)' AS `Fiscal Year`,
  COUNT(DISTINCT asq.approval_request_id) COMMENT 'Total ASQs' AS `Total ASQs`,
  COUNT(DISTINCT asq.approval_request_id) FILTER (WHERE
    LOWER(asq.support_type) LIKE '%migration%'
    OR LOWER(asq.support_type) LIKE '%competitive%'
    OR LOWER(asq.request_description) LIKE '%churn%'
    OR LOWER(asq.request_description) LIKE '%mitigation%'
    OR LOWER(asq.request_description) LIKE '%at risk%'
    OR LOWER(asq.request_description) LIKE '%competitive%'
    OR uco.competitor_status = 'Active')
    COMMENT 'ASQs with risk/competitive context' AS `Risk-Related ASQs`,
  COUNT(DISTINCT asq.approval_request_id) FILTER (WHERE
    LOWER(asq.support_type) LIKE '%migration%'
    OR LOWER(asq.request_description) LIKE '%migration%')
    COMMENT 'Migration-related ASQs' AS `Migration ASQs`,
  COUNT(DISTINCT uco.usecase_id) COMMENT 'Total UCOs' AS `Total UCOs`,
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.primary_competitor IS NOT NULL)
    COMMENT 'UCOs with competitor identified' AS `Competitive UCOs`,
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.competitor_status = 'Active')
    COMMENT 'UCOs with active competitor' AS `Active Compete UCOs`,
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE
    uco.primary_competitor IS NOT NULL AND uco.stage IN ('U5', 'U6'))
    COMMENT 'Competitive wins (UCO at production with competitor)' AS `Competitive Wins`,
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE
    uco.primary_competitor IS NOT NULL AND uco.stage = 'Lost')
    COMMENT 'Competitive losses' AS `Competitive Losses`,
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE
    uco.primary_competitor IS NOT NULL AND uco.stage IN ('U5', 'U6')) * 1.0
    / NULLIF(COUNT(DISTINCT uco.usecase_id) FILTER (WHERE
        uco.primary_competitor IS NOT NULL AND uco.stage IN ('U5', 'U6', 'Lost')), 0)
    COMMENT 'Competitive win rate (wins / (wins + losses))' AS `Competitive Win Rate`,
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE
    (uco.primary_competitor LIKE '%Microsoft%' OR uco.primary_competitor LIKE '%Fabric%'
     OR uco.primary_competitor LIKE '%Synapse%' OR uco.primary_competitor LIKE '%Power BI%')
    AND uco.stage IN ('U5', 'U6'))
    COMMENT 'Wins against Microsoft (Fabric/Synapse/Power BI)' AS `Microsoft Displacement Wins`,
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE
    uco.primary_competitor LIKE '%Snowflake%'
    AND uco.stage IN ('U5', 'U6'))
    COMMENT 'Wins against Snowflake' AS `Snowflake Displacement Wins`,
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE
    (uco.primary_competitor LIKE '%AWS%' OR uco.primary_competitor LIKE '%Redshift%')
    AND uco.stage IN ('U5', 'U6'))
    COMMENT 'Wins against AWS (Redshift/Glue)' AS `AWS Displacement Wins`,
  SUM(uco.estimated_quarterly_dollar_dbus) FILTER (WHERE
    uco.primary_competitor IS NOT NULL AND uco.stage IN ('U5', 'U6'))
    COMMENT 'Quarterly DBUs from competitive wins' AS `Competitive Win DBUs`,
  SUM(curated.estimated_arr_usd) FILTER (WHERE
    uco.primary_competitor IS NOT NULL AND uco.stage IN ('U5', 'U6'))
    COMMENT 'ARR from competitive wins' AS `Competitive Win ARR`,
  SUM(asq.actual_effort_in_days)
    COMMENT 'Total SSA effort days' AS `Total Effort Days`
FROM cjc_aws_workspace_catalog.ssa_ops_dev.approval_request_detail asq
LEFT JOIN cjc_aws_workspace_catalog.ssa_ops_dev.individual_hierarchy_salesforce hier
  ON asq.owner_user_id = hier.user_id
LEFT JOIN cjc_aws_workspace_catalog.ssa_ops_dev.use_case_detail uco
  ON asq.account_id = uco.account_id
  AND uco.is_active_ind = true
LEFT JOIN cjc_aws_workspace_catalog.ssa_ops_dev.core_usecase_curated curated
  ON uco.usecase_id = curated.use_case_id;

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================
-- Run these after deployment to verify metric views work:

-- SELECT 'mv_time_to_adopt' as view_name, COUNT(*) as sample_rows FROM cjc_aws_workspace_catalog.ssa_ops_metric_views.mv_time_to_adopt LIMIT 100
-- UNION ALL SELECT 'mv_asset_reuse', COUNT(*) FROM cjc_aws_workspace_catalog.ssa_ops_metric_views.mv_asset_reuse LIMIT 100
-- UNION ALL SELECT 'mv_self_service_health', COUNT(*) FROM cjc_aws_workspace_catalog.ssa_ops_metric_views.mv_self_service_health LIMIT 100
-- UNION ALL SELECT 'mv_product_impact', COUNT(*) FROM cjc_aws_workspace_catalog.ssa_ops_metric_views.mv_product_impact LIMIT 100
-- UNION ALL SELECT 'mv_customer_risk_reduction', COUNT(*) FROM cjc_aws_workspace_catalog.ssa_ops_metric_views.mv_customer_risk_reduction LIMIT 100;
