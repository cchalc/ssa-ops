-- ============================================================================
-- mv_product_impact - Product Adoption Impact from SSA Engagements
-- ============================================================================
-- Charter Metric #7: Product Impact
-- Measures whether SSA-engaged accounts show meaningful product adoption
-- Focus on compensation-tied products: Lakeflow, Serverless, Model Serving
-- Sources: GTM Silver (ASQ, UCO) + GTM Gold (account_obt)
-- Adoption derived from DBU consumption > 0
-- NO HARDCODED FILTERS - all BU/manager filtering at query time
-- ============================================================================

CREATE OR REPLACE METRIC VIEW home_christopher_chalcraft.cjc_views.mv_product_impact
COMMENT 'Product adoption impact from SSA engagements. Charter Metric #7: Product Impact. Tracks adoption of compensation-tied products (Lakeflow, Serverless, Model Serving).'
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
  ao.account_segment
    COMMENT 'Customer segment' AS `Account Segment`,
  ao.vertical_segment
    COMMENT 'Industry vertical' AS `Account Vertical`,

  -- ========================================================================
  -- UCO PRODUCT DIMENSIONS
  -- ========================================================================

  uco.use_case_product
    COMMENT 'UCO product category' AS `UCO Product`,
  uco.stage
    COMMENT 'Current UCO stage' AS `UCO Stage`,

  -- Product Category Grouping
  CASE
    WHEN uco.use_case_product LIKE '%Lakeflow%' OR uco.use_case_product LIKE '%DLT%'
         OR uco.use_case_product LIKE '%Delta Live%' THEN 'Lakeflow'
    WHEN uco.use_case_product LIKE '%Serverless%' OR uco.use_case_product LIKE '%SQL Warehouse%' THEN 'Serverless SQL'
    WHEN uco.use_case_product LIKE '%Model Serving%' OR uco.use_case_product LIKE '%ML%'
         OR uco.use_case_product LIKE '%AI%' THEN 'AI/ML'
    WHEN uco.use_case_product LIKE '%Unity Catalog%' OR uco.use_case_product LIKE '%Governance%' THEN 'Unity Catalog'
    WHEN uco.use_case_product LIKE '%Mosaic%' OR uco.use_case_product LIKE '%GenAI%' THEN 'Mosaic AI'
    ELSE 'Other'
  END
    COMMENT 'Product grouping for compensation tracking' AS `Product Group`,

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
  -- PRODUCT ADOPTION FLAGS (derived from DBU consumption > 0)
  -- ========================================================================

  CASE WHEN COALESCE(ao.dlt_dbu_dollars_qtd, 0) > 0 THEN 'Yes' ELSE 'No' END
    COMMENT 'Account has Lakeflow/DLT consumption' AS `Has Lakeflow`,
  CASE WHEN COALESCE(ao.dbsql_serverless_dbu_dollars_qtd, 0) > 0 THEN 'Yes' ELSE 'No' END
    COMMENT 'Account has Serverless SQL consumption' AS `Has Serverless SQL`,
  CASE WHEN COALESCE(ao.genai_gpu_model_serving_dbu_dollars_qtd, 0) + COALESCE(ao.genai_cpu_model_serving_dbu_dollars_qtd, 0) > 0 THEN 'Yes' ELSE 'No' END
    COMMENT 'Account has Model Serving consumption' AS `Has Model Serving`,
  CASE WHEN COALESCE(ao.genai_vector_search_serving_dbu_dollars_qtd, 0) > 0 THEN 'Yes' ELSE 'No' END
    COMMENT 'Account has Vector Search consumption' AS `Has Vector Search`,

  -- ========================================================================
  -- ASQ & UCO VOLUME MEASURES
  -- ========================================================================

  COUNT(DISTINCT asq.approval_request_id)
    COMMENT 'Total ASQs' AS `Total ASQs`,
  COUNT(DISTINCT asq.account_id)
    COMMENT 'Unique accounts engaged' AS `Engaged Accounts`,
  COUNT(DISTINCT uco.usecase_id)
    COMMENT 'Total UCOs linked' AS `Total UCOs`,
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.stage IN ('U4', 'U5', 'U6'))
    COMMENT 'UCOs at tech win or beyond (U4+)' AS `Confirmed UCOs`,
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.stage IN ('U5', 'U6'))
    COMMENT 'UCOs at production (U5+)' AS `Production UCOs`,

  -- ========================================================================
  -- PRODUCT-SPECIFIC UCO COUNTS
  -- ========================================================================

  -- Lakeflow UCOs
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE
    uco.use_case_product LIKE '%Lakeflow%' OR uco.use_case_product LIKE '%DLT%')
    COMMENT 'Lakeflow/DLT UCOs' AS `Lakeflow UCOs`,
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE
    (uco.use_case_product LIKE '%Lakeflow%' OR uco.use_case_product LIKE '%DLT%')
    AND uco.stage IN ('U5', 'U6'))
    COMMENT 'Lakeflow UCOs at production' AS `Lakeflow Production UCOs`,

  -- Serverless UCOs
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE
    uco.use_case_product LIKE '%Serverless%' OR uco.use_case_product LIKE '%SQL Warehouse%')
    COMMENT 'Serverless SQL UCOs' AS `Serverless UCOs`,
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE
    (uco.use_case_product LIKE '%Serverless%' OR uco.use_case_product LIKE '%SQL Warehouse%')
    AND uco.stage IN ('U5', 'U6'))
    COMMENT 'Serverless UCOs at production' AS `Serverless Production UCOs`,

  -- AI/ML UCOs
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE
    uco.use_case_product LIKE '%Model Serving%' OR uco.use_case_product LIKE '%ML%'
    OR uco.use_case_product LIKE '%AI%')
    COMMENT 'AI/ML UCOs' AS `AI ML UCOs`,
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE
    (uco.use_case_product LIKE '%Model Serving%' OR uco.use_case_product LIKE '%ML%')
    AND uco.stage IN ('U5', 'U6'))
    COMMENT 'AI/ML UCOs at production' AS `AI ML Production UCOs`,

  -- Unity Catalog UCOs
  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE
    uco.use_case_product LIKE '%Unity Catalog%' OR uco.use_case_product LIKE '%Governance%')
    COMMENT 'Unity Catalog UCOs' AS `Unity Catalog UCOs`,

  -- ========================================================================
  -- PRODUCT ADOPTION INFLUENCE MEASURES
  -- ========================================================================

  -- Accounts with product consumption (derived from DBU > 0)
  COUNT(DISTINCT asq.account_id) FILTER (WHERE COALESCE(ao.dlt_dbu_dollars_qtd, 0) > 0)
    COMMENT 'Engaged accounts with Lakeflow consumption' AS `Lakeflow Influenced Accounts`,
  COUNT(DISTINCT asq.account_id) FILTER (WHERE COALESCE(ao.dbsql_serverless_dbu_dollars_qtd, 0) > 0)
    COMMENT 'Engaged accounts with Serverless consumption' AS `Serverless Influenced Accounts`,
  COUNT(DISTINCT asq.account_id) FILTER (WHERE COALESCE(ao.genai_gpu_model_serving_dbu_dollars_qtd, 0) + COALESCE(ao.genai_cpu_model_serving_dbu_dollars_qtd, 0) > 0)
    COMMENT 'Engaged accounts with Model Serving consumption' AS `Model Serving Influenced Accounts`,

  -- ========================================================================
  -- PRODUCT ADOPTION RATES
  -- ========================================================================

  COUNT(DISTINCT asq.account_id) FILTER (WHERE COALESCE(ao.dlt_dbu_dollars_qtd, 0) > 0) * 1.0
    / NULLIF(COUNT(DISTINCT asq.account_id), 0)
    COMMENT 'Percentage of engaged accounts with Lakeflow' AS `Lakeflow Adoption Rate`,

  COUNT(DISTINCT asq.account_id) FILTER (WHERE COALESCE(ao.dbsql_serverless_dbu_dollars_qtd, 0) > 0) * 1.0
    / NULLIF(COUNT(DISTINCT asq.account_id), 0)
    COMMENT 'Percentage of engaged accounts with Serverless' AS `Serverless Adoption Rate`,

  COUNT(DISTINCT asq.account_id) FILTER (WHERE COALESCE(ao.genai_gpu_model_serving_dbu_dollars_qtd, 0) + COALESCE(ao.genai_cpu_model_serving_dbu_dollars_qtd, 0) > 0) * 1.0
    / NULLIF(COUNT(DISTINCT asq.account_id), 0)
    COMMENT 'Percentage of engaged accounts with Model Serving' AS `Model Serving Adoption Rate`,

  -- ========================================================================
  -- CONSUMPTION IMPACT BY PRODUCT (using actual column names)
  -- ========================================================================

  SUM(ao.dbsql_serverless_dbu_dollars_qtd)
    COMMENT 'Total SQL Serverless DBUs QTD' AS `Serverless DBU QTD`,
  SUM(ao.dlt_dbu_dollars_qtd)
    COMMENT 'Total Lakeflow/DLT DBUs QTD' AS `Lakeflow DBU QTD`,
  SUM(COALESCE(ao.genai_gpu_model_serving_dbu_dollars_qtd, 0) + COALESCE(ao.genai_cpu_model_serving_dbu_dollars_qtd, 0))
    COMMENT 'Total Model Serving DBUs QTD' AS `Model Serving DBU QTD`,
  SUM(ao.genai_vector_search_serving_dbu_dollars_qtd)
    COMMENT 'Total Vector Search DBUs QTD' AS `Vector Search DBU QTD`,

  -- Total product-aligned DBUs
  SUM(COALESCE(ao.dbsql_serverless_dbu_dollars_qtd, 0) +
      COALESCE(ao.dlt_dbu_dollars_qtd, 0) +
      COALESCE(ao.genai_gpu_model_serving_dbu_dollars_qtd, 0) +
      COALESCE(ao.genai_cpu_model_serving_dbu_dollars_qtd, 0))
    COMMENT 'Total compensation-aligned product DBUs' AS `Total Product DBUs`,

  -- ========================================================================
  -- PRODUCT IMPACT EFFICIENCY
  -- ========================================================================

  SUM(asq.actual_effort_in_days)
    COMMENT 'Total SSA effort days' AS `Total Effort Days`,

  SUM(COALESCE(ao.dbsql_serverless_dbu_dollars_qtd, 0) +
      COALESCE(ao.dlt_dbu_dollars_qtd, 0) +
      COALESCE(ao.genai_gpu_model_serving_dbu_dollars_qtd, 0))
    / NULLIF(SUM(asq.actual_effort_in_days), 0)
    COMMENT 'Product DBUs generated per effort day' AS `Product DBUs per Effort Day`,

  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.stage IN ('U5', 'U6')) * 1.0
    / NULLIF(SUM(asq.actual_effort_in_days), 0)
    COMMENT 'Production UCOs per effort day' AS `Production UCOs per Effort Day`

FROM main.gtm_silver.approval_request_detail asq
LEFT JOIN main.gtm_silver.individual_hierarchy_salesforce hier
  ON asq.owner_user_id = hier.user_id
LEFT JOIN main.gtm_gold.account_obt ao
  ON asq.account_id = ao.account_id
  AND ao.fiscal_year_quarter = (
    -- Use current fiscal quarter (Databricks FY ends Jan 31)
    SELECT CONCAT("FY'",
      CASE WHEN MONTH(CURRENT_DATE()) = 1 THEN YEAR(CURRENT_DATE()) ELSE YEAR(CURRENT_DATE()) + 1 END % 100,
      ' Q',
      CASE
        WHEN MONTH(CURRENT_DATE()) IN (2,3,4) THEN 1
        WHEN MONTH(CURRENT_DATE()) IN (5,6,7) THEN 2
        WHEN MONTH(CURRENT_DATE()) IN (8,9,10) THEN 3
        ELSE 4
      END)
  )
LEFT JOIN main.gtm_silver.use_case_detail uco
  ON asq.account_id = uco.account_id
  AND uco.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.use_case_detail)
  AND uco.is_active_ind = true
WHERE asq.snapshot_date = (
  SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail
);

-- ============================================================================
-- Sample Queries
-- ============================================================================
--
-- Product Impact by SSA:
-- SELECT `Owner`, MEASURE(`Total ASQs`), MEASURE(`Production UCOs`),
--        MEASURE(`Lakeflow Influenced Accounts`), MEASURE(`Serverless Influenced Accounts`),
--        MEASURE(`Total Product DBUs`)
-- FROM mv_product_impact
-- WHERE `Manager L1` = 'Christopher Chalcraft'
-- GROUP BY ALL;
--
-- Product Adoption Rates by Team:
-- SELECT `Manager L1`, MEASURE(`Engaged Accounts`),
--        MEASURE(`Lakeflow Adoption Rate`), MEASURE(`Serverless Adoption Rate`),
--        MEASURE(`Model Serving Adoption Rate`)
-- FROM mv_product_impact
-- WHERE `Business Unit` = 'AMER Enterprise & Emerging'
-- GROUP BY ALL;
