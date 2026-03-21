-- ============================================================================
-- dim_account - Account Dimension with Segments
-- ============================================================================
-- Contains customer accounts with segment, vertical, ARR, and adoption data
-- Run on: fevm-cjc workspace (reads from synced logfood data)
-- ============================================================================

CREATE OR REPLACE TABLE ${catalog}.${schema}.dim_account (
  -- Primary key
  account_id STRING NOT NULL,

  -- Identity
  account_name STRING,
  account_number STRING,

  -- Geographic
  region STRING,
  country STRING,
  state_province STRING,
  city STRING,

  -- Segment
  segment STRING,
  vertical STRING,
  sub_vertical STRING,
  industry STRING,

  -- Account team (AE/RAE)
  ae_id STRING,
  ae_name STRING,
  rae_id STRING,
  rae_name STRING,
  csm_id STRING,
  csm_name STRING,

  -- Product adoption flags
  has_unity_catalog BOOLEAN,
  has_serverless_sql BOOLEAN,
  has_serverless_compute BOOLEAN,
  has_model_serving BOOLEAN,
  has_feature_store BOOLEAN,
  has_mlflow BOOLEAN,
  has_vector_search BOOLEAN,
  has_mosaic_ai BOOLEAN,
  has_dlt BOOLEAN,
  has_workflows BOOLEAN,
  has_delta_sharing BOOLEAN,

  -- Account Segmentation (for Focus & Discipline metrics)
  account_tier STRING COMMENT 'Account tier: A+, A, B, C, Focus Account',
  is_strategic_account BOOLEAN COMMENT 'Strategic account flag (is_strategic_account_ind)',
  is_focus_account BOOLEAN COMMENT 'Focus account flag (is_focus_account_ind)',
  is_priority_account BOOLEAN COMMENT 'Priority account: A+/A tier, Focus Account, or Strategic',
  bu_top_accounts STRING COMMENT 'BU top accounts designation',

  -- Adoption scores
  ai_ml_score DECIMAL(5,2),
  modern_platform_score DECIMAL(5,2),
  data_engineering_score DECIMAL(5,2),
  adoption_tier STRING,
  maturity_stage STRING,

  -- Financials
  arr DECIMAL(15,2),
  arr_band STRING,
  dbu_consumption_monthly DECIMAL(15,2),
  dbu_consumption_band STRING,
  contract_end_date DATE,

  -- Account status
  is_active BOOLEAN,
  customer_since DATE,
  account_type STRING,

  -- Metadata
  synced_at TIMESTAMP,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP
)
USING DELTA
COMMENT 'Account dimension with segments, verticals, and product adoption'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- ============================================================================
-- Populate dim_account from Salesforce Account data
-- ============================================================================

WITH account_data AS (
  SELECT
    a.Id AS account_id,
    a.Name AS account_name,
    a.AccountNumber AS account_number,

    -- Geographic - map from Salesforce fields
    COALESCE(a.Region__c, a.BillingCountry,
      CASE
        WHEN a.BillingCountry IN ('United States', 'US', 'USA', 'Canada', 'CA') THEN 'Americas'
        WHEN a.BillingCountry IN ('United Kingdom', 'UK', 'Germany', 'France', 'Netherlands') THEN 'EMEA'
        WHEN a.BillingCountry IN ('Japan', 'Australia', 'Singapore', 'India') THEN 'APJ'
        ELSE 'Americas'
      END
    ) AS region,
    a.BillingCountry AS country,
    a.BillingState AS state_province,
    a.BillingCity AS city,

    -- Segment (ENT/COMM/MM/SMB)
    COALESCE(a.Segment__c,
      CASE
        WHEN a.AnnualRevenue >= 1000000000 THEN 'ENT'
        WHEN a.AnnualRevenue >= 100000000 THEN 'COMM'
        WHEN a.AnnualRevenue >= 10000000 THEN 'MM'
        ELSE 'SMB'
      END
    ) AS segment,

    -- Vertical / Industry
    a.Industry AS vertical,
    a.Sub_Industry__c AS sub_vertical,
    a.Industry AS industry,

    -- Account team
    a.OwnerId AS ae_id,
    ae.Name AS ae_name,
    a.Regional_AE__c AS rae_id,
    rae.Name AS rae_name,
    a.CSM__c AS csm_id,
    csm.Name AS csm_name,

    -- Product adoption flags
    -- TODO: Map these from actual adoption fields in your data
    COALESCE(a.Has_Unity_Catalog__c, FALSE) AS has_unity_catalog,
    COALESCE(a.Has_Serverless_SQL__c, FALSE) AS has_serverless_sql,
    COALESCE(a.Has_Serverless_Compute__c, FALSE) AS has_serverless_compute,
    COALESCE(a.Has_Model_Serving__c, FALSE) AS has_model_serving,
    COALESCE(a.Has_Feature_Store__c, FALSE) AS has_feature_store,
    COALESCE(a.Has_MLflow__c, FALSE) AS has_mlflow,
    COALESCE(a.Has_Vector_Search__c, FALSE) AS has_vector_search,
    COALESCE(a.Has_Mosaic_AI__c, FALSE) AS has_mosaic_ai,
    COALESCE(a.Has_DLT__c, FALSE) AS has_dlt,
    COALESCE(a.Has_Workflows__c, FALSE) AS has_workflows,
    COALESCE(a.Has_Delta_Sharing__c, FALSE) AS has_delta_sharing,

    -- Account Segmentation (from GTM Gold rpt_account_dim)
    acct.account_tier AS account_tier,
    COALESCE(acct.is_strategic_account_ind, FALSE) AS is_strategic_account,
    COALESCE(acct.is_focus_account_ind, FALSE) AS is_focus_account,
    CASE
      WHEN acct.account_tier IN ('A+', 'A')
        OR acct.account_tier LIKE 'Focus Account%'
        OR acct.is_strategic_account_ind = TRUE
      THEN TRUE ELSE FALSE
    END AS is_priority_account,
    acct.bu_top_accounts AS bu_top_accounts,

    -- Scores (calculate from flags or use existing fields)
    CAST(NULL AS DECIMAL(5,2)) AS ai_ml_score,
    CAST(NULL AS DECIMAL(5,2)) AS modern_platform_score,
    CAST(NULL AS DECIMAL(5,2)) AS data_engineering_score,

    -- Adoption tier
    COALESCE(a.Adoption_Tier__c, 'Basic') AS adoption_tier,
    COALESCE(a.Maturity_Stage__c, 'Early') AS maturity_stage,

    -- Financials
    COALESCE(a.ARR__c, 0) AS arr,
    CASE
      WHEN a.ARR__c >= 1000000 THEN '$1M+'
      WHEN a.ARR__c >= 500000 THEN '$500K-1M'
      WHEN a.ARR__c >= 100000 THEN '$100K-500K'
      WHEN a.ARR__c >= 50000 THEN '$50K-100K'
      ELSE '<$50K'
    END AS arr_band,
    COALESCE(a.DBU_Consumption_Monthly__c, 0) AS dbu_consumption_monthly,
    CASE
      WHEN a.DBU_Consumption_Monthly__c >= 100000 THEN '100K+ DBU'
      WHEN a.DBU_Consumption_Monthly__c >= 10000 THEN '10K-100K DBU'
      WHEN a.DBU_Consumption_Monthly__c >= 1000 THEN '1K-10K DBU'
      ELSE '<1K DBU'
    END AS dbu_consumption_band,
    a.Contract_End_Date__c AS contract_end_date,

    -- Status
    a.IsDeleted = FALSE AS is_active,
    DATE(a.CreatedDate) AS customer_since,
    a.Type AS account_type,

    CURRENT_TIMESTAMP() AS synced_at

  FROM ${source_catalog}.${source_schema}.sf_account a
  LEFT JOIN ${source_catalog}.${source_schema}.sf_user ae ON a.OwnerId = ae.Id
  LEFT JOIN ${source_catalog}.${source_schema}.sf_user rae ON a.Regional_AE__c = rae.Id
  LEFT JOIN ${source_catalog}.${source_schema}.sf_user csm ON a.CSM__c = csm.Id
  -- Join to GTM Gold for account segmentation (A+/A/B/C tiers, Focus Account, Strategic)
  LEFT JOIN main.gtm_gold.rpt_account_dim acct ON a.Id = acct.account_id
)

-- Merge into dim_account
MERGE INTO ${catalog}.${schema}.dim_account AS target
USING account_data AS source
ON target.account_id = source.account_id
WHEN MATCHED THEN UPDATE SET
  target.account_name = source.account_name,
  target.account_number = source.account_number,
  target.region = source.region,
  target.country = source.country,
  target.state_province = source.state_province,
  target.city = source.city,
  target.segment = source.segment,
  target.vertical = source.vertical,
  target.sub_vertical = source.sub_vertical,
  target.industry = source.industry,
  target.ae_id = source.ae_id,
  target.ae_name = source.ae_name,
  target.rae_id = source.rae_id,
  target.rae_name = source.rae_name,
  target.csm_id = source.csm_id,
  target.csm_name = source.csm_name,
  target.has_unity_catalog = source.has_unity_catalog,
  target.has_serverless_sql = source.has_serverless_sql,
  target.has_serverless_compute = source.has_serverless_compute,
  target.has_model_serving = source.has_model_serving,
  target.has_feature_store = source.has_feature_store,
  target.has_mlflow = source.has_mlflow,
  target.has_vector_search = source.has_vector_search,
  target.has_mosaic_ai = source.has_mosaic_ai,
  target.has_dlt = source.has_dlt,
  target.has_workflows = source.has_workflows,
  target.has_delta_sharing = source.has_delta_sharing,
  target.account_tier = source.account_tier,
  target.is_strategic_account = source.is_strategic_account,
  target.is_focus_account = source.is_focus_account,
  target.is_priority_account = source.is_priority_account,
  target.bu_top_accounts = source.bu_top_accounts,
  target.adoption_tier = source.adoption_tier,
  target.maturity_stage = source.maturity_stage,
  target.arr = source.arr,
  target.arr_band = source.arr_band,
  target.dbu_consumption_monthly = source.dbu_consumption_monthly,
  target.dbu_consumption_band = source.dbu_consumption_band,
  target.contract_end_date = source.contract_end_date,
  target.is_active = source.is_active,
  target.synced_at = source.synced_at,
  target.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
  account_id, account_name, account_number,
  region, country, state_province, city,
  segment, vertical, sub_vertical, industry,
  ae_id, ae_name, rae_id, rae_name, csm_id, csm_name,
  has_unity_catalog, has_serverless_sql, has_serverless_compute,
  has_model_serving, has_feature_store, has_mlflow, has_vector_search,
  has_mosaic_ai, has_dlt, has_workflows, has_delta_sharing,
  account_tier, is_strategic_account, is_focus_account, is_priority_account, bu_top_accounts,
  ai_ml_score, modern_platform_score, data_engineering_score,
  adoption_tier, maturity_stage,
  arr, arr_band, dbu_consumption_monthly, dbu_consumption_band,
  contract_end_date, is_active, customer_since, account_type,
  synced_at, created_at, updated_at
) VALUES (
  source.account_id, source.account_name, source.account_number,
  source.region, source.country, source.state_province, source.city,
  source.segment, source.vertical, source.sub_vertical, source.industry,
  source.ae_id, source.ae_name, source.rae_id, source.rae_name, source.csm_id, source.csm_name,
  source.has_unity_catalog, source.has_serverless_sql, source.has_serverless_compute,
  source.has_model_serving, source.has_feature_store, source.has_mlflow, source.has_vector_search,
  source.has_mosaic_ai, source.has_dlt, source.has_workflows, source.has_delta_sharing,
  source.account_tier, source.is_strategic_account, source.is_focus_account, source.is_priority_account, source.bu_top_accounts,
  source.ai_ml_score, source.modern_platform_score, source.data_engineering_score,
  source.adoption_tier, source.maturity_stage,
  source.arr, source.arr_band, source.dbu_consumption_monthly, source.dbu_consumption_band,
  source.contract_end_date, source.is_active, source.customer_since, source.account_type,
  source.synced_at, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- ============================================================================
-- Verification
-- ============================================================================
-- SELECT segment, COUNT(*) FROM ${catalog}.${schema}.dim_account WHERE is_active GROUP BY segment;
-- SELECT vertical, COUNT(*) FROM ${catalog}.${schema}.dim_account WHERE is_active GROUP BY vertical ORDER BY 2 DESC LIMIT 20;
-- SELECT arr_band, COUNT(*) FROM ${catalog}.${schema}.dim_account GROUP BY arr_band;
