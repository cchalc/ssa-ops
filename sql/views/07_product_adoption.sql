-- cjc_asq_product_adoption
-- Product adoption metrics linked to ASQ accounts
-- Tracks AI/ML, Lakeflow, SQL Warehouse, Unity Catalog adoption
-- ============================================================================

CREATE OR REPLACE VIEW home_christopher_chalcraft.cjc_views.cjc_asq_product_adoption AS
WITH asq_accounts AS (
  -- Get unique accounts with ASQs from our team
  SELECT DISTINCT
    a.AccountId__c AS Account_Id,
    a.Account_Name__c AS Account_Name,
    MAX(a.CreatedDate) AS Latest_ASQ_Date,
    COUNT(*) AS Total_ASQs
  FROM stitch.salesforce.approvalrequest__c a
  JOIN stitch.salesforce.user u ON a.OwnerId = u.Id
  WHERE u.ManagerId = '0053f000000pKoTAAU'
  GROUP BY a.AccountId__c, a.Account_Name__c
),
product_adoption AS (
  -- Join with product adoption data
  SELECT
    aa.Account_Id,
    aa.Account_Name,
    aa.Latest_ASQ_Date,
    aa.Total_ASQs,

    -- AI/ML Adoption flags
    pa.has_model_serving,
    pa.has_feature_store,
    pa.has_mlflow,
    pa.has_vector_search,

    -- Lakeflow/DLT
    pa.has_dlt,
    pa.has_delta_live_tables,

    -- SQL Warehouse
    pa.has_sql_warehouse,
    pa.has_serverless_sql,

    -- Unity Catalog
    pa.has_unity_catalog,

    -- Consumption metrics (if available)
    pa.total_dbu_consumption,
    pa.model_serving_dbu,
    pa.sql_warehouse_dbu

  FROM asq_accounts aa
  LEFT JOIN main.gtm_gold.account_product_adoption pa
    ON aa.Account_Id = pa.account_id
),
adoption_scored AS (
  SELECT
    *,
    -- AI/ML Adoption Score (0-4)
    (COALESCE(has_model_serving, 0) +
     COALESCE(has_feature_store, 0) +
     COALESCE(has_mlflow, 0) +
     COALESCE(has_vector_search, 0)) AS AI_ML_Score,

    -- Modern Data Platform Score (0-3)
    (COALESCE(has_dlt, 0) +
     COALESCE(has_serverless_sql, 0) +
     COALESCE(has_unity_catalog, 0)) AS Modern_Platform_Score

  FROM product_adoption
)
SELECT
  *,
  -- Adoption tier based on scores
  CASE
    WHEN AI_ML_Score >= 3 AND Modern_Platform_Score >= 2 THEN 'Advanced'
    WHEN AI_ML_Score >= 2 OR Modern_Platform_Score >= 2 THEN 'Growing'
    WHEN AI_ML_Score >= 1 OR Modern_Platform_Score >= 1 THEN 'Early'
    ELSE 'Basic'
  END AS Adoption_Tier,

  -- SSA influence flag (account has ASQ AND uses advanced features)
  CASE
    WHEN Total_ASQs > 0 AND (AI_ML_Score >= 2 OR Modern_Platform_Score >= 2) THEN 'SSA Influenced'
    WHEN Total_ASQs > 0 THEN 'SSA Engaged'
    ELSE 'No SSA'
  END AS SSA_Influence_Status

FROM adoption_scored
ORDER BY Total_ASQs DESC, AI_ML_Score DESC;


-- Team-level adoption summary
CREATE OR REPLACE VIEW home_christopher_chalcraft.cjc_views.cjc_product_adoption_summary AS
SELECT
  COUNT(DISTINCT Account_Id) AS Unique_Accounts_Supported,

  -- AI/ML Adoption
  SUM(CASE WHEN has_model_serving = 1 THEN 1 ELSE 0 END) AS Accounts_With_Model_Serving,
  SUM(CASE WHEN has_feature_store = 1 THEN 1 ELSE 0 END) AS Accounts_With_Feature_Store,
  SUM(CASE WHEN has_mlflow = 1 THEN 1 ELSE 0 END) AS Accounts_With_MLflow,
  SUM(CASE WHEN has_vector_search = 1 THEN 1 ELSE 0 END) AS Accounts_With_Vector_Search,

  -- Lakeflow
  SUM(CASE WHEN has_dlt = 1 THEN 1 ELSE 0 END) AS Accounts_With_DLT,

  -- SQL/Compute
  SUM(CASE WHEN has_serverless_sql = 1 THEN 1 ELSE 0 END) AS Accounts_With_Serverless_SQL,
  SUM(CASE WHEN has_sql_warehouse = 1 THEN 1 ELSE 0 END) AS Accounts_With_SQL_Warehouse,

  -- Unity Catalog
  SUM(CASE WHEN has_unity_catalog = 1 THEN 1 ELSE 0 END) AS Accounts_With_Unity_Catalog,

  -- Adoption tiers
  SUM(CASE WHEN Adoption_Tier = 'Advanced' THEN 1 ELSE 0 END) AS Tier_Advanced,
  SUM(CASE WHEN Adoption_Tier = 'Growing' THEN 1 ELSE 0 END) AS Tier_Growing,
  SUM(CASE WHEN Adoption_Tier = 'Early' THEN 1 ELSE 0 END) AS Tier_Early,
  SUM(CASE WHEN Adoption_Tier = 'Basic' THEN 1 ELSE 0 END) AS Tier_Basic,

  -- Consumption totals
  SUM(COALESCE(total_dbu_consumption, 0)) AS Total_DBU_Consumption,
  SUM(COALESCE(model_serving_dbu, 0)) AS Total_Model_Serving_DBU,
  SUM(COALESCE(sql_warehouse_dbu, 0)) AS Total_SQL_Warehouse_DBU,

  CURRENT_TIMESTAMP() AS Snapshot_Time

FROM home_christopher_chalcraft.cjc_views.cjc_asq_product_adoption;
