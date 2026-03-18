-- SSA Activity Dashboard - SQL Views Deployment
-- ============================================================================
-- Target: home_christopher_chalcraft.cjc_views
-- Warehouse: central-logfood-prodtools-azure-westus
--
-- Run this file to deploy all dashboard views.
-- Views build on existing cjc_* views from team_asq_analysis.sql
-- ============================================================================

-- Ensure schema exists
CREATE SCHEMA IF NOT EXISTS home_christopher_chalcraft.cjc_views;

-- ============================================================================
-- VIEW 1: cjc_asq_completed_metrics
-- Closed ASQ analysis with turnaround time and completion metrics
-- ============================================================================

CREATE OR REPLACE VIEW home_christopher_chalcraft.cjc_views.cjc_asq_completed_metrics AS
WITH completed_asqs AS (
  SELECT
    a.Id,
    a.Name AS ASQ_Number,
    a.Request_Name__c AS ASQ_Title,
    a.Status__c,
    a.Account_Name__c,
    a.Specialization__c,
    a.Support_Type__c,
    a.CreatedDate,
    a.AssignmentDate__c,
    a.End_Date__c AS Due_Date,
    a.LastModifiedDate AS Completion_Date,
    a.Actual_Effort_Days__c,
    a.Estimated_Effort_Days__c,
    a.Request_Status_Notes__c,
    u.Name AS Owner_Name,
    u.Email AS Owner_Email,
    DATEDIFF(a.LastModifiedDate, a.CreatedDate) AS Days_Total,
    DATEDIFF(a.LastModifiedDate, a.AssignmentDate__c) AS Days_In_Progress,
    DATEDIFF(a.AssignmentDate__c, a.CreatedDate) AS Days_To_Assign,
    CONCAT(YEAR(a.LastModifiedDate), '-Q', QUARTER(a.LastModifiedDate)) AS Completion_Quarter,
    YEAR(a.LastModifiedDate) AS Completion_Year,
    MONTH(a.LastModifiedDate) AS Completion_Month,
    WEEKOFYEAR(a.LastModifiedDate) AS Completion_Week
  FROM stitch.salesforce.approvalrequest__c a
  JOIN stitch.salesforce.user u ON a.OwnerId = u.Id
  WHERE u.ManagerId = '0053f000000pKoTAAU'
    AND a.Status__c IN ('Completed', 'Closed')
)
SELECT
  *,
  CASE
    WHEN Due_Date IS NOT NULL AND Completion_Date <= Due_Date THEN 1
    ELSE 0
  END AS Delivered_On_Time,
  CASE
    WHEN Request_Status_Notes__c IS NOT NULL
      AND LENGTH(Request_Status_Notes__c) > 50
      AND Actual_Effort_Days__c IS NOT NULL
    THEN 1
    ELSE 0
  END AS Quality_Closure
FROM completed_asqs
ORDER BY Completion_Date DESC;


-- ============================================================================
-- VIEW 2: cjc_asq_sla_metrics
-- SLA tracking per ASQ - measures time to key milestones
-- ============================================================================

CREATE OR REPLACE VIEW home_christopher_chalcraft.cjc_views.cjc_asq_sla_metrics AS
WITH asq_history AS (
  SELECT
    a.Id,
    a.Name AS ASQ_Number,
    a.Request_Name__c AS ASQ_Title,
    a.Status__c,
    a.Account_Name__c,
    a.CreatedDate,
    a.AssignmentDate__c,
    a.End_Date__c,
    a.Request_Status_Notes__c,
    u.Name AS Owner_Name,
    CASE
      WHEN a.AssignmentDate__c IS NOT NULL THEN
        DATEDIFF(a.AssignmentDate__c, a.CreatedDate)
      ELSE NULL
    END AS Days_To_Review,
    CASE
      WHEN a.Status__c IN ('In Progress', 'On Hold', 'Completed', 'Closed') THEN
        DATEDIFF(a.AssignmentDate__c, a.CreatedDate)
      ELSE NULL
    END AS Days_To_Assignment,
    CASE
      WHEN a.Request_Status_Notes__c IS NOT NULL
        AND LENGTH(a.Request_Status_Notes__c) > 10
        AND a.AssignmentDate__c IS NOT NULL THEN
        LEAST(DATEDIFF(CURRENT_DATE(), a.AssignmentDate__c), 7)
      ELSE NULL
    END AS Days_To_First_Response
  FROM stitch.salesforce.approvalrequest__c a
  JOIN stitch.salesforce.user u ON a.OwnerId = u.Id
  WHERE u.ManagerId = '0053f000000pKoTAAU'
)
SELECT
  *,
  CASE WHEN Days_To_Review IS NOT NULL AND Days_To_Review <= 2 THEN 1 ELSE 0 END AS Review_SLA_Met,
  CASE WHEN Days_To_Assignment IS NOT NULL AND Days_To_Assignment <= 5 THEN 1 ELSE 0 END AS Assignment_SLA_Met,
  CASE WHEN Days_To_First_Response IS NOT NULL AND Days_To_First_Response <= 5 THEN 1 ELSE 0 END AS Response_SLA_Met,
  CASE
    WHEN Days_To_Review IS NULL THEN 'Pending Review'
    WHEN Days_To_Assignment IS NULL THEN 'Pending Assignment'
    WHEN Days_To_First_Response IS NULL THEN 'Pending Response'
    ELSE 'Active'
  END AS SLA_Stage,
  CONCAT(YEAR(CreatedDate), '-W', LPAD(WEEKOFYEAR(CreatedDate), 2, '0')) AS Created_Week,
  CONCAT(YEAR(CreatedDate), '-', LPAD(MONTH(CreatedDate), 2, '0')) AS Created_Month
FROM asq_history
ORDER BY CreatedDate DESC;


-- ============================================================================
-- VIEW 3: cjc_team_summary
-- Team-level aggregations for executive dashboard
-- ============================================================================

CREATE OR REPLACE VIEW home_christopher_chalcraft.cjc_views.cjc_team_summary AS
WITH open_asqs AS (
  SELECT
    u.Name AS Owner_Name,
    a.Id,
    a.Status__c,
    a.End_Date__c,
    a.Request_Status_Notes__c,
    a.Specialization__c,
    a.Support_Type__c,
    CASE
      WHEN a.Specialization__c LIKE '%ML%' OR a.Specialization__c LIKE '%AI%' THEN 10
      WHEN a.Specialization__c LIKE '%Delta%' THEN 5
      WHEN a.Specialization__c LIKE '%SQL%' THEN 3
      WHEN a.Support_Type__c = 'Deep Dive' THEN 8
      WHEN a.Support_Type__c = 'Technical Review' THEN 3
      ELSE 5
    END AS Estimated_Days
  FROM stitch.salesforce.approvalrequest__c a
  JOIN stitch.salesforce.user u ON a.OwnerId = u.Id
  WHERE u.ManagerId = '0053f000000pKoTAAU'
    AND a.Status__c IN ('In Progress', 'Under Review', 'On Hold', 'Submitted')
),
completed_qtd AS (
  SELECT COUNT(*) AS completed_count,
         AVG(DATEDIFF(a.LastModifiedDate, a.AssignmentDate__c)) AS avg_turnaround
  FROM stitch.salesforce.approvalrequest__c a
  JOIN stitch.salesforce.user u ON a.OwnerId = u.Id
  WHERE u.ManagerId = '0053f000000pKoTAAU'
    AND a.Status__c IN ('Completed', 'Closed')
    AND QUARTER(a.LastModifiedDate) = QUARTER(CURRENT_DATE())
    AND YEAR(a.LastModifiedDate) = YEAR(CURRENT_DATE())
),
team_capacity AS (
  SELECT
    Owner_Name,
    COUNT(*) AS open_count,
    SUM(Estimated_Days) AS total_days,
    SUM(CASE WHEN End_Date__c < CURRENT_DATE() THEN 1 ELSE 0 END) AS overdue_count,
    SUM(CASE WHEN Request_Status_Notes__c IS NULL OR LENGTH(Request_Status_Notes__c) < 10 THEN 1 ELSE 0 END) AS missing_notes,
    CASE
      WHEN SUM(Estimated_Days) <= 5 THEN 'GREEN'
      WHEN SUM(Estimated_Days) <= 10 THEN 'YELLOW'
      ELSE 'RED'
    END AS capacity_status
  FROM open_asqs
  GROUP BY Owner_Name
)
SELECT
  (SELECT COUNT(*) FROM open_asqs) AS Total_Open_ASQs,
  (SELECT SUM(CASE WHEN End_Date__c < CURRENT_DATE() THEN 1 ELSE 0 END) FROM open_asqs) AS Overdue_ASQs,
  (SELECT SUM(CASE WHEN Request_Status_Notes__c IS NULL OR LENGTH(Request_Status_Notes__c) < 10 THEN 1 ELSE 0 END) FROM open_asqs) AS Missing_Notes_ASQs,
  (SELECT completed_count FROM completed_qtd) AS Completed_QTD,
  (SELECT ROUND(avg_turnaround, 1) FROM completed_qtd) AS Avg_Turnaround_Days,
  (SELECT COUNT(*) FROM team_capacity WHERE capacity_status = 'GREEN') AS Team_Members_Green,
  (SELECT COUNT(*) FROM team_capacity WHERE capacity_status = 'YELLOW') AS Team_Members_Yellow,
  (SELECT COUNT(*) FROM team_capacity WHERE capacity_status = 'RED') AS Team_Members_Red,
  CASE
    WHEN (SELECT COUNT(*) FROM team_capacity WHERE capacity_status = 'RED') > 2 THEN 'RED'
    WHEN (SELECT COUNT(*) FROM team_capacity WHERE capacity_status IN ('RED', 'YELLOW')) > 3 THEN 'YELLOW'
    ELSE 'GREEN'
  END AS Team_Capacity_Status,
  (SELECT COUNT(DISTINCT Specialization__c) FROM open_asqs) AS Unique_Specializations,
  (SELECT COUNT(DISTINCT Support_Type__c) FROM open_asqs) AS Unique_Support_Types,
  CURRENT_TIMESTAMP() AS Snapshot_Time;


-- ============================================================================
-- VIEW 4: cjc_asq_effort_accuracy
-- Estimate vs actual effort comparison for calibration
-- ============================================================================

CREATE OR REPLACE VIEW home_christopher_chalcraft.cjc_views.cjc_asq_effort_accuracy AS
WITH effort_data AS (
  SELECT
    a.Id,
    a.Name AS ASQ_Number,
    a.Request_Name__c AS ASQ_Title,
    a.Status__c,
    a.Account_Name__c,
    a.Specialization__c,
    a.Support_Type__c,
    u.Name AS Owner_Name,
    COALESCE(
      a.Estimated_Effort_Days__c,
      CASE
        WHEN a.Specialization__c LIKE '%ML%' OR a.Specialization__c LIKE '%AI%' THEN 10
        WHEN a.Specialization__c LIKE '%Delta%' THEN 5
        WHEN a.Specialization__c LIKE '%SQL%' THEN 3
        WHEN a.Support_Type__c = 'Deep Dive' THEN 8
        WHEN a.Support_Type__c = 'Technical Review' THEN 3
        ELSE 5
      END
    ) AS Estimated_Days,
    a.Actual_Effort_Days__c AS Actual_Days,
    DATEDIFF(a.LastModifiedDate, a.AssignmentDate__c) AS Days_In_Progress,
    a.LastModifiedDate AS Completion_Date,
    CONCAT(YEAR(a.LastModifiedDate), '-Q', QUARTER(a.LastModifiedDate)) AS Completion_Quarter
  FROM stitch.salesforce.approvalrequest__c a
  JOIN stitch.salesforce.user u ON a.OwnerId = u.Id
  WHERE u.ManagerId = '0053f000000pKoTAAU'
    AND a.Status__c IN ('Completed', 'Closed')
)
SELECT
  *,
  COALESCE(Actual_Days, Days_In_Progress) AS Effective_Actual_Days,
  CASE
    WHEN Estimated_Days > 0 THEN
      ROUND(COALESCE(Actual_Days, Days_In_Progress) / Estimated_Days, 2)
    ELSE NULL
  END AS Effort_Ratio,
  CASE
    WHEN Estimated_Days IS NULL OR Estimated_Days = 0 THEN 'No Estimate'
    WHEN COALESCE(Actual_Days, Days_In_Progress) <= Estimated_Days * 0.8 THEN 'Under Estimate'
    WHEN COALESCE(Actual_Days, Days_In_Progress) <= Estimated_Days * 1.2 THEN 'Accurate'
    WHEN COALESCE(Actual_Days, Days_In_Progress) <= Estimated_Days * 1.5 THEN 'Slight Over'
    ELSE 'Significant Over'
  END AS Accuracy_Category,
  COALESCE(Actual_Days, Days_In_Progress) - Estimated_Days AS Variance_Days
FROM effort_data
WHERE Estimated_Days > 0
ORDER BY Completion_Date DESC;


-- ============================================================================
-- VIEW 5: cjc_asq_reengagement
-- Repeat account tracking - identifies accounts with multiple ASQs
-- ============================================================================

CREATE OR REPLACE VIEW home_christopher_chalcraft.cjc_views.cjc_asq_reengagement AS
WITH account_asqs AS (
  SELECT
    a.Account_Name__c AS Account_Name,
    a.AccountId__c AS Account_Id,
    a.Id AS ASQ_Id,
    a.Name AS ASQ_Number,
    a.Request_Name__c AS ASQ_Title,
    a.Status__c,
    a.Specialization__c,
    a.Support_Type__c,
    a.CreatedDate,
    a.AssignmentDate__c,
    u.Name AS Owner_Name,
    YEAR(a.CreatedDate) AS Created_Year,
    QUARTER(a.CreatedDate) AS Created_Quarter,
    CONCAT(YEAR(a.CreatedDate), '-Q', QUARTER(a.CreatedDate)) AS Created_QTR
  FROM stitch.salesforce.approvalrequest__c a
  JOIN stitch.salesforce.user u ON a.OwnerId = u.Id
  WHERE u.ManagerId = '0053f000000pKoTAAU'
),
account_summary AS (
  SELECT
    Account_Name,
    Account_Id,
    COUNT(*) AS Total_ASQs,
    COUNT(DISTINCT Owner_Name) AS Unique_SSAs,
    MIN(CreatedDate) AS First_ASQ_Date,
    MAX(CreatedDate) AS Latest_ASQ_Date,
    DATEDIFF(MAX(CreatedDate), MIN(CreatedDate)) AS Engagement_Span_Days,
    SUM(CASE WHEN Created_Year = YEAR(CURRENT_DATE()) THEN 1 ELSE 0 END) AS ASQs_YTD,
    SUM(CASE
      WHEN Created_Year = YEAR(CURRENT_DATE())
        AND Created_Quarter = QUARTER(CURRENT_DATE())
      THEN 1 ELSE 0
    END) AS ASQs_QTD,
    COLLECT_SET(Specialization__c) AS Specializations_Used,
    COLLECT_SET(Support_Type__c) AS Support_Types_Used,
    SUM(CASE WHEN Status__c IN ('In Progress', 'Under Review', 'On Hold') THEN 1 ELSE 0 END) AS Active_ASQs,
    SUM(CASE WHEN Status__c IN ('Completed', 'Closed') THEN 1 ELSE 0 END) AS Completed_ASQs
  FROM account_asqs
  GROUP BY Account_Name, Account_Id
)
SELECT
  *,
  CASE
    WHEN Total_ASQs >= 5 THEN 'High Engagement'
    WHEN Total_ASQs >= 2 THEN 'Repeat Customer'
    ELSE 'Single Engagement'
  END AS Engagement_Tier,
  CASE WHEN Total_ASQs >= 2 THEN 1 ELSE 0 END AS Is_Repeat_Customer
FROM account_summary
ORDER BY Total_ASQs DESC, Latest_ASQ_Date DESC;


-- ============================================================================
-- VIEW 6: cjc_asq_uco_linkage
-- UCO (Use Case Opportunity) and consumption linkage
-- ============================================================================

CREATE OR REPLACE VIEW home_christopher_chalcraft.cjc_views.cjc_asq_uco_linkage AS
WITH asq_base AS (
  SELECT
    a.Id AS ASQ_Id,
    a.Name AS ASQ_Number,
    a.Request_Name__c AS ASQ_Title,
    a.Status__c AS ASQ_Status,
    a.Account_Name__c,
    a.AccountId__c,
    a.Specialization__c,
    a.Support_Type__c,
    a.CreatedDate AS ASQ_Created,
    a.AssignmentDate__c,
    u.Name AS Owner_Name,
    a.Approved_Use_Case__c AS Linked_UCO_Id
  FROM stitch.salesforce.approvalrequest__c a
  JOIN stitch.salesforce.user u ON a.OwnerId = u.Id
  WHERE u.ManagerId = '0053f000000pKoTAAU'
),
uco_details AS (
  SELECT
    uc.Id AS UCO_Id,
    uc.Name AS UCO_Number,
    uc.Use_Case_Title__c AS UCO_Title,
    uc.Status__c AS UCO_Status,
    uc.Stage__c AS UCO_Stage,
    uc.Estimated_DBUs__c AS Estimated_DBUs,
    uc.Account__c AS UCO_Account_Id,
    uc.CreatedDate AS UCO_Created,
    curated.use_case_type,
    curated.primary_product,
    curated.estimated_arr_usd
  FROM stitch.salesforce.approved_usecase__c uc
  LEFT JOIN main.gtm_gold.core_usecase_curated curated
    ON uc.Id = curated.use_case_id
),
joined AS (
  SELECT
    asq.*,
    uco.UCO_Number,
    uco.UCO_Title,
    uco.UCO_Status,
    uco.UCO_Stage,
    uco.Estimated_DBUs,
    uco.use_case_type,
    uco.primary_product,
    uco.estimated_arr_usd,
    CASE WHEN uco.UCO_Id IS NOT NULL THEN 1 ELSE 0 END AS Has_UCO_Link
  FROM asq_base asq
  LEFT JOIN uco_details uco ON asq.Linked_UCO_Id = uco.UCO_Id
)
SELECT
  *,
  CASE
    WHEN Has_UCO_Link = 1 AND UCO_Status = 'Active' THEN 'Strong Link'
    WHEN Has_UCO_Link = 1 THEN 'Linked'
    ELSE 'No UCO Link'
  END AS Linkage_Status
FROM joined
ORDER BY ASQ_Created DESC;


-- UCO linkage summary by owner
CREATE OR REPLACE VIEW home_christopher_chalcraft.cjc_views.cjc_asq_uco_summary AS
SELECT
  Owner_Name,
  COUNT(*) AS Total_ASQs,
  SUM(Has_UCO_Link) AS ASQs_With_UCO,
  ROUND(100.0 * SUM(Has_UCO_Link) / COUNT(*), 1) AS UCO_Linkage_Rate_Pct,
  SUM(COALESCE(Estimated_DBUs, 0)) AS Total_Linked_DBUs,
  SUM(COALESCE(estimated_arr_usd, 0)) AS Total_Linked_ARR
FROM home_christopher_chalcraft.cjc_views.cjc_asq_uco_linkage
GROUP BY Owner_Name
ORDER BY Total_ASQs DESC;


-- ============================================================================
-- VIEW 7: cjc_asq_product_adoption
-- Product adoption metrics linked to ASQ accounts
-- ============================================================================

CREATE OR REPLACE VIEW home_christopher_chalcraft.cjc_views.cjc_asq_product_adoption AS
WITH asq_accounts AS (
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
  SELECT
    aa.Account_Id,
    aa.Account_Name,
    aa.Latest_ASQ_Date,
    aa.Total_ASQs,
    pa.has_model_serving,
    pa.has_feature_store,
    pa.has_mlflow,
    pa.has_vector_search,
    pa.has_dlt,
    pa.has_delta_live_tables,
    pa.has_sql_warehouse,
    pa.has_serverless_sql,
    pa.has_unity_catalog,
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
    (COALESCE(has_model_serving, 0) +
     COALESCE(has_feature_store, 0) +
     COALESCE(has_mlflow, 0) +
     COALESCE(has_vector_search, 0)) AS AI_ML_Score,
    (COALESCE(has_dlt, 0) +
     COALESCE(has_serverless_sql, 0) +
     COALESCE(has_unity_catalog, 0)) AS Modern_Platform_Score
  FROM product_adoption
)
SELECT
  *,
  CASE
    WHEN AI_ML_Score >= 3 AND Modern_Platform_Score >= 2 THEN 'Advanced'
    WHEN AI_ML_Score >= 2 OR Modern_Platform_Score >= 2 THEN 'Growing'
    WHEN AI_ML_Score >= 1 OR Modern_Platform_Score >= 1 THEN 'Early'
    ELSE 'Basic'
  END AS Adoption_Tier,
  CASE
    WHEN Total_ASQs > 0 AND (AI_ML_Score >= 2 OR Modern_Platform_Score >= 2) THEN 'SSA Influenced'
    WHEN Total_ASQs > 0 THEN 'SSA Engaged'
    ELSE 'No SSA'
  END AS SSA_Influence_Status
FROM adoption_scored
ORDER BY Total_ASQs DESC, AI_ML_Score DESC;


-- Product adoption team summary
CREATE OR REPLACE VIEW home_christopher_chalcraft.cjc_views.cjc_product_adoption_summary AS
SELECT
  COUNT(DISTINCT Account_Id) AS Unique_Accounts_Supported,
  SUM(CASE WHEN has_model_serving = 1 THEN 1 ELSE 0 END) AS Accounts_With_Model_Serving,
  SUM(CASE WHEN has_feature_store = 1 THEN 1 ELSE 0 END) AS Accounts_With_Feature_Store,
  SUM(CASE WHEN has_mlflow = 1 THEN 1 ELSE 0 END) AS Accounts_With_MLflow,
  SUM(CASE WHEN has_vector_search = 1 THEN 1 ELSE 0 END) AS Accounts_With_Vector_Search,
  SUM(CASE WHEN has_dlt = 1 THEN 1 ELSE 0 END) AS Accounts_With_DLT,
  SUM(CASE WHEN has_serverless_sql = 1 THEN 1 ELSE 0 END) AS Accounts_With_Serverless_SQL,
  SUM(CASE WHEN has_sql_warehouse = 1 THEN 1 ELSE 0 END) AS Accounts_With_SQL_Warehouse,
  SUM(CASE WHEN has_unity_catalog = 1 THEN 1 ELSE 0 END) AS Accounts_With_Unity_Catalog,
  SUM(CASE WHEN Adoption_Tier = 'Advanced' THEN 1 ELSE 0 END) AS Tier_Advanced,
  SUM(CASE WHEN Adoption_Tier = 'Growing' THEN 1 ELSE 0 END) AS Tier_Growing,
  SUM(CASE WHEN Adoption_Tier = 'Early' THEN 1 ELSE 0 END) AS Tier_Early,
  SUM(CASE WHEN Adoption_Tier = 'Basic' THEN 1 ELSE 0 END) AS Tier_Basic,
  SUM(COALESCE(total_dbu_consumption, 0)) AS Total_DBU_Consumption,
  SUM(COALESCE(model_serving_dbu, 0)) AS Total_Model_Serving_DBU,
  SUM(COALESCE(sql_warehouse_dbu, 0)) AS Total_SQL_Warehouse_DBU,
  CURRENT_TIMESTAMP() AS Snapshot_Time
FROM home_christopher_chalcraft.cjc_views.cjc_asq_product_adoption;


-- ============================================================================
-- VERIFICATION QUERIES
-- Run these after deployment to verify data
-- ============================================================================

-- SELECT 'cjc_asq_completed_metrics' as view_name, COUNT(*) as row_count FROM home_christopher_chalcraft.cjc_views.cjc_asq_completed_metrics;
-- SELECT 'cjc_asq_sla_metrics' as view_name, COUNT(*) as row_count FROM home_christopher_chalcraft.cjc_views.cjc_asq_sla_metrics;
-- SELECT 'cjc_team_summary' as view_name, * FROM home_christopher_chalcraft.cjc_views.cjc_team_summary;
-- SELECT 'cjc_asq_effort_accuracy' as view_name, COUNT(*) as row_count FROM home_christopher_chalcraft.cjc_views.cjc_asq_effort_accuracy;
-- SELECT 'cjc_asq_reengagement' as view_name, COUNT(*) as row_count FROM home_christopher_chalcraft.cjc_views.cjc_asq_reengagement;
-- SELECT 'cjc_asq_uco_linkage' as view_name, COUNT(*) as row_count FROM home_christopher_chalcraft.cjc_views.cjc_asq_uco_linkage;
-- SELECT 'cjc_asq_product_adoption' as view_name, COUNT(*) as row_count FROM home_christopher_chalcraft.cjc_views.cjc_asq_product_adoption;
