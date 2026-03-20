-- ============================================================================
-- Export Raw Salesforce Tables to Delta
-- ============================================================================
-- Exports the raw Salesforce tables needed for the dimensional model
-- Run on: logfood workspace with cross-workspace write access
-- Target: cjc_aws_workspace_catalog.ssa_ops_dev (fevm-cjc)
--
-- This job syncs the raw tables that dim/fact tables will transform
-- ============================================================================

-- Create target schema if not exists
CREATE SCHEMA IF NOT EXISTS cjc_aws_workspace_catalog.ssa_ops_dev;

-- ============================================================================
-- SYNC: Salesforce User (for dim_ssa)
-- ============================================================================
CREATE OR REPLACE TABLE cjc_aws_workspace_catalog.ssa_ops_dev.sf_user AS
SELECT
  Id,
  Name,
  Email,
  Alias,
  Title,
  ManagerId,
  Department,
  Division,
  UserType,
  IsActive,
  CreatedDate,
  LastModifiedDate,
  current_timestamp() AS synced_at
FROM stitch.salesforce.user
WHERE UserType = 'Standard';

-- ============================================================================
-- SYNC: Salesforce Account (for dim_account)
-- ============================================================================
CREATE OR REPLACE TABLE cjc_aws_workspace_catalog.ssa_ops_dev.sf_account AS
SELECT
  Id,
  Name,
  AccountNumber,
  OwnerId,
  Type,
  Industry,
  BillingCity,
  BillingState,
  BillingCountry,
  AnnualRevenue,
  NumberOfEmployees,
  IsDeleted,
  CreatedDate,
  LastModifiedDate,
  -- Custom fields - adjust based on your Salesforce schema
  -- Region__c,
  -- Segment__c,
  -- Sub_Industry__c,
  -- ARR__c,
  -- DBU_Consumption_Monthly__c,
  -- Has_Unity_Catalog__c,
  -- etc.
  current_timestamp() AS synced_at
FROM stitch.salesforce.account
WHERE IsDeleted = FALSE;

-- ============================================================================
-- SYNC: Salesforce ApprovalRequest (for fact_asq)
-- ============================================================================
CREATE OR REPLACE TABLE cjc_aws_workspace_catalog.ssa_ops_dev.sf_approval_request AS
SELECT
  a.Id,
  a.Name,
  a.Request_Name__c,
  a.Status__c,
  a.Account_Name__c,
  a.Account__c,
  a.OwnerId,
  a.Specialization__c,
  a.Support_Type__c,
  a.Priority__c,
  a.CreatedDate,
  a.AssignmentDate__c,
  a.End_Date__c,
  a.LastModifiedDate,
  a.Estimated_Effort_Days__c,
  a.Actual_Effort_Days__c,
  a.Request_Status_Notes__c,
  a.IsDeleted,
  -- Add these if they exist in your schema:
  -- a.Complexity__c,
  -- a.Request_Source__c,
  -- a.Linked_UCO__c,
  -- a.Linked_Opportunity__c,
  -- a.Linked_Case__c,
  current_timestamp() AS synced_at
FROM stitch.salesforce.approvalrequest__c a
WHERE a.IsDeleted = FALSE;

-- ============================================================================
-- SYNC: UCO Data (for fact_uco) - if available
-- ============================================================================
-- Uncomment and adjust based on your UCO table location
-- This might be in stitch.salesforce or main.gtm_gold

-- CREATE OR REPLACE TABLE cjc_aws_workspace_catalog.ssa_ops_dev.sf_uco AS
-- SELECT
--   Id,
--   Name,
--   UCO_Number__c,
--   Account__c,
--   OwnerId,
--   Opportunity__c,
--   Status__c,
--   Stage__c,
--   Estimated_DBUs__c,
--   Estimated_ARR__c,
--   Probability__c,
--   CreatedDate,
--   Close_Date__c,
--   Expected_Close_Date__c,
--   Use_Case_Type__c,
--   Product_Category__c,
--   Workload_Type__c,
--   IsDeleted,
--   current_timestamp() AS synced_at
-- FROM stitch.salesforce.uco__c  -- or main.gtm_gold.uco
-- WHERE IsDeleted = FALSE;

-- ============================================================================
-- Verification
-- ============================================================================
SELECT 'sf_user' AS table_name, COUNT(*) AS row_count FROM cjc_aws_workspace_catalog.ssa_ops_dev.sf_user
UNION ALL SELECT 'sf_account', COUNT(*) FROM cjc_aws_workspace_catalog.ssa_ops_dev.sf_account
UNION ALL SELECT 'sf_approval_request', COUNT(*) FROM cjc_aws_workspace_catalog.ssa_ops_dev.sf_approval_request;
