-- Export SSA Dashboard views from logfood to Delta tables on fevm-cjc
-- Run on: logfood workspace with cross-workspace write access
-- Target: cjc_aws_workspace_catalog.ssa_ops_dev (fevm-cjc)
--
-- This job reads from home_christopher_chalcraft.cjc_views
-- and writes to Delta tables that can be accessed by Lakebase

-- Create target schema if not exists
CREATE SCHEMA IF NOT EXISTS cjc_aws_workspace_catalog.ssa_ops_dev;

-- ============================================================================
-- SYNC: Team Summary
-- ============================================================================
CREATE OR REPLACE TABLE cjc_aws_workspace_catalog.ssa_ops_dev.team_summary AS
SELECT
    *,
    current_timestamp() as synced_at
FROM home_christopher_chalcraft.cjc_views.cjc_team_summary;

-- ============================================================================
-- SYNC: Completed Metrics
-- ============================================================================
CREATE OR REPLACE TABLE cjc_aws_workspace_catalog.ssa_ops_dev.asq_completed_metrics AS
SELECT
    Id as asq_id,
    ASQ_Number as asq_number,
    ASQ_Title as asq_title,
    Status__c as status,
    Account_Name__c as account_name,
    Specialization__c as specialization,
    Support_Type__c as support_type,
    Owner_Name as owner_name,
    Owner_Email as owner_email,
    CreatedDate as created_date,
    AssignmentDate__c as assignment_date,
    Due_Date as due_date,
    Completion_Date as completion_date,
    Days_Total as days_total,
    Days_In_Progress as days_in_progress,
    Days_To_Assign as days_to_assign,
    Completion_Quarter as completion_quarter,
    Completion_Year as completion_year,
    Completion_Month as completion_month,
    Completion_Week as completion_week,
    Delivered_On_Time as delivered_on_time,
    Quality_Closure as quality_closure,
    current_timestamp() as synced_at
FROM home_christopher_chalcraft.cjc_views.cjc_asq_completed_metrics;

-- ============================================================================
-- SYNC: SLA Metrics
-- ============================================================================
CREATE OR REPLACE TABLE cjc_aws_workspace_catalog.ssa_ops_dev.asq_sla_metrics AS
SELECT
    Id as asq_id,
    ASQ_Number as asq_number,
    ASQ_Title as asq_title,
    Status__c as status,
    Account_Name__c as account_name,
    Owner_Name as owner_name,
    CreatedDate as created_date,
    AssignmentDate__c as assignment_date,
    End_Date__c as due_date,
    Days_To_Review as days_to_review,
    Days_To_Assignment as days_to_assignment,
    Days_To_First_Response as days_to_first_response,
    Review_SLA_Met as review_sla_met,
    Assignment_SLA_Met as assignment_sla_met,
    Response_SLA_Met as response_sla_met,
    SLA_Stage as sla_stage,
    Created_Week as created_week,
    Created_Month as created_month,
    current_timestamp() as synced_at
FROM home_christopher_chalcraft.cjc_views.cjc_asq_sla_metrics;

-- ============================================================================
-- SYNC: Effort Accuracy
-- ============================================================================
CREATE OR REPLACE TABLE cjc_aws_workspace_catalog.ssa_ops_dev.asq_effort_accuracy AS
SELECT
    Id as asq_id,
    ASQ_Number as asq_number,
    ASQ_Title as asq_title,
    Status__c as status,
    Account_Name__c as account_name,
    Specialization__c as specialization,
    Support_Type__c as support_type,
    Owner_Name as owner_name,
    Estimated_Days as estimated_days,
    Actual_Days as actual_days,
    Days_In_Progress as days_in_progress,
    Effective_Actual_Days as effective_actual_days,
    Effort_Ratio as effort_ratio,
    Accuracy_Category as accuracy_category,
    Variance_Days as variance_days,
    Completion_Quarter as completion_quarter,
    current_timestamp() as synced_at
FROM home_christopher_chalcraft.cjc_views.cjc_asq_effort_accuracy;

-- ============================================================================
-- SYNC: Re-engagement
-- ============================================================================
CREATE OR REPLACE TABLE cjc_aws_workspace_catalog.ssa_ops_dev.asq_reengagement AS
SELECT
    Account_Id as account_id,
    Account_Name as account_name,
    Total_ASQs as total_asqs,
    Unique_SSAs as unique_ssas,
    First_ASQ_Date as first_asq_date,
    Latest_ASQ_Date as latest_asq_date,
    Engagement_Span_Days as engagement_span_days,
    ASQs_YTD as asqs_ytd,
    ASQs_QTD as asqs_qtd,
    Active_ASQs as active_asqs,
    Completed_ASQs as completed_asqs,
    Engagement_Tier as engagement_tier,
    Is_Repeat_Customer as is_repeat_customer,
    current_timestamp() as synced_at
FROM home_christopher_chalcraft.cjc_views.cjc_asq_reengagement;

-- ============================================================================
-- SYNC: SSA Performance (aggregated from person metrics)
-- ============================================================================
CREATE OR REPLACE TABLE cjc_aws_workspace_catalog.ssa_ops_dev.ssa_performance AS
SELECT
    Owner_Name as owner_name,
    Total_Open_ASQs as total_open_asqs,
    Overdue_Count as overdue_count,
    Missing_Notes as missing_notes,
    Pct_Missing_Notes as pct_missing_notes,
    Pct_Overdue as pct_overdue,
    current_timestamp() as synced_at
FROM home_christopher_chalcraft.cjc_views.cjc_asq_person_metrics;

-- ============================================================================
-- Verification queries
-- ============================================================================
-- SELECT 'team_summary' as table_name, COUNT(*) as row_count FROM cjc_aws_workspace_catalog.ssa_ops_dev.team_summary
-- UNION ALL SELECT 'asq_completed_metrics', COUNT(*) FROM cjc_aws_workspace_catalog.ssa_ops_dev.asq_completed_metrics
-- UNION ALL SELECT 'asq_sla_metrics', COUNT(*) FROM cjc_aws_workspace_catalog.ssa_ops_dev.asq_sla_metrics
-- UNION ALL SELECT 'asq_effort_accuracy', COUNT(*) FROM cjc_aws_workspace_catalog.ssa_ops_dev.asq_effort_accuracy
-- UNION ALL SELECT 'asq_reengagement', COUNT(*) FROM cjc_aws_workspace_catalog.ssa_ops_dev.asq_reengagement
-- UNION ALL SELECT 'ssa_performance', COUNT(*) FROM cjc_aws_workspace_catalog.ssa_ops_dev.ssa_performance;
