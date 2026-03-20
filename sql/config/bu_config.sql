-- ============================================================================
-- SSA Ops - Business Unit Configuration
-- ============================================================================
-- Set these variables before running table/metric view deployments
-- These are used for variable substitution in SQL files
-- ============================================================================

-- Target catalog and schema
SET VAR catalog = 'cjc_aws_workspace_catalog';
SET VAR schema = 'ssa_ops';

-- Source catalog (logfood views synced to Delta)
SET VAR source_catalog = 'cjc_aws_workspace_catalog';
SET VAR source_schema = 'ssa_ops_dev';

-- ============================================================================
-- Business Unit Filters (uncomment one set)
-- ============================================================================

-- === Canada (CAN) - Default ===
-- Use for CJC's team on fevm-cjc workspace
SET VAR bu_name = 'CAN';
SET VAR bu_filter = "business_unit = 'CAN'";
SET VAR manager_l2_id = '0053f000000pKoTAAU';  -- CJC's ID

-- === US West ===
-- SET VAR bu_name = 'US-WEST';
-- SET VAR bu_filter = "business_unit = 'US-WEST'";
-- SET VAR manager_l2_id = 'XXXXXXXXXXXXXXXXXX';

-- === US East ===
-- SET VAR bu_name = 'US-EAST';
-- SET VAR bu_filter = "business_unit = 'US-EAST'";
-- SET VAR manager_l2_id = 'XXXXXXXXXXXXXXXXXX';

-- === EMEA ===
-- SET VAR bu_name = 'EMEA';
-- SET VAR bu_filter = "business_unit = 'EMEA'";
-- SET VAR manager_l2_id = 'XXXXXXXXXXXXXXXXXX';

-- === APJ ===
-- SET VAR bu_name = 'APJ';
-- SET VAR bu_filter = "business_unit = 'APJ'";
-- SET VAR manager_l2_id = 'XXXXXXXXXXXXXXXXXX';

-- === All BUs (no filter) ===
-- SET VAR bu_name = 'ALL';
-- SET VAR bu_filter = '1=1';
-- SET VAR manager_l2_id = NULL;

-- ============================================================================
-- Fiscal Calendar Configuration
-- ============================================================================
-- Databricks fiscal year ends January 31
-- FY26 = Feb 1, 2025 - Jan 31, 2026

SET VAR fy_end_month = 1;   -- January
SET VAR fy_end_day = 31;    -- 31st

-- ============================================================================
-- SLA Thresholds (business days)
-- ============================================================================

SET VAR sla_review_days = 2;      -- Review/triage within 2 days
SET VAR sla_assignment_days = 5;  -- Assignment within 5 days
SET VAR sla_response_days = 5;    -- First response within 5 days
