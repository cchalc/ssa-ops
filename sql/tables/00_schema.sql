-- ============================================================================
-- SSA Ops Dimensional Model - Schema Setup
-- ============================================================================
-- Creates the ssa_ops schema for the dimensional model
-- Run on: fevm-cjc workspace
-- ============================================================================

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema}
COMMENT 'SSA Activity Dashboard dimensional model with metric views';

-- Grant permissions (adjust as needed for your workspace)
-- GRANT USAGE ON SCHEMA ${catalog}.${schema} TO `account users`;
-- GRANT SELECT ON SCHEMA ${catalog}.${schema} TO `account users`;
