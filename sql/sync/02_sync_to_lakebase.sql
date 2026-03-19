-- Sync Delta tables from fevm-cjc catalog to Lakebase PostgreSQL
-- Run on: fevm-cjc workspace with Lakebase access
-- Source: cjc_aws_workspace_catalog.ssa_ops_dev (Delta tables)
-- Target: ssa_ops_dev.dashboard (Lakebase PostgreSQL)
--
-- Prerequisites:
--   1. Delta tables must be populated from logfood sync (01_export_to_delta.sql)
--   2. Lakebase database and schema must exist (02_dashboard_schema.sql)
--   3. Lakehouse Federation must be configured

-- ============================================================================
-- NOTE: This SQL demonstrates the sync pattern. In practice, use one of:
--   A) Lakehouse Federation (automatic sync via foreign catalog)
--   B) Python job with pg8000/psycopg2 for explicit sync
--   C) Spark JDBC write from Delta to PostgreSQL
-- ============================================================================

-- OPTION A: Lakehouse Federation Pattern
-- If Lakehouse Federation is configured, Delta tables appear as foreign tables
-- in Lakebase automatically. No manual sync needed.

-- OPTION B: Manual JDBC Sync (run as Python task)
-- See: src/jobs/sync_to_lakebase.py

-- OPTION C: Verification queries (run on SQL warehouse)
-- Verify Delta tables exist and have data

SELECT 'team_summary' as table_name,
       COUNT(*) as row_count,
       MAX(synced_at) as last_sync
FROM cjc_aws_workspace_catalog.ssa_ops_dev.team_summary

UNION ALL

SELECT 'asq_completed_metrics',
       COUNT(*),
       MAX(synced_at)
FROM cjc_aws_workspace_catalog.ssa_ops_dev.asq_completed_metrics

UNION ALL

SELECT 'asq_sla_metrics',
       COUNT(*),
       MAX(synced_at)
FROM cjc_aws_workspace_catalog.ssa_ops_dev.asq_sla_metrics

UNION ALL

SELECT 'asq_effort_accuracy',
       COUNT(*),
       MAX(synced_at)
FROM cjc_aws_workspace_catalog.ssa_ops_dev.asq_effort_accuracy

UNION ALL

SELECT 'asq_reengagement',
       COUNT(*),
       MAX(synced_at)
FROM cjc_aws_workspace_catalog.ssa_ops_dev.asq_reengagement

UNION ALL

SELECT 'ssa_performance',
       COUNT(*),
       MAX(synced_at)
FROM cjc_aws_workspace_catalog.ssa_ops_dev.ssa_performance;
