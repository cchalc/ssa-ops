-- ============================================================================
-- materialization_config.sql - Metric View Materialization Configuration
-- ============================================================================
-- Enables materialization for metric views with configured refresh schedules
-- Run on target catalog/schema after deploying metric views
-- ============================================================================

-- ============================================================================
-- ENABLE MATERIALIZATION: Core Metric Views
-- ============================================================================

-- mv_asq_operations - Core operational metrics
-- Refresh every 6 hours during business hours
ALTER METRIC VIEW ${catalog}.${schema}.mv_asq_operations
SET MATERIALIZATION (
  cron = '0 0,6,12,18 * * *',
  warehouse_id = '${warehouse_id}',
  mode = 'relaxed'
);

-- mv_sla_compliance - SLA tracking
-- Refresh every 6 hours
ALTER METRIC VIEW ${catalog}.${schema}.mv_sla_compliance
SET MATERIALIZATION (
  cron = '0 1,7,13,19 * * *',
  warehouse_id = '${warehouse_id}',
  mode = 'relaxed'
);

-- mv_effort_capacity - Capacity planning
-- Refresh every 6 hours
ALTER METRIC VIEW ${catalog}.${schema}.mv_effort_capacity
SET MATERIALIZATION (
  cron = '0 2,8,14,20 * * *',
  warehouse_id = '${warehouse_id}',
  mode = 'relaxed'
);

-- ============================================================================
-- ENABLE MATERIALIZATION: Extended Metric Views
-- ============================================================================

-- mv_consumption_impact - Business impact correlation
-- Refresh every 12 hours (less critical, more expensive to compute)
ALTER METRIC VIEW ${catalog}.${schema}.mv_consumption_impact
SET MATERIALIZATION (
  cron = '0 3,15 * * *',
  warehouse_id = '${warehouse_id}',
  mode = 'relaxed'
);

-- mv_pipeline_impact - UCO pipeline tracking
-- Refresh every 12 hours
ALTER METRIC VIEW ${catalog}.${schema}.mv_pipeline_impact
SET MATERIALIZATION (
  cron = '0 4,16 * * *',
  warehouse_id = '${warehouse_id}',
  mode = 'relaxed'
);

-- mv_team_comparison - Cross-BU benchmarking
-- Refresh every 12 hours
ALTER METRIC VIEW ${catalog}.${schema}.mv_team_comparison
SET MATERIALIZATION (
  cron = '0 5,17 * * *',
  warehouse_id = '${warehouse_id}',
  mode = 'relaxed'
);

-- mv_trend_analysis - Time series trends
-- Refresh once daily (trends don't change frequently)
ALTER METRIC VIEW ${catalog}.${schema}.mv_trend_analysis
SET MATERIALIZATION (
  cron = '0 6 * * *',
  warehouse_id = '${warehouse_id}',
  mode = 'relaxed'
);

-- ============================================================================
-- CHECK MATERIALIZATION STATUS
-- ============================================================================

-- View materialization status for all metric views
SELECT
  table_name,
  table_type,
  data_source_format,
  created,
  last_altered,
  comment
FROM ${catalog}.information_schema.tables
WHERE table_schema = '${schema}'
  AND table_name LIKE 'mv_%'
ORDER BY table_name;

-- View refresh history
SELECT
  metric_view_name,
  refresh_start_time,
  refresh_end_time,
  refresh_status,
  TIMESTAMPDIFF(SECOND, refresh_start_time, refresh_end_time) AS duration_seconds
FROM ${catalog}.information_schema.metric_view_refresh_history
WHERE table_schema = '${schema}'
ORDER BY refresh_start_time DESC
LIMIT 20;

-- ============================================================================
-- DISABLE MATERIALIZATION (if needed)
-- ============================================================================
-- Uncomment to disable materialization on specific views

-- ALTER METRIC VIEW ${catalog}.${schema}.mv_asq_operations
-- UNSET MATERIALIZATION;

-- ============================================================================
-- REFRESH ON DEMAND
-- ============================================================================
-- Trigger immediate refresh of a specific metric view

-- REFRESH METRIC VIEW ${catalog}.${schema}.mv_asq_operations;

-- ============================================================================
-- PERFORMANCE MONITORING
-- ============================================================================
-- Track materialization performance over time

-- Query to check average refresh duration by view
SELECT
  metric_view_name,
  COUNT(*) AS refresh_count,
  AVG(TIMESTAMPDIFF(SECOND, refresh_start_time, refresh_end_time)) AS avg_duration_sec,
  MAX(TIMESTAMPDIFF(SECOND, refresh_start_time, refresh_end_time)) AS max_duration_sec,
  SUM(CASE WHEN refresh_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_count
FROM ${catalog}.information_schema.metric_view_refresh_history
WHERE table_schema = '${schema}'
  AND refresh_start_time >= DATE_SUB(CURRENT_DATE(), 7)
GROUP BY metric_view_name
ORDER BY avg_duration_sec DESC;
