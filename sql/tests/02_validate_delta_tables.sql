-- Validation Tests for Delta Tables on fevm-cjc
-- Run on: fevm-cjc workspace
-- Target: cjc_aws_workspace_catalog.ssa_ops_dev
--
-- These tests verify that Delta tables are synced correctly from logfood

-- ============================================================================
-- TEST 1: Table Existence and Row Counts
-- ============================================================================
SELECT
    'team_summary' as table_name,
    COUNT(*) as row_count,
    MAX(synced_at) as last_sync,
    CASE
        WHEN COUNT(*) = 1 THEN 'PASS'
        ELSE 'FAIL: Expected 1 row'
    END as status
FROM cjc_aws_workspace_catalog.ssa_ops_dev.team_summary

UNION ALL

SELECT
    'asq_completed_metrics',
    COUNT(*),
    MAX(synced_at),
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL: No data' END
FROM cjc_aws_workspace_catalog.ssa_ops_dev.asq_completed_metrics

UNION ALL

SELECT
    'asq_sla_metrics',
    COUNT(*),
    MAX(synced_at),
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL: No data' END
FROM cjc_aws_workspace_catalog.ssa_ops_dev.asq_sla_metrics

UNION ALL

SELECT
    'asq_effort_accuracy',
    COUNT(*),
    MAX(synced_at),
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL: No data' END
FROM cjc_aws_workspace_catalog.ssa_ops_dev.asq_effort_accuracy

UNION ALL

SELECT
    'asq_reengagement',
    COUNT(*),
    MAX(synced_at),
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL: No data' END
FROM cjc_aws_workspace_catalog.ssa_ops_dev.asq_reengagement

UNION ALL

SELECT
    'ssa_performance',
    COUNT(*),
    MAX(synced_at),
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL: No data' END
FROM cjc_aws_workspace_catalog.ssa_ops_dev.ssa_performance;

-- ============================================================================
-- TEST 2: Sync Freshness (should be within 24 hours)
-- ============================================================================
SELECT
    'sync_freshness' as test_name,
    table_name,
    last_sync,
    hours_since_sync,
    CASE
        WHEN hours_since_sync <= 24 THEN 'PASS'
        WHEN hours_since_sync <= 48 THEN 'WARN: Sync > 24 hours old'
        ELSE 'FAIL: Sync > 48 hours old'
    END as status
FROM (
    SELECT 'team_summary' as table_name, MAX(synced_at) as last_sync,
           TIMESTAMPDIFF(HOUR, MAX(synced_at), CURRENT_TIMESTAMP()) as hours_since_sync
    FROM cjc_aws_workspace_catalog.ssa_ops_dev.team_summary
    UNION ALL
    SELECT 'asq_completed_metrics', MAX(synced_at),
           TIMESTAMPDIFF(HOUR, MAX(synced_at), CURRENT_TIMESTAMP())
    FROM cjc_aws_workspace_catalog.ssa_ops_dev.asq_completed_metrics
    UNION ALL
    SELECT 'asq_sla_metrics', MAX(synced_at),
           TIMESTAMPDIFF(HOUR, MAX(synced_at), CURRENT_TIMESTAMP())
    FROM cjc_aws_workspace_catalog.ssa_ops_dev.asq_sla_metrics
);

-- ============================================================================
-- TEST 3: Primary Key Uniqueness
-- ============================================================================
SELECT
    'pk_uniqueness_completed' as test_name,
    COUNT(*) as total_rows,
    COUNT(DISTINCT asq_id) as unique_ids,
    CASE
        WHEN COUNT(*) = COUNT(DISTINCT asq_id) THEN 'PASS'
        ELSE 'FAIL: Duplicate ASQ IDs'
    END as status
FROM cjc_aws_workspace_catalog.ssa_ops_dev.asq_completed_metrics

UNION ALL

SELECT
    'pk_uniqueness_sla',
    COUNT(*),
    COUNT(DISTINCT asq_id),
    CASE
        WHEN COUNT(*) = COUNT(DISTINCT asq_id) THEN 'PASS'
        ELSE 'FAIL: Duplicate ASQ IDs'
    END
FROM cjc_aws_workspace_catalog.ssa_ops_dev.asq_sla_metrics

UNION ALL

SELECT
    'pk_uniqueness_reengagement',
    COUNT(*),
    COUNT(DISTINCT account_id),
    CASE
        WHEN COUNT(*) = COUNT(DISTINCT account_id) THEN 'PASS'
        ELSE 'FAIL: Duplicate Account IDs'
    END
FROM cjc_aws_workspace_catalog.ssa_ops_dev.asq_reengagement

UNION ALL

SELECT
    'pk_uniqueness_ssa_perf',
    COUNT(*),
    COUNT(DISTINCT owner_name),
    CASE
        WHEN COUNT(*) = COUNT(DISTINCT owner_name) THEN 'PASS'
        ELSE 'FAIL: Duplicate Owner Names'
    END
FROM cjc_aws_workspace_catalog.ssa_ops_dev.ssa_performance;

-- ============================================================================
-- TEST 4: Required Field Completeness
-- ============================================================================
SELECT
    'required_fields_completed' as test_name,
    COUNT(*) as total_records,
    SUM(CASE WHEN asq_id IS NULL THEN 1 ELSE 0 END) as null_asq_id,
    SUM(CASE WHEN asq_number IS NULL THEN 1 ELSE 0 END) as null_asq_number,
    SUM(CASE WHEN owner_name IS NULL THEN 1 ELSE 0 END) as null_owner_name,
    CASE
        WHEN SUM(CASE WHEN asq_id IS NULL OR asq_number IS NULL OR owner_name IS NULL THEN 1 ELSE 0 END) = 0
        THEN 'PASS'
        ELSE 'FAIL: Null required fields'
    END as status
FROM cjc_aws_workspace_catalog.ssa_ops_dev.asq_completed_metrics;

-- ============================================================================
-- TEST 5: Referential Integrity (SSA names match across tables)
-- ============================================================================
SELECT
    'ssa_name_consistency' as test_name,
    sp.owner_name as ssa_in_performance,
    COUNT(DISTINCT acm.asq_id) as asqs_in_completed,
    CASE
        WHEN COUNT(DISTINCT acm.asq_id) > 0 THEN 'PASS'
        ELSE 'WARN: No completed ASQs for this SSA'
    END as status
FROM cjc_aws_workspace_catalog.ssa_ops_dev.ssa_performance sp
LEFT JOIN cjc_aws_workspace_catalog.ssa_ops_dev.asq_completed_metrics acm
    ON sp.owner_name = acm.owner_name
GROUP BY sp.owner_name
ORDER BY asqs_in_completed;

-- ============================================================================
-- TEST 6: Data Range Validation
-- ============================================================================
SELECT
    'date_range_check' as test_name,
    MIN(created_date) as earliest_created,
    MAX(created_date) as latest_created,
    MIN(completion_date) as earliest_completed,
    MAX(completion_date) as latest_completed,
    CASE
        WHEN MIN(created_date) >= '2020-01-01'
         AND MAX(created_date) <= CURRENT_DATE()
         AND MAX(completion_date) <= CURRENT_DATE()
        THEN 'PASS'
        ELSE 'WARN: Dates outside expected range'
    END as status
FROM cjc_aws_workspace_catalog.ssa_ops_dev.asq_completed_metrics;

-- ============================================================================
-- TEST 7: Numeric Range Validation
-- ============================================================================
SELECT
    'numeric_ranges' as test_name,
    MIN(days_total) as min_days_total,
    MAX(days_total) as max_days_total,
    AVG(days_total) as avg_days_total,
    CASE
        WHEN MIN(days_total) >= 0 AND MAX(days_total) <= 365 THEN 'PASS'
        ELSE 'WARN: Days total outside expected range'
    END as status
FROM cjc_aws_workspace_catalog.ssa_ops_dev.asq_completed_metrics;
