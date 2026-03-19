-- Cross-Tier Validation: Logfood Views vs Delta Tables
-- Run on: fevm-cjc workspace (with cross-catalog access to logfood)
--
-- NOTE: This requires Unity Catalog federation between workspaces.
-- If federation is not available, run comparisons manually.

-- ============================================================================
-- TEST 1: Row Count Comparison (requires cross-catalog access)
-- ============================================================================
-- This test compares row counts between source views and synced Delta tables
-- Uncomment when cross-catalog access is configured

/*
SELECT
    'row_count_comparison' as test_name,
    source.view_name,
    source.source_count,
    target.target_count,
    source.source_count - target.target_count as difference,
    CASE
        WHEN source.source_count = target.target_count THEN 'PASS'
        WHEN ABS(source.source_count - target.target_count) <= 5 THEN 'WARN: Minor difference'
        ELSE 'FAIL: Significant row count mismatch'
    END as status
FROM (
    SELECT 'cjc_asq_completed_metrics' as view_name, COUNT(*) as source_count
    FROM home_christopher_chalcraft.cjc_views.cjc_asq_completed_metrics
    UNION ALL
    SELECT 'cjc_asq_sla_metrics', COUNT(*)
    FROM home_christopher_chalcraft.cjc_views.cjc_asq_sla_metrics
    UNION ALL
    SELECT 'cjc_asq_effort_accuracy', COUNT(*)
    FROM home_christopher_chalcraft.cjc_views.cjc_asq_effort_accuracy
    UNION ALL
    SELECT 'cjc_asq_reengagement', COUNT(*)
    FROM home_christopher_chalcraft.cjc_views.cjc_asq_reengagement
) source
JOIN (
    SELECT 'cjc_asq_completed_metrics' as view_name, COUNT(*) as target_count
    FROM cjc_aws_workspace_catalog.ssa_ops_dev.asq_completed_metrics
    UNION ALL
    SELECT 'cjc_asq_sla_metrics', COUNT(*)
    FROM cjc_aws_workspace_catalog.ssa_ops_dev.asq_sla_metrics
    UNION ALL
    SELECT 'cjc_asq_effort_accuracy', COUNT(*)
    FROM cjc_aws_workspace_catalog.ssa_ops_dev.asq_effort_accuracy
    UNION ALL
    SELECT 'cjc_asq_reengagement', COUNT(*)
    FROM cjc_aws_workspace_catalog.ssa_ops_dev.asq_reengagement
) target
ON source.view_name = target.view_name;
*/

-- ============================================================================
-- TEST 2: Sample Data Comparison (alternative when no cross-catalog)
-- Run this on Delta tables and compare to logfood results manually
-- ============================================================================
SELECT
    'sample_asq_ids' as test_name,
    asq_id,
    asq_number,
    owner_name,
    completion_date,
    synced_at
FROM cjc_aws_workspace_catalog.ssa_ops_dev.asq_completed_metrics
ORDER BY completion_date DESC
LIMIT 10;

-- ============================================================================
-- TEST 3: Checksum Validation (aggregate comparison)
-- ============================================================================
SELECT
    'aggregate_checksum_completed' as test_name,
    COUNT(*) as row_count,
    COUNT(DISTINCT asq_id) as unique_asqs,
    COUNT(DISTINCT owner_name) as unique_owners,
    MIN(completion_date) as min_completion,
    MAX(completion_date) as max_completion,
    SUM(days_total) as sum_days_total,
    AVG(days_total) as avg_days_total
FROM cjc_aws_workspace_catalog.ssa_ops_dev.asq_completed_metrics;

-- Compare these values to logfood:
-- SELECT COUNT(*), COUNT(DISTINCT Id), COUNT(DISTINCT Owner_Name),
--        MIN(Completion_Date), MAX(Completion_Date),
--        SUM(Days_Total), AVG(Days_Total)
-- FROM home_christopher_chalcraft.cjc_views.cjc_asq_completed_metrics;

-- ============================================================================
-- TEST 4: Team Summary Consistency
-- ============================================================================
SELECT
    'team_summary_consistency' as test_name,
    ts.total_open_asqs as summary_open,
    ts.overdue_asqs as summary_overdue,
    sp.total_performance_open,
    sp.total_performance_overdue,
    CASE
        WHEN ts.total_open_asqs = sp.total_performance_open THEN 'PASS'
        ELSE 'WARN: Open ASQ count mismatch between team_summary and ssa_performance'
    END as status
FROM cjc_aws_workspace_catalog.ssa_ops_dev.team_summary ts
CROSS JOIN (
    SELECT
        SUM(total_open_asqs) as total_performance_open,
        SUM(overdue_count) as total_performance_overdue
    FROM cjc_aws_workspace_catalog.ssa_ops_dev.ssa_performance
) sp;

-- ============================================================================
-- TEST 5: SLA Metrics Sanity Check
-- ============================================================================
SELECT
    'sla_sanity_check' as test_name,
    COUNT(*) as total_asqs,
    SUM(review_sla_met) as review_sla_met_count,
    SUM(assignment_sla_met) as assignment_sla_met_count,
    SUM(response_sla_met) as response_sla_met_count,
    ROUND(100.0 * SUM(review_sla_met) / COUNT(*), 1) as review_sla_pct,
    ROUND(100.0 * SUM(assignment_sla_met) / COUNT(*), 1) as assignment_sla_pct,
    ROUND(100.0 * SUM(response_sla_met) / COUNT(*), 1) as response_sla_pct,
    CASE
        WHEN SUM(review_sla_met) >= 0 AND SUM(assignment_sla_met) >= 0 THEN 'PASS'
        ELSE 'FAIL: Invalid SLA counts'
    END as status
FROM cjc_aws_workspace_catalog.ssa_ops_dev.asq_sla_metrics;

-- ============================================================================
-- TEST 6: Effort Accuracy Distribution
-- ============================================================================
SELECT
    'effort_accuracy_distribution' as test_name,
    accuracy_category,
    COUNT(*) as count,
    ROUND(AVG(effort_ratio), 2) as avg_effort_ratio,
    'INFO' as status
FROM cjc_aws_workspace_catalog.ssa_ops_dev.asq_effort_accuracy
WHERE accuracy_category IS NOT NULL
GROUP BY accuracy_category
ORDER BY count DESC;

-- ============================================================================
-- TEST 7: Reengagement Tier Distribution
-- ============================================================================
SELECT
    'reengagement_distribution' as test_name,
    engagement_tier,
    COUNT(*) as account_count,
    SUM(total_asqs) as total_asqs_in_tier,
    ROUND(AVG(total_asqs), 1) as avg_asqs_per_account,
    'INFO' as status
FROM cjc_aws_workspace_catalog.ssa_ops_dev.asq_reengagement
GROUP BY engagement_tier
ORDER BY account_count DESC;
