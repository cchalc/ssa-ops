-- Validation Tests for Logfood SQL Views
-- Run on: logfood workspace (Shared SQL Endpoint)
-- Target: home_christopher_chalcraft.cjc_views
--
-- These tests verify that the source views exist and return valid data

-- ============================================================================
-- TEST 1: View Existence and Row Counts
-- ============================================================================
SELECT
    'cjc_team_summary' as view_name,
    COUNT(*) as row_count,
    CASE WHEN COUNT(*) = 1 THEN 'PASS' ELSE 'FAIL: Expected 1 row' END as status
FROM home_christopher_chalcraft.cjc_views.cjc_team_summary

UNION ALL

SELECT
    'cjc_asq_completed_metrics',
    COUNT(*),
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL: No data' END
FROM home_christopher_chalcraft.cjc_views.cjc_asq_completed_metrics

UNION ALL

SELECT
    'cjc_asq_sla_metrics',
    COUNT(*),
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL: No data' END
FROM home_christopher_chalcraft.cjc_views.cjc_asq_sla_metrics

UNION ALL

SELECT
    'cjc_asq_effort_accuracy',
    COUNT(*),
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL: No data' END
FROM home_christopher_chalcraft.cjc_views.cjc_asq_effort_accuracy

UNION ALL

SELECT
    'cjc_asq_reengagement',
    COUNT(*),
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL: No data' END
FROM home_christopher_chalcraft.cjc_views.cjc_asq_reengagement

UNION ALL

SELECT
    'cjc_asq_person_metrics',
    COUNT(*),
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL: No data' END
FROM home_christopher_chalcraft.cjc_views.cjc_asq_person_metrics;

-- ============================================================================
-- TEST 2: Team Summary Data Quality
-- ============================================================================
SELECT
    'team_summary_totals' as test_name,
    CASE
        WHEN Total_Open_ASQs >= 0
         AND Overdue_ASQs >= 0
         AND Overdue_ASQs <= Total_Open_ASQs
         AND Missing_Notes_ASQs >= 0
        THEN 'PASS'
        ELSE 'FAIL: Invalid totals'
    END as status,
    Total_Open_ASQs,
    Overdue_ASQs,
    Missing_Notes_ASQs
FROM home_christopher_chalcraft.cjc_views.cjc_team_summary;

-- ============================================================================
-- TEST 3: Completed Metrics Data Quality
-- ============================================================================
SELECT
    'completed_metrics_quality' as test_name,
    COUNT(*) as total_records,
    SUM(CASE WHEN Id IS NULL THEN 1 ELSE 0 END) as null_ids,
    SUM(CASE WHEN ASQ_Number IS NULL THEN 1 ELSE 0 END) as null_numbers,
    SUM(CASE WHEN Owner_Name IS NULL THEN 1 ELSE 0 END) as null_owners,
    SUM(CASE WHEN Completion_Date IS NULL THEN 1 ELSE 0 END) as null_completion_dates,
    CASE
        WHEN SUM(CASE WHEN Id IS NULL THEN 1 ELSE 0 END) = 0
         AND SUM(CASE WHEN ASQ_Number IS NULL THEN 1 ELSE 0 END) = 0
         AND SUM(CASE WHEN Owner_Name IS NULL THEN 1 ELSE 0 END) = 0
        THEN 'PASS'
        ELSE 'FAIL: Null required fields'
    END as status
FROM home_christopher_chalcraft.cjc_views.cjc_asq_completed_metrics;

-- ============================================================================
-- TEST 4: SLA Metrics Value Ranges
-- ============================================================================
SELECT
    'sla_metrics_ranges' as test_name,
    COUNT(*) as total_records,
    SUM(CASE WHEN Review_SLA_Met NOT IN (0, 1) THEN 1 ELSE 0 END) as invalid_review_sla,
    SUM(CASE WHEN Assignment_SLA_Met NOT IN (0, 1) THEN 1 ELSE 0 END) as invalid_assignment_sla,
    SUM(CASE WHEN Response_SLA_Met NOT IN (0, 1) THEN 1 ELSE 0 END) as invalid_response_sla,
    CASE
        WHEN SUM(CASE WHEN Review_SLA_Met NOT IN (0, 1) THEN 1 ELSE 0 END) = 0
         AND SUM(CASE WHEN Assignment_SLA_Met NOT IN (0, 1) THEN 1 ELSE 0 END) = 0
         AND SUM(CASE WHEN Response_SLA_Met NOT IN (0, 1) THEN 1 ELSE 0 END) = 0
        THEN 'PASS'
        ELSE 'FAIL: Invalid SLA values'
    END as status
FROM home_christopher_chalcraft.cjc_views.cjc_asq_sla_metrics;

-- ============================================================================
-- TEST 5: Effort Accuracy Calculations
-- ============================================================================
SELECT
    'effort_accuracy_calc' as test_name,
    COUNT(*) as total_records,
    SUM(CASE WHEN Effort_Ratio < 0 THEN 1 ELSE 0 END) as negative_ratios,
    SUM(CASE WHEN Effort_Ratio > 10 THEN 1 ELSE 0 END) as extreme_ratios,
    SUM(CASE WHEN Accuracy_Category NOT IN ('Under-estimated', 'Accurate', 'Over-estimated') THEN 1 ELSE 0 END) as invalid_categories,
    CASE
        WHEN SUM(CASE WHEN Effort_Ratio < 0 THEN 1 ELSE 0 END) = 0
         AND SUM(CASE WHEN Accuracy_Category NOT IN ('Under-estimated', 'Accurate', 'Over-estimated') THEN 1 ELSE 0 END) = 0
        THEN 'PASS'
        ELSE 'FAIL: Invalid effort calculations'
    END as status
FROM home_christopher_chalcraft.cjc_views.cjc_asq_effort_accuracy;

-- ============================================================================
-- TEST 6: Reengagement Tier Distribution
-- ============================================================================
SELECT
    'reengagement_tiers' as test_name,
    Engagement_Tier,
    COUNT(*) as account_count,
    'INFO' as status
FROM home_christopher_chalcraft.cjc_views.cjc_asq_reengagement
GROUP BY Engagement_Tier
ORDER BY account_count DESC;

-- ============================================================================
-- TEST 7: Data Freshness Check
-- ============================================================================
SELECT
    'data_freshness' as test_name,
    MAX(CreatedDate) as latest_created,
    MAX(Completion_Date) as latest_completed,
    DATEDIFF(CURRENT_DATE(), MAX(CreatedDate)) as days_since_latest_created,
    CASE
        WHEN DATEDIFF(CURRENT_DATE(), MAX(CreatedDate)) <= 7 THEN 'PASS'
        ELSE 'WARN: No recent ASQs in last 7 days'
    END as status
FROM home_christopher_chalcraft.cjc_views.cjc_asq_completed_metrics;
