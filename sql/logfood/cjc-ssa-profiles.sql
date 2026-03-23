-- SSA Profile Builder Query
-- Analyzes 2-year work history for SSAs to determine specializations and capacity
-- Usage: Replace {{ user_ids }} with comma-separated quoted IDs
-- Example: '0058Y00000CPeiKQAT', '0058Y00000CP6yKQAT'

WITH ssa_history AS (
    SELECT
        owner_user_id,
        owner_user_name,
        technical_specialization,
        support_type,
        COALESCE(actual_effort_in_days, estimated_effort_in_days, 5) as effort_days,
        status,
        account_name,
        account_id,
        created_date
    FROM main.gtm_silver.approval_request_detail
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
      AND owner_user_id IN ({{ user_ids }})
      AND created_date >= DATE_SUB(CURRENT_DATE, 730)
),

-- Specialization summary (top 3 per SSA)
spec_summary AS (
    SELECT
        owner_user_name,
        owner_user_id,
        technical_specialization,
        COUNT(*) as ticket_count,
        SUM(effort_days) as total_effort,
        COUNT(CASE WHEN status = 'Complete' THEN 1 END) as completed_count,
        ROW_NUMBER() OVER (PARTITION BY owner_user_id ORDER BY SUM(effort_days) DESC) as spec_rank
    FROM ssa_history
    WHERE technical_specialization IS NOT NULL
    GROUP BY owner_user_name, owner_user_id, technical_specialization
),

-- Current capacity (open ASQs)
capacity AS (
    SELECT
        owner_user_id,
        COUNT(*) as open_asqs,
        SUM(COALESCE(estimated_effort_in_days, 5)) as planned_effort,
        COUNT(CASE WHEN DATEDIFF(CURRENT_DATE, target_end_date) > 0 THEN 1 END) as overdue_count
    FROM main.gtm_silver.approval_request_detail
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
      AND owner_user_id IN ({{ user_ids }})
      AND status IN ('Approved', 'In Progress', 'New', 'Assigned')
    GROUP BY owner_user_id
),

-- Overall stats per SSA
overall_stats AS (
    SELECT
        owner_user_id,
        owner_user_name,
        COUNT(*) as total_asqs_2yr,
        COUNT(CASE WHEN status = 'Complete' THEN 1 END) as completed_asqs,
        SUM(effort_days) as total_effort_days,
        COUNT(DISTINCT account_id) as unique_accounts
    FROM ssa_history
    GROUP BY owner_user_id, owner_user_name
)

SELECT
    o.owner_user_name as ssa_name,
    o.owner_user_id as ssa_id,
    o.total_asqs_2yr,
    o.completed_asqs,
    ROUND(100.0 * o.completed_asqs / NULLIF(o.total_asqs_2yr, 0), 1) as completion_rate,
    o.total_effort_days,
    o.unique_accounts,
    c.open_asqs,
    c.planned_effort as current_effort_days,
    c.overdue_count,
    CASE
        WHEN c.planned_effort > 50 THEN 'OVERLOADED'
        WHEN c.planned_effort > 30 THEN 'HEAVY'
        WHEN c.planned_effort > 15 THEN 'MODERATE'
        ELSE 'LIGHT'
    END as workload_status,
    -- Top 3 specializations as JSON array
    COLLECT_LIST(
        NAMED_STRUCT(
            'specialization', s.technical_specialization,
            'ticket_count', s.ticket_count,
            'effort_days', s.total_effort
        )
    ) as top_specializations
FROM overall_stats o
LEFT JOIN capacity c ON o.owner_user_id = c.owner_user_id
LEFT JOIN spec_summary s ON o.owner_user_id = s.owner_user_id AND s.spec_rank <= 3
GROUP BY
    o.owner_user_name, o.owner_user_id, o.total_asqs_2yr, o.completed_asqs,
    o.total_effort_days, o.unique_accounts, c.open_asqs, c.planned_effort, c.overdue_count
ORDER BY c.planned_effort ASC NULLS LAST
