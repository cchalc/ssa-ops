-- Unassigned ASQs with UCO Linkage Query
-- Finds ASQs needing assignment and links to production UCOs
-- Usage: Replace {{ region }} with region code (e.g., 'CAN')
-- Usage: Replace {{ days }} with lookback period (e.g., 30)

WITH recent_asqs AS (
    SELECT
        a.approval_request_name as asq_number,
        a.account_name,
        a.owner_user_name,
        a.owner_user_id,
        a.status,
        a.technical_specialization,
        a.support_type,
        COALESCE(a.estimated_effort_in_days, 5) as estimated_effort,
        a.created_date,
        a.account_id,
        a.description
    FROM main.gtm_silver.approval_request_detail a
    WHERE a.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
      AND a.region_level_1 = '{{ region }}'
      AND a.created_date >= DATE_SUB(CURRENT_DATE, {{ days }})
      AND a.owner_user_name IS NULL
),

-- UCO linkage (production stages only)
uco_links AS (
    SELECT
        u.account_id,
        MAX(u.stage) as best_stage,
        MAX(TRY_CAST(u.monthly_total_dollar_dbus AS DOUBLE)) as max_dbu,
        MAX(u.competitors) as competitors
    FROM main.gtm_silver.use_case_detail u
    WHERE u.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.use_case_detail)
      AND u.stage IN ('U3', 'U4', 'U5', 'U6')
    GROUP BY u.account_id
)

SELECT
    r.asq_number,
    r.account_name,
    r.technical_specialization,
    r.support_type,
    r.estimated_effort,
    r.created_date,
    r.description,
    u.best_stage as uco_stage,
    u.max_dbu as monthly_dbu,
    u.competitors,
    -- Importance score for prioritization
    CASE
        WHEN u.best_stage = 'U6' AND u.max_dbu > 100000 THEN 'HIGH_VALUE_PRODUCTION'
        WHEN u.best_stage = 'U6' THEN 'PRODUCTION'
        WHEN u.best_stage IN ('U4', 'U5') THEN 'NEAR_WIN'
        WHEN u.competitors IS NOT NULL AND u.competitors != 'No Competitor' THEN 'COMPETITIVE'
        ELSE 'STANDARD'
    END as importance
FROM recent_asqs r
LEFT JOIN uco_links u ON r.account_id = u.account_id
ORDER BY
    CASE
        WHEN u.best_stage = 'U6' THEN 1
        WHEN u.best_stage = 'U5' THEN 2
        WHEN u.best_stage = 'U4' THEN 3
        ELSE 4
    END,
    COALESCE(u.max_dbu, 0) DESC,
    r.created_date
