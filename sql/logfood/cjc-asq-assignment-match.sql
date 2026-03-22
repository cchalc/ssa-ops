-- ASQ Assignment Matching Query
-- Matches unassigned ASQs to SSAs based on specialization and capacity
-- Usage: Replace {{ region }} with region code (e.g., 'CAN')

-- Step 1: Define direct reports (CJC's team)
WITH direct_reports AS (
    SELECT name, user_id FROM (
        VALUES
            ('Volodymyr Vragov', '005Vp000002lC2zIAE'),
            ('Allan Cao', '0058Y00000CPeiKQAT'),
            ('Harsha Pasala', '0058Y00000CP6yKQAT'),
            ('Réda Khouani', '0053f000000Wi00AAC'),
            ('Scott McKean', '005Vp0000016p45IAA'),
            ('Mathieu Pelletier', '0058Y00000CPn0bQAD')
    ) AS t(name, user_id)
),

-- Step 2: Calculate current capacity
ssa_capacity AS (
    SELECT
        owner_user_id,
        owner_user_name,
        COUNT(*) as open_asqs,
        SUM(COALESCE(estimated_effort_in_days, 5)) as planned_effort,
        CASE
            WHEN SUM(COALESCE(estimated_effort_in_days, 5)) > 50 THEN 'OVERLOADED'
            WHEN SUM(COALESCE(estimated_effort_in_days, 5)) > 30 THEN 'HEAVY'
            WHEN SUM(COALESCE(estimated_effort_in_days, 5)) > 15 THEN 'MODERATE'
            ELSE 'LIGHT'
        END as workload_status,
        CASE
            WHEN SUM(COALESCE(estimated_effort_in_days, 5)) > 50 THEN -20
            WHEN SUM(COALESCE(estimated_effort_in_days, 5)) > 30 THEN -5
            WHEN SUM(COALESCE(estimated_effort_in_days, 5)) > 15 THEN 5
            ELSE 15
        END as capacity_score
    FROM main.gtm_silver.approval_request_detail
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
      AND owner_user_id IN (SELECT user_id FROM direct_reports)
      AND status IN ('Approved', 'In Progress', 'New', 'Assigned')
    GROUP BY owner_user_id, owner_user_name
),

-- Step 3: Get top specialization per SSA (2-year history)
ssa_specializations AS (
    SELECT
        owner_user_id,
        owner_user_name,
        technical_specialization as primary_spec,
        ticket_count,
        ROW_NUMBER() OVER (PARTITION BY owner_user_id ORDER BY ticket_count DESC) as rn
    FROM (
        SELECT
            owner_user_id,
            owner_user_name,
            technical_specialization,
            COUNT(*) as ticket_count
        FROM main.gtm_silver.approval_request_detail
        WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
          AND owner_user_id IN (SELECT user_id FROM direct_reports)
          AND created_date >= DATE_SUB(CURRENT_DATE, 730)
          AND technical_specialization IS NOT NULL
        GROUP BY owner_user_id, owner_user_name, technical_specialization
    )
),

-- Step 4: Build SSA profiles
ssa_profiles AS (
    SELECT
        c.owner_user_id as ssa_id,
        c.owner_user_name as ssa_name,
        c.workload_status,
        c.capacity_score,
        c.planned_effort,
        s.primary_spec,
        s.ticket_count as spec_experience
    FROM ssa_capacity c
    LEFT JOIN ssa_specializations s ON c.owner_user_id = s.owner_user_id AND s.rn = 1
),

-- Step 5: Get unassigned ASQs
unassigned_asqs AS (
    SELECT
        approval_request_name as asq_number,
        account_name,
        technical_specialization,
        support_type,
        COALESCE(estimated_effort_in_days, 5) as estimated_effort,
        account_id
    FROM main.gtm_silver.approval_request_detail
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
      AND region_level_1 = '{{ region }}'
      AND created_date >= DATE_SUB(CURRENT_DATE, 30)
      AND owner_user_name IS NULL
),

-- Step 6: Score each SSA for each ASQ
assignment_scores AS (
    SELECT
        a.asq_number,
        a.account_name,
        a.technical_specialization as asq_spec,
        a.estimated_effort,
        p.ssa_name,
        p.workload_status,
        p.planned_effort,
        p.primary_spec as ssa_spec,
        -- Calculate match score
        p.capacity_score +
        CASE
            WHEN a.technical_specialization = p.primary_spec THEN 20
            WHEN a.technical_specialization LIKE '%Governance%' AND p.primary_spec LIKE '%Governance%' THEN 15
            WHEN a.technical_specialization LIKE '%Engineering%' AND p.primary_spec LIKE '%Engineering%' THEN 15
            WHEN a.technical_specialization LIKE '%Science%' AND p.primary_spec LIKE '%Science%' THEN 15
            WHEN a.technical_specialization LIKE '%Analytics%' AND p.primary_spec LIKE '%Analytics%' THEN 15
            WHEN a.technical_specialization LIKE '%Geospatial%' AND p.primary_spec LIKE '%Geospatial%' THEN 20
            ELSE 0
        END as match_score,
        ROW_NUMBER() OVER (
            PARTITION BY a.asq_number
            ORDER BY
                p.capacity_score +
                CASE
                    WHEN a.technical_specialization = p.primary_spec THEN 20
                    WHEN a.technical_specialization LIKE '%Governance%' AND p.primary_spec LIKE '%Governance%' THEN 15
                    WHEN a.technical_specialization LIKE '%Engineering%' AND p.primary_spec LIKE '%Engineering%' THEN 15
                    WHEN a.technical_specialization LIKE '%Science%' AND p.primary_spec LIKE '%Science%' THEN 15
                    WHEN a.technical_specialization LIKE '%Analytics%' AND p.primary_spec LIKE '%Analytics%' THEN 15
                    WHEN a.technical_specialization LIKE '%Geospatial%' AND p.primary_spec LIKE '%Geospatial%' THEN 20
                    ELSE 0
                END DESC
        ) as rank
    FROM unassigned_asqs a
    CROSS JOIN ssa_profiles p
    WHERE p.workload_status != 'OVERLOADED'  -- Don't assign to overloaded SSAs
)

SELECT
    asq_number,
    account_name,
    asq_spec,
    estimated_effort,
    ssa_name as recommended_ssa,
    workload_status as ssa_status,
    planned_effort as ssa_current_effort,
    ssa_spec as ssa_primary_spec,
    match_score
FROM assignment_scores
WHERE rank = 1
ORDER BY match_score DESC, asq_number
