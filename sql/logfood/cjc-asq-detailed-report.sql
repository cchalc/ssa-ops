-- ASQ Detailed Report Query
-- Generates comprehensive ASQ data with descriptions, notes, UCO linkage, and charter scoring
-- Usage: Replace {{ region }} with region code (e.g., 'CAN')

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

-- Get ASQs with full details
asq_details AS (
    SELECT
        a.approval_request_id,
        a.approval_request_name as asq_number,
        a.account_name,
        a.account_id,
        a.owner_user_name,
        a.owner_user_id,
        a.status,
        a.technical_specialization,
        a.support_type,
        COALESCE(a.estimated_effort_in_days, 5) as estimated_effort,
        a.created_date,
        a.target_end_date,
        DATEDIFF(CURRENT_DATE, a.created_date) as days_open,
        DATEDIFF(CURRENT_DATE, a.target_end_date) as days_overdue,
        -- Description and notes
        a.request_description,
        a.request_status_notes,
        a.last_reviewer_comment,
        a.additional_notes_or_doc_links,
        -- Salesforce link
        CONCAT('https://databricks.lightning.force.com/', a.approval_request_id) as sf_link,
        -- Hygiene status
        CASE
            WHEN DATEDIFF(CURRENT_DATE, a.created_date) > 90 THEN 'RULE5_EXCESSIVE'
            WHEN a.target_end_date IS NOT NULL AND DATEDIFF(CURRENT_DATE, a.target_end_date) > 7 THEN 'RULE4_EXPIRED'
            WHEN DATEDIFF(CURRENT_DATE, a.created_date) BETWEEN 30 AND 90
                 AND (a.target_end_date IS NULL OR a.target_end_date < CURRENT_DATE) THEN 'RULE3_STALE'
            WHEN DATEDIFF(CURRENT_DATE, a.created_date) > 7
                 AND (a.request_status_notes IS NULL OR LENGTH(a.request_status_notes) < 10) THEN 'RULE1_MISSING_NOTES'
            ELSE 'COMPLIANT'
        END as hygiene_status
    FROM main.gtm_silver.approval_request_detail a
    WHERE a.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
      AND a.region_level_1 = '{{ region }}'
      AND a.status NOT IN ('Complete', 'Rejected', 'Withdrawn')
),

-- UCO linkage with competitive and stage info
uco_info AS (
    SELECT
        u.account_id,
        MAX(CASE WHEN u.stage = 'U6' THEN 1 ELSE 0 END) as has_production,
        MAX(CASE WHEN u.stage IN ('U4', 'U5') THEN 1 ELSE 0 END) as has_near_win,
        MAX(CASE WHEN u.competitors IS NOT NULL AND u.competitors != 'No Competitor' THEN 1 ELSE 0 END) as is_competitive,
        MAX(u.competitors) as competitors,
        MAX(TRY_CAST(u.monthly_total_dollar_dbus AS DOUBLE)) as max_dbu,
        MAX(u.stage) as best_stage
    FROM main.gtm_silver.use_case_detail u
    WHERE u.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.use_case_detail)
      AND u.stage IN ('U3', 'U4', 'U5', 'U6')
    GROUP BY u.account_id
),

-- Account history with SSAs (for continuity matching) - get best SSA per account
account_history AS (
    SELECT account_id, owner_user_name as prior_ssa, prior_engagements
    FROM (
        SELECT
            account_id,
            owner_user_name,
            COUNT(*) as prior_engagements,
            ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY COUNT(*) DESC) as rn
        FROM main.gtm_silver.approval_request_detail
        WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
          AND owner_user_id IN (SELECT user_id FROM direct_reports)
          AND status = 'Complete'
        GROUP BY account_id, owner_user_name
    )
    WHERE rn = 1
)

SELECT
    a.asq_number,
    a.sf_link,
    a.account_name,
    a.owner_user_name,
    a.status,
    a.technical_specialization,
    a.support_type,
    a.days_open,
    a.days_overdue,
    a.hygiene_status,
    a.estimated_effort,
    -- UCO info
    u.best_stage as uco_stage,
    u.has_production,
    u.has_near_win,
    u.is_competitive,
    u.competitors,
    u.max_dbu,
    -- Charter score calculation
    COALESCE(u.has_production, 0) * 15 +
    COALESCE(u.has_near_win, 0) * 10 +
    COALESCE(u.is_competitive, 0) * 10 +
    CASE WHEN u.max_dbu > 100000 THEN 20
         WHEN u.max_dbu > 50000 THEN 10
         WHEN u.max_dbu > 10000 THEN 5
         ELSE 0 END +
    CASE WHEN a.days_open > 90 THEN -10 ELSE 0 END as charter_score,
    -- Charter alignment
    CASE
        WHEN COALESCE(u.has_production, 0) * 15 + COALESCE(u.has_near_win, 0) * 10 + COALESCE(u.is_competitive, 0) * 10 >= 20 THEN 'HIGHLY_ALIGNED'
        WHEN COALESCE(u.has_production, 0) * 15 + COALESCE(u.has_near_win, 0) * 10 + COALESCE(u.is_competitive, 0) * 10 >= 10 THEN 'ALIGNED'
        WHEN COALESCE(u.has_production, 0) * 15 + COALESCE(u.has_near_win, 0) * 10 + COALESCE(u.is_competitive, 0) * 10 >= 0 THEN 'NEUTRAL'
        ELSE 'MISALIGNED'
    END as charter_alignment,
    -- Description and notes
    SUBSTRING(a.request_description, 1, 500) as description_preview,
    a.request_description as full_description,
    a.request_status_notes,
    a.last_reviewer_comment,
    -- Account continuity
    ah.prior_ssa,
    ah.prior_engagements,
    -- Best SSA match for unassigned
    CASE
        WHEN a.owner_user_name IS NOT NULL THEN a.owner_user_name
        WHEN ah.prior_ssa IS NOT NULL THEN ah.prior_ssa  -- Account continuity
        WHEN a.technical_specialization LIKE '%Geospatial%' THEN 'Mathieu Pelletier'
        WHEN a.technical_specialization LIKE '%Governance%' THEN 'Allan Cao'
        WHEN a.technical_specialization LIKE '%Engineering%' THEN 'Harsha Pasala'
        WHEN a.technical_specialization LIKE '%Science%' OR a.technical_specialization LIKE '%Machine%' THEN 'Volodymyr Vragov'
        WHEN a.technical_specialization LIKE '%Analytics%' THEN 'Allan Cao'
        ELSE 'Harsha Pasala'  -- Default to lightest load
    END as recommended_ssa
FROM asq_details a
LEFT JOIN uco_info u ON a.account_id = u.account_id
LEFT JOIN account_history ah ON a.account_id = ah.account_id
ORDER BY
    CASE WHEN a.owner_user_name IS NULL THEN 0 ELSE 1 END,  -- Unassigned first
    COALESCE(u.has_production, 0) * 15 + COALESCE(u.has_near_win, 0) * 10 + COALESCE(u.is_competitive, 0) * 10 DESC,
    a.days_open DESC
