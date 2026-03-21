-- cjc-asq-team-capacity
-- Team capacity and workload distribution by SSA
-- Parameters: {{ manager_id }} (e.g., 0053f000000pKoTAAU)
--
-- Shows workload distribution across team members:
-- - Total open ASQs per person
-- - Estimated effort days
-- - Overdue count
-- - Workload classification (HEAVY/MODERATE/LIGHT)

WITH team_members AS (
  SELECT DISTINCT user_id, user_name
  FROM main.gtm_silver.individual_hierarchy_field
  WHERE manager_level_1_id = '{{ manager_id }}'
),
asq_workload AS (
  SELECT
    asq.owner_user_id,
    asq.owner_user_name,
    asq.approval_request_name,
    asq.status,
    asq.region_level_1,
    COALESCE(asq.estimated_effort_in_days, 5) AS effort_days,
    CASE
      WHEN asq.target_end_date < CURRENT_DATE THEN 1
      ELSE 0
    END AS is_overdue,
    CASE
      WHEN asq.target_end_date < CURRENT_DATE - INTERVAL 14 DAYS THEN 'CRITICAL'
      WHEN asq.target_end_date < CURRENT_DATE - INTERVAL 7 DAYS THEN 'HIGH'
      WHEN asq.target_end_date < CURRENT_DATE THEN 'MEDIUM'
      ELSE 'NORMAL'
    END AS urgency
  FROM main.gtm_silver.approval_request_detail asq
  JOIN team_members tm ON asq.owner_user_id = tm.user_id
  WHERE asq.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
    AND asq.status IN ('New', 'Submitted', 'Under Review', 'In Progress')
)
SELECT
  owner_user_name AS ssa,
  COUNT(*) AS total_asqs,
  SUM(effort_days) AS total_effort_days,
  SUM(is_overdue) AS overdue_count,
  SUM(CASE WHEN urgency = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_count,
  SUM(CASE WHEN urgency = 'HIGH' THEN 1 ELSE 0 END) AS high_count,
  ROUND(AVG(effort_days), 1) AS avg_effort_days,

  -- Workload classification
  CASE
    WHEN COUNT(*) > 10 OR SUM(effort_days) > 50 THEN 'HEAVY'
    WHEN COUNT(*) > 5 OR SUM(effort_days) > 25 THEN 'MODERATE'
    ELSE 'LIGHT'
  END AS workload,

  -- Region distribution
  CONCAT_WS(', ', COLLECT_SET(region_level_1)) AS regions

FROM asq_workload
GROUP BY owner_user_id, owner_user_name
ORDER BY
  SUM(is_overdue) DESC,
  COUNT(*) DESC
