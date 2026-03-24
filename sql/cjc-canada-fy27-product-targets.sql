-- cjc-canada-fy27-product-targets
-- Canada BU FY27 Product-Level Targets (DWH, Lakebase, AI)
-- Source: gtm_silver.targets_account
-- Author: Chris Chalcraft
-- Created: 2026-03-24

-- FY27 Product Targets for Canada BU
WITH canada_targets AS (
  SELECT
    account_id,
    account_name,
    -- DWH Target
    COALESCE(dbu_dollar_dwh_target, 0) as dwh_target,
    -- Lakebase Target (formerly Lakeflow)
    COALESCE(dbu_dollar_lakebase_target, 0) as lakebase_target,
    -- AI Targets (broken into components)
    COALESCE(dbu_dollar_ai_bi_target, 0) as ai_bi_target,
    COALESCE(dbu_dollar_agent_bricks_target, 0) as agent_bricks_target,
    COALESCE(dbu_dollar_fmapi_target, 0) as fmapi_target,
    -- Total AI
    COALESCE(dbu_dollar_ai_bi_target, 0) +
    COALESCE(dbu_dollar_agent_bricks_target, 0) +
    COALESCE(dbu_dollar_fmapi_target, 0) as total_ai_target
  FROM main.gtm_silver.targets_account
  WHERE LOWER(region) = 'can'
     OR LOWER(sub_bu) = 'can'
     OR LOWER(sfdc_region_l1) = 'can'
)

SELECT
  'DWH' as product,
  SUM(dwh_target) as fy27_target,
  COUNT(DISTINCT account_id) as account_count
FROM canada_targets
WHERE dwh_target > 0

UNION ALL

SELECT
  'Lakebase' as product,
  SUM(lakebase_target) as fy27_target,
  COUNT(DISTINCT account_id) as account_count
FROM canada_targets
WHERE lakebase_target > 0

UNION ALL

SELECT
  'AI (Total)' as product,
  SUM(total_ai_target) as fy27_target,
  COUNT(DISTINCT account_id) as account_count
FROM canada_targets
WHERE total_ai_target > 0

UNION ALL

SELECT
  'AI - AI/BI' as product,
  SUM(ai_bi_target) as fy27_target,
  COUNT(DISTINCT account_id) as account_count
FROM canada_targets
WHERE ai_bi_target > 0

UNION ALL

SELECT
  'AI - Agent Bricks' as product,
  SUM(agent_bricks_target) as fy27_target,
  COUNT(DISTINCT account_id) as account_count
FROM canada_targets
WHERE agent_bricks_target > 0

UNION ALL

SELECT
  'AI - FMAPI' as product,
  SUM(fmapi_target) as fy27_target,
  COUNT(DISTINCT account_id) as account_count
FROM canada_targets
WHERE fmapi_target > 0

ORDER BY
  CASE product
    WHEN 'DWH' THEN 1
    WHEN 'Lakebase' THEN 2
    WHEN 'AI (Total)' THEN 3
    WHEN 'AI - AI/BI' THEN 4
    WHEN 'AI - Agent Bricks' THEN 5
    WHEN 'AI - FMAPI' THEN 6
  END;
