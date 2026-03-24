-- ============================================================================
-- mv_time_to_adopt - UCO Stage Acceleration (U3→U4 Transition Time)
-- ============================================================================
-- Charter Metric #4: Time-to-Adopt
-- Measures how quickly SSA-engaged UCOs move from Evaluating (U3) to Tech Win (U4)
-- This is the primary SSA impact zone - accelerating technical validation
-- Uses direct stage date columns from use_case_detail
-- Sources: GTM Silver (approval_request_detail, use_case_detail, individual_hierarchy)
-- NO HARDCODED FILTERS - all BU/manager filtering at query time
-- ============================================================================

CREATE OR REPLACE METRIC VIEW home_christopher_chalcraft.cjc_views.mv_time_to_adopt
COMMENT 'UCO stage acceleration metrics. Charter Metric #4: Time-to-Adopt. Measures U3→U4 transition time for SSA-engaged use cases.'
AS
SELECT
  -- ========================================================================
  -- ORGANIZATIONAL DIMENSIONS
  -- ========================================================================

  asq.business_unit
    COMMENT 'BU: AMER Enterprise & Emerging, AMER Industries, EMEA, APJ' AS `Business Unit`,
  asq.region_level_1
    COMMENT 'Region: CAN, RCT, FINS, etc.' AS `Region`,
  asq.owner_user_name
    COMMENT 'SSA owner name' AS `Owner`,

  -- Manager Hierarchy
  hier.line_manager_name
    COMMENT 'Direct manager (L1)' AS `Manager L1`,
  hier.`2nd_line_manager_name`
    COMMENT 'Second-level manager (L2)' AS `Manager L2`,

  -- ========================================================================
  -- CUSTOMER DIMENSIONS
  -- ========================================================================

  asq.account_name
    COMMENT 'Customer account name' AS `Account`,

  -- ========================================================================
  -- UCO CLASSIFICATION DIMENSIONS
  -- ========================================================================

  uco.stage
    COMMENT 'Current UCO stage' AS `Current Stage`,
  uco.use_case_product
    COMMENT 'Product category' AS `Product Category`,

  -- Adoption Speed Tier (based on U3→U4 days)
  CASE
    WHEN uco.u4_date_sfdc_original IS NULL THEN 'Not Yet Adopted'
    WHEN uco.u3_date_sfdc_original IS NULL THEN 'No U3 Date'
    WHEN DATEDIFF(uco.u4_date_sfdc_original, uco.u3_date_sfdc_original) <= 14 THEN 'Fast (≤14 days)'
    WHEN DATEDIFF(uco.u4_date_sfdc_original, uco.u3_date_sfdc_original) <= 30 THEN 'Normal (15-30 days)'
    WHEN DATEDIFF(uco.u4_date_sfdc_original, uco.u3_date_sfdc_original) <= 60 THEN 'Slow (31-60 days)'
    ELSE 'Very Slow (>60 days)'
  END
    COMMENT 'Adoption speed tier based on U3→U4 days' AS `Adoption Speed`,

  -- ========================================================================
  -- TIME DIMENSIONS
  -- ========================================================================

  YEAR(asq.created_date)
    COMMENT 'Calendar year' AS `Created Year`,
  CONCAT(YEAR(asq.created_date), '-Q', QUARTER(asq.created_date))
    COMMENT 'Calendar year-quarter' AS `Created Year-Quarter`,

  -- Fiscal
  CASE WHEN MONTH(asq.created_date) = 1 THEN YEAR(asq.created_date)
       ELSE YEAR(asq.created_date) + 1 END
    COMMENT 'Fiscal year (ends Jan 31)' AS `Fiscal Year`,

  -- ========================================================================
  -- ADOPTION VOLUME MEASURES
  -- ========================================================================

  COUNT(DISTINCT uco.usecase_id)
    COMMENT 'Total UCOs linked to ASQs' AS `Total UCOs`,

  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.u4_date_sfdc_original IS NOT NULL)
    COMMENT 'UCOs that reached U4 (tech win)' AS `Adopted UCOs`,

  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.stage IN ('U5', 'U6'))
    COMMENT 'UCOs at production (U5+)' AS `Production UCOs`,

  -- ========================================================================
  -- TIME-TO-ADOPT MEASURES (U3→U4)
  -- ========================================================================

  AVG(DATEDIFF(uco.u4_date_sfdc_original, uco.u3_date_sfdc_original))
    COMMENT 'Average days from U3 (evaluating) to U4 (tech win)' AS `Avg Days to Adopt`,

  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY DATEDIFF(uco.u4_date_sfdc_original, uco.u3_date_sfdc_original))
    COMMENT 'Median days U3→U4' AS `Median Days to Adopt`,

  PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY DATEDIFF(uco.u4_date_sfdc_original, uco.u3_date_sfdc_original))
    COMMENT '25th percentile (fast adopters)' AS `P25 Days to Adopt`,

  PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY DATEDIFF(uco.u4_date_sfdc_original, uco.u3_date_sfdc_original))
    COMMENT '90th percentile (slow adopters)' AS `P90 Days to Adopt`,

  MIN(DATEDIFF(uco.u4_date_sfdc_original, uco.u3_date_sfdc_original))
    COMMENT 'Fastest adoption' AS `Min Days to Adopt`,

  MAX(DATEDIFF(uco.u4_date_sfdc_original, uco.u3_date_sfdc_original))
    COMMENT 'Slowest adoption' AS `Max Days to Adopt`,

  -- ========================================================================
  -- FULL PATH MEASURES (U3→U5, U4→U5)
  -- ========================================================================

  AVG(DATEDIFF(uco.u5_date_sfdc_original, uco.u3_date_sfdc_original))
    COMMENT 'Average days from U3 to U5 (full adoption path)' AS `Avg Days U3 to Production`,

  AVG(DATEDIFF(uco.u5_date_sfdc_original, uco.u4_date_sfdc_original))
    COMMENT 'Average days from U4 to U5 (implementation time)' AS `Avg Days U4 to Production`,

  -- ========================================================================
  -- ADOPTION RATES
  -- ========================================================================

  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.u4_date_sfdc_original IS NOT NULL) * 1.0
    / NULLIF(COUNT(DISTINCT uco.usecase_id), 0)
    COMMENT 'Percentage of UCOs that reached tech win (U4)' AS `Adoption Rate`,

  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE
    uco.u3_date_sfdc_original IS NOT NULL
    AND uco.u4_date_sfdc_original IS NOT NULL
    AND DATEDIFF(uco.u4_date_sfdc_original, uco.u3_date_sfdc_original) <= 14) * 1.0
    / NULLIF(COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.u4_date_sfdc_original IS NOT NULL), 0)
    COMMENT 'Percentage of adoptions that were fast (≤14 days)' AS `Fast Adoption Rate`,

  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE
    uco.u3_date_sfdc_original IS NOT NULL
    AND uco.u4_date_sfdc_original IS NOT NULL
    AND DATEDIFF(uco.u4_date_sfdc_original, uco.u3_date_sfdc_original) > 60) * 1.0
    / NULLIF(COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.u4_date_sfdc_original IS NOT NULL), 0)
    COMMENT 'Percentage of adoptions that were slow (>60 days)' AS `Slow Adoption Rate`,

  -- ========================================================================
  -- ACCELERATION MEASURES
  -- ========================================================================

  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE
    uco.u3_date_sfdc_original IS NOT NULL
    AND uco.u4_date_sfdc_original IS NOT NULL
    AND DATEDIFF(uco.u4_date_sfdc_original, uco.u3_date_sfdc_original) <= 14)
    COMMENT 'Fast adoptions (≤14 days)' AS `Fast Adoptions`,

  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE
    uco.u3_date_sfdc_original IS NOT NULL
    AND uco.u4_date_sfdc_original IS NOT NULL
    AND DATEDIFF(uco.u4_date_sfdc_original, uco.u3_date_sfdc_original) BETWEEN 15 AND 30)
    COMMENT 'Normal adoptions (15-30 days)' AS `Normal Adoptions`,

  COUNT(DISTINCT uco.usecase_id) FILTER (WHERE
    uco.u3_date_sfdc_original IS NOT NULL
    AND uco.u4_date_sfdc_original IS NOT NULL
    AND DATEDIFF(uco.u4_date_sfdc_original, uco.u3_date_sfdc_original) > 60)
    COMMENT 'Slow adoptions (>60 days)' AS `Slow Adoptions`

FROM main.gtm_silver.approval_request_detail asq
LEFT JOIN main.gtm_silver.individual_hierarchy_salesforce hier
  ON asq.owner_user_id = hier.user_id
LEFT JOIN main.gtm_silver.use_case_detail uco
  ON asq.account_id = uco.account_id
  AND uco.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.use_case_detail)
  AND uco.is_active_ind = true
WHERE asq.snapshot_date = (
  SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail
)
  AND uco.usecase_id IS NOT NULL;

-- ============================================================================
-- Sample Queries
-- ============================================================================
--
-- Adoption Speed by Team:
-- SELECT `Manager L1`, MEASURE(`Total UCOs`), MEASURE(`Adopted UCOs`),
--        MEASURE(`Avg Days to Adopt`), MEASURE(`Fast Adoption Rate`)
-- FROM mv_time_to_adopt
-- WHERE `Business Unit` = 'AMER Enterprise & Emerging'
-- GROUP BY ALL;
--
-- Adoption Speed by Product:
-- SELECT `Product Category`, MEASURE(`Total UCOs`), MEASURE(`Avg Days to Adopt`),
--        MEASURE(`Adoption Rate`), MEASURE(`Fast Adoptions`)
-- FROM mv_time_to_adopt
-- GROUP BY ALL
-- ORDER BY `Avg Days to Adopt`;
--
-- SSA Adoption Leaderboard:
-- SELECT `Owner`, MEASURE(`Adopted UCOs`), MEASURE(`Avg Days to Adopt`),
--        MEASURE(`Fast Adoption Rate`)
-- FROM mv_time_to_adopt
-- WHERE `Fiscal Year` = 2026
-- GROUP BY ALL
-- ORDER BY `Avg Days to Adopt`;
