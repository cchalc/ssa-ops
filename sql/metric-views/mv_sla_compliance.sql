-- ============================================================================
-- mv_sla_compliance - SLA Milestone Tracking
-- ============================================================================
-- SLA performance metrics for ASQ workflows
-- Sources: GTM Silver tables on logfood (main catalog)
-- NO HARDCODED FILTERS - all BU/manager filtering at query time
-- ============================================================================

CREATE OR REPLACE METRIC VIEW ${catalog}.${schema}.mv_sla_compliance
COMMENT 'SLA milestone tracking for ASQs. Filter by business_unit, manager hierarchy, or time period. No hardcoded team filters.'
AS
SELECT
  -- ========================================================================
  -- ORGANIZATIONAL DIMENSIONS
  -- ========================================================================

  asq.business_unit
    COMMENT 'BU: AMER Enterprise & Emerging, AMER Industries, EMEA, APJ' AS `Business Unit`,
  asq.region_level_1
    COMMENT 'Region: CAN, RCT, FINS, etc.' AS `Region`,
  asq.owner_user_id
    COMMENT 'SSA owner user ID' AS `Owner ID`,
  asq.owner_user_name
    COMMENT 'SSA owner name' AS `Owner`,

  -- Manager Hierarchy
  hier.manager_level_1_name
    COMMENT 'Direct manager (L1)' AS `Manager L1`,
  hier.manager_level_1_id
    COMMENT 'Direct manager ID' AS `Manager L1 ID`,
  hier.manager_level_2_name
    COMMENT 'Second-level manager (L2)' AS `Manager L2`,

  -- ========================================================================
  -- CUSTOMER DIMENSIONS
  -- ========================================================================

  asq.account_id
    COMMENT 'Account ID' AS `Account ID`,
  asq.account_name
    COMMENT 'Customer account name' AS `Account`,

  -- ========================================================================
  -- WORK CLASSIFICATION DIMENSIONS
  -- ========================================================================

  asq.status
    COMMENT 'Current ASQ status' AS `ASQ Status`,
  asq.technical_specialization
    COMMENT 'Technical focus' AS `Specialization`,
  asq.support_type
    COMMENT 'Support type' AS `Support Type`,
  asq.priority
    COMMENT 'Request priority level' AS `Priority`,

  -- ========================================================================
  -- TIME DIMENSIONS
  -- ========================================================================

  YEAR(asq.created_date)
    COMMENT 'Calendar year ASQ was created' AS `Created Year`,
  CONCAT(YEAR(asq.created_date), '-Q', QUARTER(asq.created_date))
    COMMENT 'Calendar year-quarter' AS `Created Year-Quarter`,
  MONTH(asq.created_date)
    COMMENT 'Calendar month (1-12)' AS `Created Month`,
  DATE(asq.created_date)
    COMMENT 'Date ASQ was created' AS `Created Date`,

  -- Fiscal
  CASE WHEN MONTH(asq.created_date) = 1 THEN YEAR(asq.created_date)
       ELSE YEAR(asq.created_date) + 1 END
    COMMENT 'Fiscal year (ends Jan 31)' AS `Fiscal Year`,
  CASE
    WHEN MONTH(asq.created_date) IN (2, 3, 4) THEN 1
    WHEN MONTH(asq.created_date) IN (5, 6, 7) THEN 2
    WHEN MONTH(asq.created_date) IN (8, 9, 10) THEN 3
    ELSE 4
  END
    COMMENT 'Fiscal quarter (1-4)' AS `Fiscal Quarter`,

  -- ========================================================================
  -- SLA FLAG DIMENSIONS
  -- ========================================================================

  -- Assignment SLA (target: 2 business days)
  CASE WHEN asq.assigned_date IS NOT NULL
        AND DATEDIFF(asq.assigned_date, asq.created_date) <= 2
       THEN 'Met' ELSE 'Missed' END
    COMMENT 'Assignment SLA status (2 day target)' AS `Assignment SLA`,

  -- Response SLA (target: 5 business days to first contact)
  CASE WHEN asq.first_contact_date IS NOT NULL
        AND DATEDIFF(asq.first_contact_date, asq.created_date) <= 5
       THEN 'Met' ELSE 'Missed' END
    COMMENT 'Response SLA status (5 day target)' AS `Response SLA`,

  -- Completion SLA (by target_end_date)
  CASE WHEN asq.actual_completion_date IS NOT NULL
        AND asq.actual_completion_date <= asq.target_end_date
       THEN 'Met' ELSE 'Missed' END
    COMMENT 'Completion SLA status (by due date)' AS `Completion SLA`,

  -- ========================================================================
  -- SLA VOLUME MEASURES
  -- ========================================================================

  COUNT(1)
    COMMENT 'Total number of ASQs' AS `Total ASQs`,

  -- Assignment SLA
  COUNT(1) FILTER (WHERE asq.assigned_date IS NOT NULL
                     AND DATEDIFF(asq.assigned_date, asq.created_date) <= 2)
    COMMENT 'ASQs assigned within 2 days' AS `Assignment SLA Met`,
  COUNT(1) FILTER (WHERE asq.assigned_date IS NOT NULL
                     AND DATEDIFF(asq.assigned_date, asq.created_date) > 2)
    COMMENT 'ASQs assigned after 2 days' AS `Assignment SLA Missed`,
  COUNT(1) FILTER (WHERE asq.assigned_date IS NULL)
    COMMENT 'ASQs not yet assigned' AS `Unassigned ASQs`,

  -- Response SLA
  COUNT(1) FILTER (WHERE asq.first_contact_date IS NOT NULL
                     AND DATEDIFF(asq.first_contact_date, asq.created_date) <= 5)
    COMMENT 'ASQs with first contact within 5 days' AS `Response SLA Met`,
  COUNT(1) FILTER (WHERE asq.first_contact_date IS NOT NULL
                     AND DATEDIFF(asq.first_contact_date, asq.created_date) > 5)
    COMMENT 'ASQs with first contact after 5 days' AS `Response SLA Missed`,

  -- Completion SLA
  COUNT(1) FILTER (WHERE asq.actual_completion_date IS NOT NULL
                     AND asq.actual_completion_date <= asq.target_end_date)
    COMMENT 'Completed on or before due date' AS `Completion SLA Met`,
  COUNT(1) FILTER (WHERE asq.actual_completion_date IS NOT NULL
                     AND asq.actual_completion_date > asq.target_end_date)
    COMMENT 'Completed after due date' AS `Completion SLA Missed`,

  -- ========================================================================
  -- SLA RATE MEASURES
  -- ========================================================================

  COUNT(1) FILTER (WHERE asq.assigned_date IS NOT NULL
                     AND DATEDIFF(asq.assigned_date, asq.created_date) <= 2) * 1.0
    / NULLIF(COUNT(1) FILTER (WHERE asq.assigned_date IS NOT NULL), 0)
    COMMENT 'Percentage of ASQs assigned within SLA' AS `Assignment SLA Rate`,

  COUNT(1) FILTER (WHERE asq.first_contact_date IS NOT NULL
                     AND DATEDIFF(asq.first_contact_date, asq.created_date) <= 5) * 1.0
    / NULLIF(COUNT(1) FILTER (WHERE asq.first_contact_date IS NOT NULL), 0)
    COMMENT 'Percentage of ASQs with timely first contact' AS `Response SLA Rate`,

  COUNT(1) FILTER (WHERE asq.actual_completion_date IS NOT NULL
                     AND asq.actual_completion_date <= asq.target_end_date) * 1.0
    / NULLIF(COUNT(1) FILTER (WHERE asq.actual_completion_date IS NOT NULL), 0)
    COMMENT 'Percentage of ASQs completed on time' AS `Completion SLA Rate`,

  -- Perfect SLA (all milestones met)
  COUNT(1) FILTER (
    WHERE asq.assigned_date IS NOT NULL AND DATEDIFF(asq.assigned_date, asq.created_date) <= 2
      AND asq.first_contact_date IS NOT NULL AND DATEDIFF(asq.first_contact_date, asq.created_date) <= 5
      AND asq.actual_completion_date IS NOT NULL AND asq.actual_completion_date <= asq.target_end_date
  ) * 1.0 / NULLIF(COUNT(1) FILTER (WHERE asq.actual_completion_date IS NOT NULL), 0)
    COMMENT 'Percentage of ASQs meeting all SLA milestones' AS `Perfect SLA Rate`,

  -- ========================================================================
  -- SLA TIMING MEASURES
  -- ========================================================================

  AVG(DATEDIFF(asq.assigned_date, asq.created_date))
    COMMENT 'Average days from creation to assignment' AS `Avg Days to Assignment`,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY DATEDIFF(asq.assigned_date, asq.created_date))
    COMMENT 'Median days to assignment' AS `Median Days to Assignment`,
  PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY DATEDIFF(asq.assigned_date, asq.created_date))
    COMMENT '90th percentile days to assignment' AS `P90 Days to Assignment`,

  AVG(DATEDIFF(asq.first_contact_date, asq.created_date))
    COMMENT 'Average days from creation to first contact' AS `Avg Days to Response`,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY DATEDIFF(asq.first_contact_date, asq.created_date))
    COMMENT 'Median days to first contact' AS `Median Days to Response`,

  AVG(DATEDIFF(asq.actual_completion_date, asq.created_date))
    COMMENT 'Average days from creation to completion' AS `Avg Days to Complete`,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY DATEDIFF(asq.actual_completion_date, asq.created_date))
    COMMENT 'Median days to completion' AS `Median Days to Complete`,
  PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY DATEDIFF(asq.actual_completion_date, asq.created_date))
    COMMENT '90th percentile days to completion' AS `P90 Days to Complete`,

  -- ========================================================================
  -- SLA VARIANCE MEASURES
  -- ========================================================================

  AVG(DATEDIFF(asq.actual_completion_date, asq.target_end_date))
    COMMENT 'Average days early/late (negative = early)' AS `Avg Days Variance`,
  AVG(DATEDIFF(asq.actual_completion_date, asq.target_end_date))
    FILTER (WHERE asq.actual_completion_date > asq.target_end_date)
    COMMENT 'Average days late for late completions' AS `Avg Days Late`

FROM main.gtm_silver.approval_request_detail asq
LEFT JOIN main.gtm_silver.individual_hierarchy_field hier
  ON asq.owner_user_id = hier.user_id
WHERE asq.snapshot_date = (
  SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail
);

-- ============================================================================
-- Sample Queries
-- ============================================================================
--
-- Team SLA Performance:
-- SELECT `Manager L1`, MEASURE(`Completion SLA Rate`), MEASURE(`Perfect SLA Rate`)
-- FROM mv_sla_compliance
-- WHERE `Business Unit` = 'AMER Enterprise & Emerging'
-- GROUP BY ALL;
--
-- SLA Trend by Month:
-- SELECT `Created Year-Quarter`, `Created Month`,
--        MEASURE(`Assignment SLA Rate`), MEASURE(`Completion SLA Rate`)
-- FROM mv_sla_compliance
-- WHERE `Fiscal Year` = 2026
-- GROUP BY ALL
-- ORDER BY `Created Year-Quarter`, `Created Month`;
--
-- Priority Impact on SLA:
-- SELECT `Priority`, MEASURE(`Total ASQs`), MEASURE(`Completion SLA Rate`)
-- FROM mv_sla_compliance
-- GROUP BY ALL;
