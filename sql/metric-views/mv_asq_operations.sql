-- ============================================================================
-- mv_asq_operations - Core ASQ Operational Metrics
-- ============================================================================
-- Primary metric view with 30+ measures across all organizational dimensions
-- Sources: GTM Silver/Gold tables on logfood (main catalog)
-- NO HARDCODED FILTERS - all BU/manager filtering at query time
-- ============================================================================

CREATE OR REPLACE METRIC VIEW ${catalog}.${schema}.mv_asq_operations
COMMENT 'Core ASQ operational metrics. Filter by business_unit, manager hierarchy, or any dimension at query time. No hardcoded team filters.'
AS
SELECT
  -- ========================================================================
  -- ORGANIZATIONAL DIMENSIONS
  -- ========================================================================

  -- Business Unit / Geographic
  asq.business_unit
    COMMENT 'BU: AMER Enterprise & Emerging, AMER Industries, EMEA, APJ' AS `Business Unit`,
  asq.region_level_1
    COMMENT 'Region: CAN, RCT, FINS, etc.' AS `Region`,
  asq.region_level_2
    COMMENT 'Sub-region' AS `Sub-Region`,

  -- SSA Owner
  asq.owner_user_id
    COMMENT 'SSA owner user ID' AS `Owner ID`,
  asq.owner_user_name
    COMMENT 'SSA owner name' AS `Owner`,

  -- Manager Hierarchy (from individual_hierarchy_field)
  hier.manager_level_1_name
    COMMENT 'Direct manager (L1)' AS `Manager L1`,
  hier.manager_level_1_id
    COMMENT 'Direct manager ID' AS `Manager L1 ID`,
  hier.manager_level_2_name
    COMMENT 'Second-level manager (L2)' AS `Manager L2`,
  hier.manager_level_2_id
    COMMENT 'Second-level manager ID' AS `Manager L2 ID`,
  hier.manager_level_3_name
    COMMENT 'Third-level manager (L3)' AS `Manager L3`,
  hier.manager_level_4_name
    COMMENT 'Fourth-level manager (L4)' AS `Manager L4`,
  hier.manager_level_5_name
    COMMENT 'Fifth-level manager (L5)' AS `Manager L5`,

  -- ========================================================================
  -- CUSTOMER DIMENSIONS
  -- ========================================================================

  asq.account_id
    COMMENT 'Account ID' AS `Account ID`,
  asq.account_name
    COMMENT 'Customer account name' AS `Account`,
  ao.segment
    COMMENT 'Customer segment: ENT, COMM, MM, SMB' AS `Account Segment`,
  ao.vertical
    COMMENT 'Industry vertical' AS `Account Vertical`,
  ao.spend_tier
    COMMENT 'Account spend tier' AS `Spend Tier`,

  -- ========================================================================
  -- WORK CLASSIFICATION DIMENSIONS
  -- ========================================================================

  asq.status
    COMMENT 'Current ASQ status' AS `ASQ Status`,
  -- Status category mapping (handles 30+ actual status values)
  CASE
    WHEN asq.status IN ('New', 'Assigned', 'In Progress', 'Under Review', 'On Hold',
                        'Unassigned', 'Review In Progress', 'Review Pending',
                        '1. Briefing complete', '2. Actively engaged',
                        'Additional Details Required', 'Pending', 'Draft', 'Pending Confirmation')
    THEN 'Open'
    WHEN asq.status IN ('Complete', 'Closed', 'Completed', 'Rejected', 'Cancelled',
                        '3. Delivered', '4. Engagement lost', 'Relegated', 'Delivered', '0. Not engaged')
    THEN 'Closed'
    WHEN asq.status IN ('Approved', 'Approved. Project creation in progress',
                        'Pilot Approved', 'Manually Triggered', 'Submitted')
    THEN 'Approved'
    WHEN asq.status IN ('5. On hold')
    THEN 'On Hold'
    WHEN asq.status IS NULL OR asq.status = 'None'
    THEN 'Unknown'
    ELSE 'Other'
  END
    COMMENT 'Status category: Open, Closed, Approved, On Hold, Other' AS `Status Category`,
  -- Overdue flag (only for truly open statuses)
  CASE WHEN asq.status IN ('New', 'Assigned', 'In Progress', 'Under Review', 'On Hold',
                           'Unassigned', 'Review In Progress', 'Review Pending',
                           '1. Briefing complete', '2. Actively engaged',
                           'Additional Details Required', 'Pending', 'Draft')
        AND asq.target_end_date < CURRENT_DATE()
       THEN 'Overdue' ELSE 'On Track' END
    COMMENT 'Overdue status flag' AS `Is Overdue`,
  -- At Risk flag (due within 3 days)
  CASE WHEN asq.status IN ('New', 'Assigned', 'In Progress', 'Under Review', 'On Hold',
                           'Unassigned', 'Review In Progress', 'Review Pending',
                           '1. Briefing complete', '2. Actively engaged',
                           'Additional Details Required', 'Pending', 'Draft')
        AND asq.target_end_date BETWEEN CURRENT_DATE() AND DATE_ADD(CURRENT_DATE(), 3)
       THEN 'At Risk' ELSE 'OK' END
    COMMENT 'At risk (due within 3 days)' AS `Is At Risk`,
  asq.technical_specialization
    COMMENT 'Technical focus: Data Science, Data Engineering, Platform, etc.' AS `Specialization`,
  asq.support_type
    COMMENT 'Support type: Platform Administration, Production Architecture Review, etc.' AS `Support Type`,
  asq.priority
    COMMENT 'Request priority level' AS `Priority`,

  -- ========================================================================
  -- TIME DIMENSIONS (Calendar)
  -- ========================================================================

  YEAR(asq.created_date)
    COMMENT 'Calendar year ASQ was created' AS `Created Year`,
  CONCAT(YEAR(asq.created_date), '-Q', QUARTER(asq.created_date))
    COMMENT 'Calendar year-quarter' AS `Created Year-Quarter`,
  QUARTER(asq.created_date)
    COMMENT 'Calendar quarter (1-4)' AS `Created Quarter`,
  MONTH(asq.created_date)
    COMMENT 'Calendar month (1-12)' AS `Created Month`,
  WEEKOFYEAR(asq.created_date)
    COMMENT 'Calendar week of year' AS `Created Week`,
  DATE(asq.created_date)
    COMMENT 'Date ASQ was created' AS `Created Date`,
  DATE_TRUNC('MONTH', asq.created_date)
    COMMENT 'First day of creation month' AS `Created Month Start`,
  DATE_TRUNC('WEEK', asq.created_date)
    COMMENT 'First day of creation week' AS `Created Week Start`,

  -- ========================================================================
  -- TIME DIMENSIONS (Fiscal - FY ends Jan 31)
  -- ========================================================================

  -- Databricks fiscal year: FY ends Jan 31
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
  -- VOLUME MEASURES
  -- ========================================================================

  COUNT(1)
    COMMENT 'Total number of ASQs' AS `Total ASQs`,
  -- Open ASQs (all open statuses)
  COUNT(1) FILTER (WHERE asq.status IN ('New', 'Assigned', 'In Progress', 'Under Review', 'On Hold',
                                        'Unassigned', 'Review In Progress', 'Review Pending',
                                        '1. Briefing complete', '2. Actively engaged',
                                        'Additional Details Required', 'Pending', 'Draft', 'Pending Confirmation'))
    COMMENT 'Currently open ASQs' AS `Open ASQs`,
  -- Closed ASQs (all closed statuses)
  COUNT(1) FILTER (WHERE asq.status IN ('Complete', 'Closed', 'Completed', 'Rejected', 'Cancelled',
                                        '3. Delivered', '4. Engagement lost', 'Relegated', 'Delivered', '0. Not engaged'))
    COMMENT 'Completed or closed ASQs' AS `Closed ASQs`,
  -- Approved ASQs
  COUNT(1) FILTER (WHERE asq.status IN ('Approved', 'Approved. Project creation in progress',
                                        'Pilot Approved', 'Manually Triggered', 'Submitted'))
    COMMENT 'Approved ASQs awaiting action' AS `Approved ASQs`,
  -- Overdue (open and past due date)
  COUNT(1) FILTER (WHERE asq.status IN ('New', 'Assigned', 'In Progress', 'Under Review', 'On Hold',
                                        'Unassigned', 'Review In Progress', 'Review Pending',
                                        '1. Briefing complete', '2. Actively engaged',
                                        'Additional Details Required', 'Pending', 'Draft')
                     AND asq.target_end_date < CURRENT_DATE())
    COMMENT 'Past due date, not completed' AS `Overdue ASQs`,
  -- At Risk (open and due within 3 days)
  COUNT(1) FILTER (WHERE asq.status IN ('New', 'Assigned', 'In Progress', 'Under Review', 'On Hold',
                                        'Unassigned', 'Review In Progress', 'Review Pending',
                                        '1. Briefing complete', '2. Actively engaged',
                                        'Additional Details Required', 'Pending', 'Draft')
                     AND asq.target_end_date BETWEEN CURRENT_DATE() AND DATE_ADD(CURRENT_DATE(), 3))
    COMMENT 'Due within 3 days and still open' AS `At Risk ASQs`,
  COUNT(1) FILTER (WHERE asq.status = 'New')
    COMMENT 'Newly created, not yet assigned' AS `New ASQs`,
  COUNT(1) FILTER (WHERE asq.status = 'In Progress')
    COMMENT 'Currently being worked' AS `In Progress ASQs`,
  COUNT(1) FILTER (WHERE asq.status = 'Under Review')
    COMMENT 'Under initial review' AS `Under Review ASQs`,
  COUNT(1) FILTER (WHERE asq.status IN ('On Hold', '5. On hold'))
    COMMENT 'Temporarily on hold' AS `On Hold ASQs`,

  -- ========================================================================
  -- COMPLETION MEASURES
  -- ========================================================================

  COUNT(1) FILTER (WHERE asq.status IN ('Complete', 'Closed', 'Completed', '3. Delivered', 'Delivered', '0. Not engaged'))
    COMMENT 'ASQs with completion status' AS `Completed ASQs`,
  COUNT(1) FILTER (WHERE asq.status IN ('Complete', 'Closed', 'Completed', '3. Delivered', 'Delivered')
                     AND asq.actual_completion_date <= asq.target_end_date)
    COMMENT 'Completed on or before due date' AS `On-Time Completions`,
  COUNT(1) FILTER (WHERE asq.status IN ('Complete', 'Closed', 'Completed', '3. Delivered', 'Delivered')
                     AND asq.actual_completion_date > asq.target_end_date)
    COMMENT 'Completed after due date' AS `Late Completions`,

  -- ========================================================================
  -- RATE MEASURES (safe re-aggregation)
  -- ========================================================================

  -- On-Time Rate
  COUNT(1) FILTER (WHERE asq.status IN ('Complete', 'Closed', 'Completed', '3. Delivered', 'Delivered')
                     AND asq.actual_completion_date <= asq.target_end_date) * 1.0
    / NULLIF(COUNT(1) FILTER (WHERE asq.status IN ('Complete', 'Closed', 'Completed', '3. Delivered', 'Delivered', '0. Not engaged')), 0)
    COMMENT 'Percentage of ASQs completed on time' AS `On-Time Rate`,

  -- Overdue Rate
  COUNT(1) FILTER (WHERE asq.status IN ('New', 'Assigned', 'In Progress', 'Under Review', 'On Hold',
                                        'Unassigned', 'Review In Progress', 'Review Pending',
                                        '1. Briefing complete', '2. Actively engaged',
                                        'Additional Details Required', 'Pending', 'Draft')
                     AND asq.target_end_date < CURRENT_DATE()) * 1.0
    / NULLIF(COUNT(1) FILTER (WHERE asq.status IN ('New', 'Assigned', 'In Progress', 'Under Review', 'On Hold',
                                                   'Unassigned', 'Review In Progress', 'Review Pending',
                                                   '1. Briefing complete', '2. Actively engaged',
                                                   'Additional Details Required', 'Pending', 'Draft')), 0)
    COMMENT 'Percentage of open ASQs that are overdue' AS `Overdue Rate`,

  -- Completion Rate
  COUNT(1) FILTER (WHERE asq.status IN ('Complete', 'Closed', 'Completed', '3. Delivered', 'Delivered', '0. Not engaged')) * 1.0
    / NULLIF(COUNT(1), 0)
    COMMENT 'Percentage of ASQs completed (closed)' AS `Completion Rate`,

  -- At Risk Rate
  COUNT(1) FILTER (WHERE asq.status IN ('New', 'Assigned', 'In Progress', 'Under Review', 'On Hold',
                                        'Unassigned', 'Review In Progress', 'Review Pending',
                                        '1. Briefing complete', '2. Actively engaged',
                                        'Additional Details Required', 'Pending', 'Draft')
                     AND asq.target_end_date BETWEEN CURRENT_DATE() AND DATE_ADD(CURRENT_DATE(), 3)) * 1.0
    / NULLIF(COUNT(1) FILTER (WHERE asq.status IN ('New', 'Assigned', 'In Progress', 'Under Review', 'On Hold',
                                                   'Unassigned', 'Review In Progress', 'Review Pending',
                                                   '1. Briefing complete', '2. Actively engaged',
                                                   'Additional Details Required', 'Pending', 'Draft')), 0)
    COMMENT 'Percentage of open ASQs at risk' AS `At Risk Rate`,

  -- ========================================================================
  -- TURNAROUND MEASURES
  -- ========================================================================

  AVG(DATEDIFF(asq.actual_completion_date, asq.created_date))
    COMMENT 'Average turnaround time in days' AS `Avg Days to Complete`,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY DATEDIFF(asq.actual_completion_date, asq.created_date))
    COMMENT 'Median turnaround time' AS `Median Days to Complete`,
  PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY DATEDIFF(asq.actual_completion_date, asq.created_date))
    COMMENT '90th percentile turnaround' AS `P90 Days to Complete`,
  MIN(DATEDIFF(asq.actual_completion_date, asq.created_date))
    COMMENT 'Fastest completion time' AS `Min Days to Complete`,
  MAX(DATEDIFF(asq.actual_completion_date, asq.created_date))
    COMMENT 'Longest completion time' AS `Max Days to Complete`,
  AVG(DATEDIFF(CURRENT_DATE(), asq.created_date)) FILTER (WHERE asq.status IN ('New', 'Assigned', 'In Progress', 'Under Review', 'On Hold',
                                                                                 'Unassigned', 'Review In Progress', 'Review Pending',
                                                                                 '1. Briefing complete', '2. Actively engaged',
                                                                                 'Additional Details Required', 'Pending', 'Draft'))
    COMMENT 'Average days open (for open ASQs)' AS `Avg Days Open`,

  -- ========================================================================
  -- EFFORT MEASURES
  -- ========================================================================

  SUM(asq.estimated_effort_in_days)
    COMMENT 'Total estimated effort in days' AS `Total Estimated Days`,
  SUM(asq.actual_effort_in_days)
    COMMENT 'Total actual effort in days' AS `Total Actual Days`,
  AVG(asq.actual_effort_in_days / NULLIF(asq.estimated_effort_in_days, 0))
    COMMENT 'Average ratio of actual to estimated effort' AS `Avg Effort Ratio`,

  -- ========================================================================
  -- DISTRIBUTION MEASURES
  -- ========================================================================

  COUNT(DISTINCT asq.account_id)
    COMMENT 'Number of unique customer accounts' AS `Unique Accounts`,
  COUNT(DISTINCT asq.owner_user_id)
    COMMENT 'Number of unique SSAs working ASQs' AS `Unique SSAs`,
  COUNT(1) * 1.0 / NULLIF(COUNT(DISTINCT asq.owner_user_id), 0)
    COMMENT 'Average ASQ load per SSA' AS `ASQs per SSA`,
  COUNT(1) * 1.0 / NULLIF(COUNT(DISTINCT asq.account_id), 0)
    COMMENT 'Average ASQs per customer account' AS `ASQs per Account`,

  -- ========================================================================
  -- CONSUMPTION LINKAGE MEASURES (from account_obt)
  -- ========================================================================

  SUM(ao.dbu_dollars_qtd)
    COMMENT 'Total DBU consumption (QTD) for linked accounts' AS `Linked Account DBU QTD`,
  AVG(ao.dbu_dollars_qtd)
    COMMENT 'Average DBU consumption (QTD) per account' AS `Avg Account DBU QTD`

FROM main.gtm_silver.approval_request_detail asq
LEFT JOIN main.gtm_silver.individual_hierarchy_field hier
  ON asq.owner_user_id = hier.user_id
LEFT JOIN main.gtm_gold.account_obt ao
  ON asq.account_id = ao.account_id
  AND ao.fiscal_year_quarter = (
    SELECT MAX(fiscal_year_quarter) FROM main.gtm_gold.account_obt
  )
WHERE asq.snapshot_date = (
  SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail
);

-- ============================================================================
-- Sample Queries
-- ============================================================================
--
-- Individual SSA:
-- SELECT `Owner`, MEASURE(`Total ASQs`), MEASURE(`On-Time Rate`)
-- FROM mv_asq_operations
-- WHERE `Business Unit` = 'AMER Enterprise & Emerging' AND `Region` = 'CAN'
-- GROUP BY ALL;
--
-- Team rollup (L1 Manager):
-- SELECT `Manager L1`, `Owner`, MEASURE(`Total ASQs`)
-- FROM mv_asq_operations
-- WHERE `Manager L1` = 'Christopher Chalcraft'
-- GROUP BY ALL;
--
-- BU Comparison:
-- SELECT `Business Unit`, MEASURE(`Total ASQs`), MEASURE(`On-Time Rate`)
-- FROM mv_asq_operations
-- WHERE `Fiscal Year` = 2026
-- GROUP BY ALL;
--
-- Consumption Impact:
-- SELECT `Owner`, MEASURE(`Total ASQs`), MEASURE(`Linked Account DBU QTD`)
-- FROM mv_asq_operations
-- WHERE `Business Unit` = 'AMER Enterprise & Emerging'
-- GROUP BY ALL;
