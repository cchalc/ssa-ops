-- ============================================================================
-- fact_asq - Core ASQ Fact Table
-- ============================================================================
-- Contains ALL ASQs from ALL business units (no hardcoded filters)
-- Pre-computes SLA flags, date calculations, and status flags
-- Run on: fevm-cjc workspace (reads from synced logfood data)
-- ============================================================================

CREATE OR REPLACE TABLE ${catalog}.${schema}.fact_asq (
  -- Primary key
  asq_id STRING NOT NULL,

  -- Natural key
  asq_number STRING,
  asq_name STRING,

  -- Status
  status STRING,
  is_open BOOLEAN,
  is_overdue BOOLEAN,
  is_at_risk BOOLEAN,

  -- Foreign keys
  owner_id STRING,
  account_id STRING,
  created_date_key DATE,
  assigned_date_key DATE,
  due_date_key DATE,
  completed_date_key DATE,

  -- Raw timestamps
  created_date TIMESTAMP,
  assignment_date TIMESTAMP,
  due_date TIMESTAMP,
  completion_date TIMESTAMP,
  first_note_date TIMESTAMP,
  last_update_date TIMESTAMP,

  -- Request details
  specialization STRING,
  support_type STRING,
  priority STRING,
  complexity STRING,
  request_source STRING,

  -- Effort
  estimated_effort_days DECIMAL(5,2),
  actual_effort_days DECIMAL(5,2),
  effort_ratio DECIMAL(5,2),
  effort_variance_days DECIMAL(5,2),
  accuracy_category STRING,

  -- Quality indicators
  has_quality_closure BOOLEAN,
  has_artifacts BOOLEAN,
  has_follow_up BOOLEAN,
  has_notes BOOLEAN,
  notes_length INT,
  customer_satisfaction STRING,

  -- Linkage
  linked_uco_id STRING,
  linked_opportunity_id STRING,
  linked_case_id STRING,

  -- SLA milestones (days)
  days_to_review INT,
  days_to_assignment INT,
  days_to_first_note INT,
  days_to_completion INT,
  days_open INT,
  days_until_due INT,
  days_overdue INT,

  -- SLA compliance flags
  review_sla_met BOOLEAN,
  assignment_sla_met BOOLEAN,
  response_sla_met BOOLEAN,
  completion_sla_met BOOLEAN,

  -- Time dimensions (for partitioning/filtering)
  created_year INT,
  created_quarter INT,
  created_month INT,
  created_week INT,
  completed_year INT,
  completed_quarter INT,
  completed_month INT,

  -- Fiscal time
  created_fy INT,
  created_fq INT,
  completed_fy INT,
  completed_fq INT,

  -- Metadata
  synced_at TIMESTAMP,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (created_year)
COMMENT 'Core ASQ fact table with all BUs, pre-computed SLAs, and date calculations'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- ============================================================================
-- Populate fact_asq from Salesforce ApprovalRequest data
-- ============================================================================

WITH asq_raw AS (
  SELECT
    a.Id AS asq_id,
    a.Name AS asq_number,
    a.Request_Name__c AS asq_name,
    a.Status__c AS status,

    -- Owner and Account
    a.OwnerId AS owner_id,
    a.Account__c AS account_id,

    -- Timestamps
    a.CreatedDate AS created_date,
    a.AssignmentDate__c AS assignment_date,
    a.End_Date__c AS due_date,
    CASE
      WHEN a.Status__c IN ('Completed', 'Closed') THEN a.LastModifiedDate
      ELSE NULL
    END AS completion_date,
    -- First note date approximation (would need Activity history for accuracy)
    CASE
      WHEN a.Request_Status_Notes__c IS NOT NULL
        AND LENGTH(a.Request_Status_Notes__c) > 10
        AND a.AssignmentDate__c IS NOT NULL
      THEN a.AssignmentDate__c
      ELSE NULL
    END AS first_note_date,
    a.LastModifiedDate AS last_update_date,

    -- Request details
    a.Specialization__c AS specialization,
    a.Support_Type__c AS support_type,
    a.Priority__c AS priority,
    COALESCE(a.Complexity__c,
      CASE
        WHEN a.Specialization__c LIKE '%ML%' OR a.Specialization__c LIKE '%AI%' THEN 'High'
        WHEN a.Support_Type__c = 'Deep Dive' THEN 'High'
        WHEN a.Support_Type__c = 'Technical Review' THEN 'Medium'
        ELSE 'Standard'
      END
    ) AS complexity,
    a.Request_Source__c AS request_source,

    -- Effort
    a.Estimated_Effort_Days__c AS estimated_effort_days,
    a.Actual_Effort_Days__c AS actual_effort_days,

    -- Quality indicators
    a.Request_Status_Notes__c AS request_notes,

    -- Linkage
    a.Linked_UCO__c AS linked_uco_id,
    a.Linked_Opportunity__c AS linked_opportunity_id,
    a.Linked_Case__c AS linked_case_id

  FROM ${source_catalog}.${source_schema}.sf_approval_request a
  WHERE a.IsDeleted = FALSE
),

asq_calculated AS (
  SELECT
    asq_id,
    asq_number,
    asq_name,
    status,

    -- Status flags
    status IN ('Submitted', 'Under Review', 'In Progress', 'On Hold') AS is_open,
    due_date < CURRENT_DATE() AND completion_date IS NULL AS is_overdue,
    DATEDIFF(due_date, CURRENT_DATE()) BETWEEN 0 AND 3
      AND completion_date IS NULL AS is_at_risk,

    -- Foreign keys
    owner_id,
    account_id,
    DATE(created_date) AS created_date_key,
    DATE(assignment_date) AS assigned_date_key,
    DATE(due_date) AS due_date_key,
    DATE(completion_date) AS completed_date_key,

    -- Raw timestamps
    created_date,
    assignment_date,
    due_date,
    completion_date,
    first_note_date,
    last_update_date,

    -- Request details
    specialization,
    support_type,
    priority,
    complexity,
    request_source,

    -- Effort calculations
    estimated_effort_days,
    actual_effort_days,
    CASE
      WHEN estimated_effort_days > 0 AND actual_effort_days IS NOT NULL
      THEN ROUND(actual_effort_days / estimated_effort_days, 2)
      ELSE NULL
    END AS effort_ratio,
    CASE
      WHEN estimated_effort_days IS NOT NULL AND actual_effort_days IS NOT NULL
      THEN actual_effort_days - estimated_effort_days
      ELSE NULL
    END AS effort_variance_days,
    CASE
      WHEN estimated_effort_days IS NULL OR actual_effort_days IS NULL THEN 'No Data'
      WHEN actual_effort_days <= estimated_effort_days * 0.8 THEN 'Under Actual'
      WHEN actual_effort_days <= estimated_effort_days * 1.2 THEN 'Accurate'
      WHEN actual_effort_days <= estimated_effort_days * 1.5 THEN 'Slight Over'
      ELSE 'Significant Over'
    END AS accuracy_category,

    -- Quality indicators
    request_notes IS NOT NULL
      AND LENGTH(request_notes) > 50
      AND actual_effort_days IS NOT NULL AS has_quality_closure,
    FALSE AS has_artifacts,  -- Would need separate tracking
    FALSE AS has_follow_up,  -- Would need separate tracking
    request_notes IS NOT NULL AND LENGTH(request_notes) > 10 AS has_notes,
    COALESCE(LENGTH(request_notes), 0) AS notes_length,
    CAST(NULL AS STRING) AS customer_satisfaction,

    -- Linkage
    linked_uco_id,
    linked_opportunity_id,
    linked_case_id,

    -- SLA milestones (days)
    CASE
      WHEN assignment_date IS NOT NULL
      THEN DATEDIFF(assignment_date, created_date)
      ELSE DATEDIFF(CURRENT_DATE(), created_date)
    END AS days_to_review,
    CASE
      WHEN assignment_date IS NOT NULL
      THEN DATEDIFF(assignment_date, created_date)
      ELSE NULL
    END AS days_to_assignment,
    CASE
      WHEN first_note_date IS NOT NULL
      THEN DATEDIFF(first_note_date, created_date)
      ELSE NULL
    END AS days_to_first_note,
    CASE
      WHEN completion_date IS NOT NULL
      THEN DATEDIFF(completion_date, created_date)
      ELSE NULL
    END AS days_to_completion,
    CASE
      WHEN completion_date IS NULL
      THEN DATEDIFF(CURRENT_DATE(), created_date)
      ELSE NULL
    END AS days_open,
    CASE
      WHEN due_date IS NOT NULL AND completion_date IS NULL
      THEN DATEDIFF(due_date, CURRENT_DATE())
      ELSE NULL
    END AS days_until_due,
    CASE
      WHEN due_date IS NOT NULL
        AND due_date < CURRENT_DATE()
        AND completion_date IS NULL
      THEN DATEDIFF(CURRENT_DATE(), due_date)
      ELSE 0
    END AS days_overdue,

    -- SLA compliance flags (using configured thresholds)
    COALESCE(DATEDIFF(assignment_date, created_date) <= 2, FALSE) AS review_sla_met,
    COALESCE(DATEDIFF(assignment_date, created_date) <= 5, FALSE) AS assignment_sla_met,
    COALESCE(DATEDIFF(first_note_date, created_date) <= 5, FALSE) AS response_sla_met,
    COALESCE(completion_date <= due_date, FALSE) AS completion_sla_met,

    -- Calendar time dimensions
    YEAR(created_date) AS created_year,
    QUARTER(created_date) AS created_quarter,
    MONTH(created_date) AS created_month,
    WEEKOFYEAR(created_date) AS created_week,
    YEAR(completion_date) AS completed_year,
    QUARTER(completion_date) AS completed_quarter,
    MONTH(completion_date) AS completed_month,

    -- Fiscal time (FY ends Jan 31)
    CASE
      WHEN MONTH(created_date) = 1 THEN YEAR(created_date)
      ELSE YEAR(created_date) + 1
    END AS created_fy,
    CASE
      WHEN MONTH(created_date) = 1 THEN 4  -- Jan is FQ4
      WHEN MONTH(created_date) IN (2, 3, 4) THEN 1  -- Feb-Apr is FQ1
      WHEN MONTH(created_date) IN (5, 6, 7) THEN 2  -- May-Jul is FQ2
      WHEN MONTH(created_date) IN (8, 9, 10) THEN 3  -- Aug-Oct is FQ3
      ELSE 4  -- Nov-Dec is FQ4
    END AS created_fq,
    CASE
      WHEN completion_date IS NULL THEN NULL
      WHEN MONTH(completion_date) = 1 THEN YEAR(completion_date)
      ELSE YEAR(completion_date) + 1
    END AS completed_fy,
    CASE
      WHEN completion_date IS NULL THEN NULL
      WHEN MONTH(completion_date) = 1 THEN 4
      WHEN MONTH(completion_date) IN (2, 3, 4) THEN 1
      WHEN MONTH(completion_date) IN (5, 6, 7) THEN 2
      WHEN MONTH(completion_date) IN (8, 9, 10) THEN 3
      ELSE 4
    END AS completed_fq,

    CURRENT_TIMESTAMP() AS synced_at

  FROM asq_raw
)

-- Merge into fact_asq
MERGE INTO ${catalog}.${schema}.fact_asq AS target
USING asq_calculated AS source
ON target.asq_id = source.asq_id
WHEN MATCHED THEN UPDATE SET
  target.asq_number = source.asq_number,
  target.asq_name = source.asq_name,
  target.status = source.status,
  target.is_open = source.is_open,
  target.is_overdue = source.is_overdue,
  target.is_at_risk = source.is_at_risk,
  target.owner_id = source.owner_id,
  target.account_id = source.account_id,
  target.created_date_key = source.created_date_key,
  target.assigned_date_key = source.assigned_date_key,
  target.due_date_key = source.due_date_key,
  target.completed_date_key = source.completed_date_key,
  target.created_date = source.created_date,
  target.assignment_date = source.assignment_date,
  target.due_date = source.due_date,
  target.completion_date = source.completion_date,
  target.first_note_date = source.first_note_date,
  target.last_update_date = source.last_update_date,
  target.specialization = source.specialization,
  target.support_type = source.support_type,
  target.priority = source.priority,
  target.complexity = source.complexity,
  target.request_source = source.request_source,
  target.estimated_effort_days = source.estimated_effort_days,
  target.actual_effort_days = source.actual_effort_days,
  target.effort_ratio = source.effort_ratio,
  target.effort_variance_days = source.effort_variance_days,
  target.accuracy_category = source.accuracy_category,
  target.has_quality_closure = source.has_quality_closure,
  target.has_artifacts = source.has_artifacts,
  target.has_follow_up = source.has_follow_up,
  target.has_notes = source.has_notes,
  target.notes_length = source.notes_length,
  target.customer_satisfaction = source.customer_satisfaction,
  target.linked_uco_id = source.linked_uco_id,
  target.linked_opportunity_id = source.linked_opportunity_id,
  target.linked_case_id = source.linked_case_id,
  target.days_to_review = source.days_to_review,
  target.days_to_assignment = source.days_to_assignment,
  target.days_to_first_note = source.days_to_first_note,
  target.days_to_completion = source.days_to_completion,
  target.days_open = source.days_open,
  target.days_until_due = source.days_until_due,
  target.days_overdue = source.days_overdue,
  target.review_sla_met = source.review_sla_met,
  target.assignment_sla_met = source.assignment_sla_met,
  target.response_sla_met = source.response_sla_met,
  target.completion_sla_met = source.completion_sla_met,
  target.created_year = source.created_year,
  target.created_quarter = source.created_quarter,
  target.created_month = source.created_month,
  target.created_week = source.created_week,
  target.completed_year = source.completed_year,
  target.completed_quarter = source.completed_quarter,
  target.completed_month = source.completed_month,
  target.created_fy = source.created_fy,
  target.created_fq = source.created_fq,
  target.completed_fy = source.completed_fy,
  target.completed_fq = source.completed_fq,
  target.synced_at = source.synced_at,
  target.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT *;

-- ============================================================================
-- Create indexes for common query patterns
-- ============================================================================
-- Note: Delta Lake handles optimization automatically, but we can add
-- ZORDER optimization for frequently filtered columns

-- OPTIMIZE ${catalog}.${schema}.fact_asq ZORDER BY (owner_id, created_date_key);

-- ============================================================================
-- Verification
-- ============================================================================
-- SELECT created_year, is_open, COUNT(*) FROM ${catalog}.${schema}.fact_asq GROUP BY ALL ORDER BY 1, 2;
-- SELECT status, COUNT(*) FROM ${catalog}.${schema}.fact_asq GROUP BY status;
-- SELECT is_overdue, COUNT(*) FROM ${catalog}.${schema}.fact_asq WHERE is_open GROUP BY is_overdue;
