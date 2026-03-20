-- ============================================================================
-- fact_uco - UCO Pipeline Fact Table
-- ============================================================================
-- Contains UCO (Use Case Opportunity) data for pipeline/revenue linkage
-- Used to track business impact of ASQ engagements
-- Run on: fevm-cjc workspace (reads from GTM Gold or synced data)
-- ============================================================================

CREATE OR REPLACE TABLE ${catalog}.${schema}.fact_uco (
  -- Primary key
  uco_id STRING NOT NULL,

  -- Identity
  uco_name STRING,
  uco_number STRING,

  -- Foreign keys
  account_id STRING,
  owner_id STRING,
  opportunity_id STRING,

  -- Status
  status STRING,
  stage STRING,
  is_open BOOLEAN,
  is_won BOOLEAN,

  -- Financials
  estimated_dbus DECIMAL(15,2),
  estimated_arr DECIMAL(15,2),
  weighted_arr DECIMAL(15,2),
  probability_pct DECIMAL(5,2),

  -- Dates
  created_date TIMESTAMP,
  close_date DATE,
  expected_close_date DATE,
  won_date DATE,

  -- Classification
  use_case_type STRING,
  product_category STRING,
  workload_type STRING,

  -- Linkage counts
  linked_asq_count INT,

  -- Time dimensions
  created_year INT,
  created_quarter INT,
  created_fy INT,
  created_fq INT,
  close_year INT,
  close_quarter INT,
  close_fy INT,
  close_fq INT,

  -- Metadata
  synced_at TIMESTAMP,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (created_year)
COMMENT 'UCO pipeline fact table for business impact tracking'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- ============================================================================
-- Populate fact_uco from UCO data
-- ============================================================================

-- Note: The source table depends on your data architecture.
-- This example assumes UCO data is available in GTM Gold or a synced table.
-- Adjust the source table reference as needed.

WITH uco_data AS (
  SELECT
    u.Id AS uco_id,
    u.Name AS uco_name,
    u.UCO_Number__c AS uco_number,

    -- Foreign keys
    u.Account__c AS account_id,
    u.OwnerId AS owner_id,
    u.Opportunity__c AS opportunity_id,

    -- Status
    u.Status__c AS status,
    u.Stage__c AS stage,
    u.Status__c IN ('Open', 'In Progress', 'Pending') AS is_open,
    u.Status__c = 'Won' AS is_won,

    -- Financials
    COALESCE(u.Estimated_DBUs__c, 0) AS estimated_dbus,
    COALESCE(u.Estimated_ARR__c, 0) AS estimated_arr,
    COALESCE(u.Estimated_ARR__c * u.Probability__c / 100, 0) AS weighted_arr,
    COALESCE(u.Probability__c, 0) AS probability_pct,

    -- Dates
    u.CreatedDate AS created_date,
    DATE(u.Close_Date__c) AS close_date,
    DATE(u.Expected_Close_Date__c) AS expected_close_date,
    CASE WHEN u.Status__c = 'Won' THEN DATE(u.Close_Date__c) ELSE NULL END AS won_date,

    -- Classification
    u.Use_Case_Type__c AS use_case_type,
    u.Product_Category__c AS product_category,
    u.Workload_Type__c AS workload_type,

    -- Linkage counts (calculated from fact_asq)
    0 AS linked_asq_count,  -- Will be updated via separate query

    -- Calendar time
    YEAR(u.CreatedDate) AS created_year,
    QUARTER(u.CreatedDate) AS created_quarter,
    YEAR(u.Close_Date__c) AS close_year,
    QUARTER(u.Close_Date__c) AS close_quarter,

    -- Fiscal time (FY ends Jan 31)
    CASE
      WHEN MONTH(u.CreatedDate) = 1 THEN YEAR(u.CreatedDate)
      ELSE YEAR(u.CreatedDate) + 1
    END AS created_fy,
    CASE
      WHEN MONTH(u.CreatedDate) = 1 THEN 4
      WHEN MONTH(u.CreatedDate) IN (2, 3, 4) THEN 1
      WHEN MONTH(u.CreatedDate) IN (5, 6, 7) THEN 2
      WHEN MONTH(u.CreatedDate) IN (8, 9, 10) THEN 3
      ELSE 4
    END AS created_fq,
    CASE
      WHEN u.Close_Date__c IS NULL THEN NULL
      WHEN MONTH(u.Close_Date__c) = 1 THEN YEAR(u.Close_Date__c)
      ELSE YEAR(u.Close_Date__c) + 1
    END AS close_fy,
    CASE
      WHEN u.Close_Date__c IS NULL THEN NULL
      WHEN MONTH(u.Close_Date__c) = 1 THEN 4
      WHEN MONTH(u.Close_Date__c) IN (2, 3, 4) THEN 1
      WHEN MONTH(u.Close_Date__c) IN (5, 6, 7) THEN 2
      WHEN MONTH(u.Close_Date__c) IN (8, 9, 10) THEN 3
      ELSE 4
    END AS close_fq,

    CURRENT_TIMESTAMP() AS synced_at

  FROM ${source_catalog}.${source_schema}.sf_uco u
  WHERE u.IsDeleted = FALSE
)

-- Merge into fact_uco
MERGE INTO ${catalog}.${schema}.fact_uco AS target
USING uco_data AS source
ON target.uco_id = source.uco_id
WHEN MATCHED THEN UPDATE SET
  target.uco_name = source.uco_name,
  target.uco_number = source.uco_number,
  target.account_id = source.account_id,
  target.owner_id = source.owner_id,
  target.opportunity_id = source.opportunity_id,
  target.status = source.status,
  target.stage = source.stage,
  target.is_open = source.is_open,
  target.is_won = source.is_won,
  target.estimated_dbus = source.estimated_dbus,
  target.estimated_arr = source.estimated_arr,
  target.weighted_arr = source.weighted_arr,
  target.probability_pct = source.probability_pct,
  target.created_date = source.created_date,
  target.close_date = source.close_date,
  target.expected_close_date = source.expected_close_date,
  target.won_date = source.won_date,
  target.use_case_type = source.use_case_type,
  target.product_category = source.product_category,
  target.workload_type = source.workload_type,
  target.created_year = source.created_year,
  target.created_quarter = source.created_quarter,
  target.created_fy = source.created_fy,
  target.created_fq = source.created_fq,
  target.close_year = source.close_year,
  target.close_quarter = source.close_quarter,
  target.close_fy = source.close_fy,
  target.close_fq = source.close_fq,
  target.synced_at = source.synced_at,
  target.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
  uco_id, uco_name, uco_number,
  account_id, owner_id, opportunity_id,
  status, stage, is_open, is_won,
  estimated_dbus, estimated_arr, weighted_arr, probability_pct,
  created_date, close_date, expected_close_date, won_date,
  use_case_type, product_category, workload_type,
  linked_asq_count,
  created_year, created_quarter, created_fy, created_fq,
  close_year, close_quarter, close_fy, close_fq,
  synced_at, created_at, updated_at
) VALUES (
  source.uco_id, source.uco_name, source.uco_number,
  source.account_id, source.owner_id, source.opportunity_id,
  source.status, source.stage, source.is_open, source.is_won,
  source.estimated_dbus, source.estimated_arr, source.weighted_arr, source.probability_pct,
  source.created_date, source.close_date, source.expected_close_date, source.won_date,
  source.use_case_type, source.product_category, source.workload_type,
  source.linked_asq_count,
  source.created_year, source.created_quarter, source.created_fy, source.created_fq,
  source.close_year, source.close_quarter, source.close_fy, source.close_fq,
  source.synced_at, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- ============================================================================
-- Update linked ASQ counts (run after fact_asq is populated)
-- ============================================================================

-- UPDATE ${catalog}.${schema}.fact_uco AS u
-- SET linked_asq_count = (
--   SELECT COUNT(*)
--   FROM ${catalog}.${schema}.fact_asq a
--   WHERE a.linked_uco_id = u.uco_id
-- );

-- ============================================================================
-- Verification
-- ============================================================================
-- SELECT status, COUNT(*), SUM(estimated_arr) FROM ${catalog}.${schema}.fact_uco GROUP BY status;
-- SELECT created_fy, created_fq, COUNT(*), SUM(estimated_arr) FROM ${catalog}.${schema}.fact_uco GROUP BY ALL ORDER BY 1, 2;
