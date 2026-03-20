-- ============================================================================
-- dim_ssa - SSA Dimension with Manager Hierarchy
-- ============================================================================
-- Contains all SSAs with N-level manager hierarchy for flexible rollups
-- Hierarchy is built via recursive CTE from Salesforce User table
-- Run on: fevm-cjc workspace (reads from synced logfood data)
-- ============================================================================

CREATE OR REPLACE TABLE ${catalog}.${schema}.dim_ssa (
  -- Primary key
  ssa_id STRING NOT NULL,

  -- Identity
  ssa_name STRING,
  ssa_email STRING,
  ssa_alias STRING,
  title STRING,

  -- Direct manager
  manager_id STRING,
  manager_name STRING,

  -- Manager hierarchy (L1 = direct, L5 = highest)
  level_1_manager_id STRING,
  level_1_manager_name STRING,
  level_2_manager_id STRING,
  level_2_manager_name STRING,
  level_3_manager_id STRING,
  level_3_manager_name STRING,
  level_4_manager_id STRING,
  level_4_manager_name STRING,
  level_5_manager_id STRING,
  level_5_manager_name STRING,

  -- Hierarchy depth
  hierarchy_level INT,
  full_hierarchy_path STRING,

  -- Organizational alignment
  business_unit STRING,
  region STRING,
  geo STRING,
  segment STRING,
  vertical STRING,
  cost_center STRING,

  -- Role classification
  role_type STRING,
  specialization STRING,
  is_people_manager BOOLEAN,

  -- Status
  is_active BOOLEAN,
  hire_date DATE,
  termination_date DATE,

  -- Metadata
  synced_at TIMESTAMP,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP
)
USING DELTA
COMMENT 'SSA dimension with N-level manager hierarchy for multi-BU filtering'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- ============================================================================
-- Populate dim_ssa from Salesforce User data
-- ============================================================================

-- Build manager hierarchy using recursive CTE
WITH RECURSIVE user_hierarchy AS (
  -- Base case: Get all users
  SELECT
    u.Id AS ssa_id,
    u.Name AS ssa_name,
    u.Email AS ssa_email,
    u.Alias AS ssa_alias,
    u.Title AS title,
    u.ManagerId AS manager_id,
    m.Name AS manager_name,
    u.IsActive AS is_active,
    u.Department AS department,
    u.Division AS division,

    -- Initialize hierarchy columns
    u.ManagerId AS level_1_manager_id,
    m.Name AS level_1_manager_name,
    CAST(NULL AS STRING) AS level_2_manager_id,
    CAST(NULL AS STRING) AS level_2_manager_name,
    CAST(NULL AS STRING) AS level_3_manager_id,
    CAST(NULL AS STRING) AS level_3_manager_name,
    CAST(NULL AS STRING) AS level_4_manager_id,
    CAST(NULL AS STRING) AS level_4_manager_name,
    CAST(NULL AS STRING) AS level_5_manager_id,
    CAST(NULL AS STRING) AS level_5_manager_name,

    1 AS hierarchy_level,
    u.Name AS full_hierarchy_path

  FROM ${source_catalog}.${source_schema}.sf_user u
  LEFT JOIN ${source_catalog}.${source_schema}.sf_user m ON u.ManagerId = m.Id
  WHERE u.UserType = 'Standard'
),

-- Note: True recursive CTE for hierarchy would require Databricks Runtime 14.0+
-- For now, we'll use iterative joins to build up to 5 levels

hierarchy_l2 AS (
  SELECT
    h1.*,
    m2.ManagerId AS l2_manager_id,
    m2.Name AS l2_manager_name
  FROM user_hierarchy h1
  LEFT JOIN ${source_catalog}.${source_schema}.sf_user m2
    ON h1.level_1_manager_id = m2.Id
),

hierarchy_l3 AS (
  SELECT
    h2.*,
    m3.ManagerId AS l3_manager_id,
    m3.Name AS l3_manager_name
  FROM hierarchy_l2 h2
  LEFT JOIN ${source_catalog}.${source_schema}.sf_user m3
    ON h2.l2_manager_id = m3.Id
),

hierarchy_l4 AS (
  SELECT
    h3.*,
    m4.ManagerId AS l4_manager_id,
    m4.Name AS l4_manager_name
  FROM hierarchy_l3 h3
  LEFT JOIN ${source_catalog}.${source_schema}.sf_user m4
    ON h3.l3_manager_id = m4.Id
),

hierarchy_l5 AS (
  SELECT
    h4.*,
    m5.ManagerId AS l5_manager_id,
    m5.Name AS l5_manager_name
  FROM hierarchy_l4 h4
  LEFT JOIN ${source_catalog}.${source_schema}.sf_user m5
    ON h4.l4_manager_id = m5.Id
),

-- Derive organizational attributes from department/division
final_hierarchy AS (
  SELECT
    ssa_id,
    ssa_name,
    ssa_email,
    ssa_alias,
    title,
    manager_id,
    manager_name,

    -- Hierarchy levels
    level_1_manager_id,
    level_1_manager_name,
    l2_manager_id AS level_2_manager_id,
    l2_manager_name AS level_2_manager_name,
    l3_manager_id AS level_3_manager_id,
    l3_manager_name AS level_3_manager_name,
    l4_manager_id AS level_4_manager_id,
    l4_manager_name AS level_4_manager_name,
    l5_manager_id AS level_5_manager_id,
    l5_manager_name AS level_5_manager_name,

    -- Calculate hierarchy depth
    CASE
      WHEN l5_manager_id IS NOT NULL THEN 5
      WHEN l4_manager_id IS NOT NULL THEN 4
      WHEN l3_manager_id IS NOT NULL THEN 3
      WHEN l2_manager_id IS NOT NULL THEN 2
      WHEN level_1_manager_id IS NOT NULL THEN 1
      ELSE 0
    END AS hierarchy_level,

    -- Build full path
    CONCAT_WS(' > ',
      COALESCE(l5_manager_name, ''),
      COALESCE(l4_manager_name, ''),
      COALESCE(l3_manager_name, ''),
      COALESCE(l2_manager_name, ''),
      COALESCE(level_1_manager_name, ''),
      ssa_name
    ) AS full_hierarchy_path,

    -- Derive business unit from department/division
    -- TODO: Update this mapping based on actual Salesforce data
    CASE
      WHEN department LIKE '%Canada%' OR division LIKE '%CAN%' THEN 'CAN'
      WHEN department LIKE '%West%' THEN 'US-WEST'
      WHEN department LIKE '%East%' THEN 'US-EAST'
      WHEN department LIKE '%EMEA%' OR department LIKE '%Europe%' THEN 'EMEA'
      WHEN department LIKE '%APJ%' OR department LIKE '%Asia%' THEN 'APJ'
      -- Fallback: derive from known manager IDs
      WHEN level_2_manager_id = '0053f000000pKoTAAU' THEN 'CAN'  -- CJC's team
      ELSE 'UNKNOWN'
    END AS business_unit,

    -- Derive region
    CASE
      WHEN department LIKE '%Canada%' OR department LIKE '%US%' OR department LIKE '%West%' OR department LIKE '%East%' THEN 'Americas'
      WHEN department LIKE '%EMEA%' OR department LIKE '%Europe%' THEN 'EMEA'
      WHEN department LIKE '%APJ%' OR department LIKE '%Asia%' THEN 'APJ'
      ELSE 'Americas'  -- Default
    END AS region,

    -- Derive geo
    CASE
      WHEN department LIKE '%Canada%' OR department LIKE '%US%' THEN 'NA'
      WHEN department LIKE '%LATAM%' OR department LIKE '%Latin%' THEN 'LATAM'
      WHEN department LIKE '%EMEA%' THEN 'EMEA'
      WHEN department LIKE '%APJ%' THEN 'APJ'
      ELSE 'NA'
    END AS geo,

    -- Segment (ENT, COMM, etc.) - derive from title or department
    CASE
      WHEN title LIKE '%Enterprise%' OR department LIKE '%ENT%' THEN 'ENT'
      WHEN title LIKE '%Commercial%' OR department LIKE '%COMM%' THEN 'COMM'
      WHEN title LIKE '%Mid%Market%' OR department LIKE '%MM%' THEN 'MM'
      WHEN title LIKE '%SMB%' THEN 'SMB'
      ELSE NULL
    END AS segment,

    -- Vertical - not typically in User data, set to NULL
    CAST(NULL AS STRING) AS vertical,

    -- Cost center
    CAST(NULL AS STRING) AS cost_center,

    -- Role type from title
    CASE
      WHEN title LIKE '%Specialist%' OR title LIKE '%SSA%' THEN 'SSA'
      WHEN title LIKE '%Solutions Architect%' OR title LIKE '%SA%' THEN 'SA'
      WHEN title LIKE '%Customer Success%' OR title LIKE '%CSM%' THEN 'CSM'
      WHEN title LIKE '%TAM%' THEN 'TAM'
      ELSE 'SSA'  -- Default for this project
    END AS role_type,

    -- Specialization from title
    CASE
      WHEN title LIKE '%ML%' OR title LIKE '%AI%' OR title LIKE '%Machine Learning%' THEN 'AI/ML'
      WHEN title LIKE '%Data%Eng%' OR title LIKE '%Spark%' THEN 'Data Engineering'
      WHEN title LIKE '%Platform%' THEN 'Platform'
      WHEN title LIKE '%SQL%' OR title LIKE '%BI%' THEN 'SQL/BI'
      ELSE NULL
    END AS specialization,

    -- Is people manager
    EXISTS (
      SELECT 1 FROM ${source_catalog}.${source_schema}.sf_user sub
      WHERE sub.ManagerId = h.ssa_id
    ) AS is_people_manager,

    is_active,

    -- Dates (not typically in User data with good accuracy)
    CAST(NULL AS DATE) AS hire_date,
    CAST(NULL AS DATE) AS termination_date,

    CURRENT_TIMESTAMP() AS synced_at

  FROM hierarchy_l5 h
)

-- Merge into dim_ssa
MERGE INTO ${catalog}.${schema}.dim_ssa AS target
USING final_hierarchy AS source
ON target.ssa_id = source.ssa_id
WHEN MATCHED THEN UPDATE SET
  target.ssa_name = source.ssa_name,
  target.ssa_email = source.ssa_email,
  target.ssa_alias = source.ssa_alias,
  target.title = source.title,
  target.manager_id = source.manager_id,
  target.manager_name = source.manager_name,
  target.level_1_manager_id = source.level_1_manager_id,
  target.level_1_manager_name = source.level_1_manager_name,
  target.level_2_manager_id = source.level_2_manager_id,
  target.level_2_manager_name = source.level_2_manager_name,
  target.level_3_manager_id = source.level_3_manager_id,
  target.level_3_manager_name = source.level_3_manager_name,
  target.level_4_manager_id = source.level_4_manager_id,
  target.level_4_manager_name = source.level_4_manager_name,
  target.level_5_manager_id = source.level_5_manager_id,
  target.level_5_manager_name = source.level_5_manager_name,
  target.hierarchy_level = source.hierarchy_level,
  target.full_hierarchy_path = source.full_hierarchy_path,
  target.business_unit = source.business_unit,
  target.region = source.region,
  target.geo = source.geo,
  target.segment = source.segment,
  target.vertical = source.vertical,
  target.role_type = source.role_type,
  target.specialization = source.specialization,
  target.is_people_manager = source.is_people_manager,
  target.is_active = source.is_active,
  target.synced_at = source.synced_at,
  target.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
  ssa_id, ssa_name, ssa_email, ssa_alias, title,
  manager_id, manager_name,
  level_1_manager_id, level_1_manager_name,
  level_2_manager_id, level_2_manager_name,
  level_3_manager_id, level_3_manager_name,
  level_4_manager_id, level_4_manager_name,
  level_5_manager_id, level_5_manager_name,
  hierarchy_level, full_hierarchy_path,
  business_unit, region, geo, segment, vertical, cost_center,
  role_type, specialization, is_people_manager,
  is_active, hire_date, termination_date,
  synced_at, created_at, updated_at
) VALUES (
  source.ssa_id, source.ssa_name, source.ssa_email, source.ssa_alias, source.title,
  source.manager_id, source.manager_name,
  source.level_1_manager_id, source.level_1_manager_name,
  source.level_2_manager_id, source.level_2_manager_name,
  source.level_3_manager_id, source.level_3_manager_name,
  source.level_4_manager_id, source.level_4_manager_name,
  source.level_5_manager_id, source.level_5_manager_name,
  source.hierarchy_level, source.full_hierarchy_path,
  source.business_unit, source.region, source.geo, source.segment, source.vertical, NULL,
  source.role_type, source.specialization, source.is_people_manager,
  source.is_active, source.hire_date, source.termination_date,
  source.synced_at, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- ============================================================================
-- Verification
-- ============================================================================
-- SELECT business_unit, COUNT(*) FROM ${catalog}.${schema}.dim_ssa WHERE is_active GROUP BY business_unit;
-- SELECT level_2_manager_name, COUNT(*) FROM ${catalog}.${schema}.dim_ssa WHERE is_active GROUP BY level_2_manager_name;
