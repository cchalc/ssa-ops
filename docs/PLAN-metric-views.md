# SSA Metric Views Data Model Redesign

## Executive Summary

This plan outlines the transformation of the SSA Activity Dashboard from traditional SQL views to Unity Catalog Metric Views. The architecture is designed for **multi-BU scalability** - built for Canada (CAN) but portable to any business unit with configurable filters.

**Key Design Principles:**
- **Multi-level granularity** - Roll up/down across org hierarchy, time, geography
- **BU-agnostic schema** - No hardcoded team filters; configurable at query time
- **Manager hierarchy** - Support N-level manager chains for any team structure
- **Vertical alignment** - Industry/segment dimensions for cross-BU analysis
- **Portable YAML** - Move definitions between workspaces with config changes only

**Branch:** `ssa-metric-views`
**Target:** fevm-cjc workspace (`cjc_aws_workspace_catalog.ssa_ops`)
**Portable:** YAML definitions can be recreated on any workspace

---

## 1. Current Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        CURRENT DATA FLOW                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  SALESFORCE ──(Stitch)──▶ LOGFOOD (Azure)                                  │
│                              │                                              │
│                              ├── stitch.salesforce.approvalrequest__c       │
│                              ├── stitch.salesforce.user                     │
│                              ├── stitch.salesforce.account                  │
│                              └── main.gtm_gold.* (UCO, adoption)            │
│                              │                                              │
│                              ▼                                              │
│                     SQL VIEWS (logfood)                                     │
│                     cjc_views.cjc_*                                         │
│                              │                                              │
│                    (6:30 AM daily)                                          │
│                              │                                              │
│                              ▼                                              │
│                     DELTA TABLES (fevm-cjc)                                 │
│                     ssa_ops_dev.*                                           │
│                              │                                              │
│                    (7:00 AM daily)                                          │
│                              │                                              │
│                              ▼                                              │
│                     LAKEBASE (PostgreSQL)                                   │
│                     dashboard.*                                             │
│                              │                                              │
│                              ▼                                              │
│                     SSA-OPS APP (TanStack)                                  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Current Pain Points

1. **Hardcoded team filters** - Views only work for CJC's team
2. **No hierarchy support** - Can't roll up to RVP/AVP/VP levels
3. **Views computed at query time** - Slow for large datasets
4. **No semantic layer** - Business logic scattered across SQL
5. **Ratio measures unsafe** - Risk of incorrect re-aggregation
6. **No standardization** - Each dashboard interprets metrics differently
7. **No cross-BU comparison** - Can't benchmark against other teams

---

## 2. Target Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        TARGET DATA FLOW                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  SALESFORCE ──(Stitch)──▶ LOGFOOD (Azure)                                  │
│                              │                                              │
│                              ├── stitch.salesforce.approvalrequest__c       │
│                              ├── stitch.salesforce.user (+ hierarchy)       │
│                              ├── stitch.salesforce.account                  │
│                              └── main.gtm_gold.* (UCO, adoption)            │
│                              │                                              │
│           ┌──────────────────┴──────────────────┐                          │
│           │                                      │                          │
│           ▼                                      ▼                          │
│    FEDERATED CATALOG                      DELTA TABLES                      │
│    (logfood → fevm-cjc)                   (fevm-cjc daily sync)             │
│           │                                      │                          │
│           └──────────────────┬──────────────────┘                          │
│                              │                                              │
│                              ▼                                              │
│    ┌─────────────────────────────────────────────────────────────────┐     │
│    │              DIMENSIONAL MODEL (Star Schema)                     │     │
│    │                                                                  │     │
│    │   ┌─────────────────┐    ┌─────────────────┐                    │     │
│    │   │    dim_ssa      │    │   dim_account   │                    │     │
│    │   │  + hierarchy    │    │  + segments     │                    │     │
│    │   └────────┬────────┘    └────────┬────────┘                    │     │
│    │            │                       │                             │     │
│    │   ┌────────┴───────────────────────┴────────┐                   │     │
│    │   │              fact_asq                    │                   │     │
│    │   │   (all BUs, all time, all statuses)     │                   │     │
│    │   └────────┬───────────────────────┬────────┘                   │     │
│    │            │                       │                             │     │
│    │   ┌────────┴────────┐    ┌────────┴────────┐                    │     │
│    │   │   dim_date      │    │   fact_uco      │                    │     │
│    │   │  (fiscal/cal)   │    │  (pipeline)     │                    │     │
│    │   └─────────────────┘    └─────────────────┘                    │     │
│    └─────────────────────────────────────────────────────────────────┘     │
│                              │                                              │
│                              ▼                                              │
│    ┌─────────────────────────────────────────────────────────────────┐     │
│    │                    METRIC VIEWS LAYER                            │     │
│    │                                                                  │     │
│    │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐             │     │
│    │  │ asq_metrics  │ │ sla_metrics  │ │effort_metrics│             │     │
│    │  │ (operational)│ │ (compliance) │ │  (capacity)  │             │     │
│    │  └──────────────┘ └──────────────┘ └──────────────┘             │     │
│    │                                                                  │     │
│    │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐             │     │
│    │  │account_metric│ │ uco_metrics  │ │ team_metrics │             │     │
│    │  │ (engagement) │ │  (pipeline)  │ │  (compare)   │             │     │
│    │  └──────────────┘ └──────────────┘ └──────────────┘             │     │
│    │                                                                  │     │
│    │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐             │     │
│    │  │ trend_metrics│ │ aging_metrics│ │quality_metric│             │     │
│    │  │   (YoY/QoQ)  │ │  (backlog)   │ │  (closure)   │             │     │
│    │  └──────────────┘ └──────────────┘ └──────────────┘             │     │
│    └─────────────────────────────────────────────────────────────────┘     │
│                              │                                              │
│              ┌───────────────┴───────────────┐                             │
│              │                               │                              │
│              ▼                               ▼                              │
│       AI/BI DASHBOARDS                 LAKEBASE SYNC                       │
│       (Native Genie)                   (For TanStack app)                  │
│                                              │                              │
│                                              ▼                              │
│                                        SSA-OPS APP                         │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Benefits of Metric Views

| Aspect | Current | With Metric Views |
|--------|---------|-------------------|
| **BU Portability** | Hardcoded CAN filters | Configurable at query time |
| **Manager Hierarchy** | Single level | N-level rollup/drill |
| **Aggregation** | Manual in SQL | Engine handles correctly |
| **Ratios** | Risk of double-counting | Safe re-aggregation |
| **Governance** | None | Unity Catalog permissions |
| **AI/BI** | Manual dashboards | Native Genie integration |
| **Cross-BU Compare** | Not possible | Built-in benchmarking |

---

## 3. Dimensional Model Design

### 3.1 Hierarchy Dimensions

#### `dim_ssa` - SSA with Full Management Hierarchy

```sql
CREATE TABLE cjc_aws_workspace_catalog.ssa_ops.dim_ssa (
  -- Primary key
  ssa_id STRING NOT NULL,

  -- Identity
  ssa_name STRING,
  ssa_email STRING,
  ssa_alias STRING,
  title STRING,

  -- Direct management
  manager_id STRING,
  manager_name STRING,

  -- Hierarchy levels (populated via recursive CTE)
  level_1_manager_id STRING,    -- Direct manager
  level_1_manager_name STRING,
  level_2_manager_id STRING,    -- Manager's manager (e.g., RVP)
  level_2_manager_name STRING,
  level_3_manager_id STRING,    -- Third level (e.g., AVP)
  level_3_manager_name STRING,
  level_4_manager_id STRING,    -- Fourth level (e.g., VP)
  level_4_manager_name STRING,
  level_5_manager_id STRING,    -- Fifth level (e.g., SVP/C-level)
  level_5_manager_name STRING,

  -- Organizational alignment
  business_unit STRING,         -- CAN, US-WEST, US-EAST, EMEA, APJ
  region STRING,                -- Americas, EMEA, APJ
  geo STRING,                   -- NA, LATAM, EMEA, APJ
  segment STRING,               -- ENT, COMM, MM, SMB
  vertical STRING,              -- FSI, Healthcare, Retail, Tech, etc.

  -- Specialization
  role_type STRING,             -- SSA, SA, CSM, TAM
  specialization STRING,        -- AI/ML, Data Eng, Platform, etc.

  -- Status
  is_active BOOLEAN,
  hire_date DATE,

  -- Metadata
  synced_at TIMESTAMP,

  PRIMARY KEY (ssa_id)
);
```

#### `dim_account` - Customer with Segment Dimensions

```sql
CREATE TABLE cjc_aws_workspace_catalog.ssa_ops.dim_account (
  -- Primary key
  account_id STRING NOT NULL,

  -- Identity
  account_name STRING,
  account_number STRING,

  -- Geographic
  region STRING,                -- Americas, EMEA, APJ
  country STRING,
  state_province STRING,

  -- Segment
  segment STRING,               -- ENT, COMM, MM, SMB
  vertical STRING,              -- FSI, Healthcare, Retail, etc.
  sub_vertical STRING,          -- More specific industry

  -- Account team
  ae_id STRING,
  ae_name STRING,
  rae_id STRING,
  rae_name STRING,

  -- Product adoption (flags)
  has_model_serving BOOLEAN,
  has_feature_store BOOLEAN,
  has_mlflow BOOLEAN,
  has_vector_search BOOLEAN,
  has_dlt BOOLEAN,
  has_serverless_sql BOOLEAN,
  has_unity_catalog BOOLEAN,
  has_mosaic_ai BOOLEAN,

  -- Scores
  ai_ml_score DECIMAL(5,2),
  modern_platform_score DECIMAL(5,2),
  adoption_tier STRING,         -- Advanced, Growing, Early, Basic

  -- Financials
  arr DECIMAL(15,2),
  arr_band STRING,              -- $0-100K, $100K-500K, $500K-1M, $1M+
  dbu_consumption_monthly DECIMAL(15,2),

  -- Status
  is_active BOOLEAN,
  customer_since DATE,

  -- Metadata
  synced_at TIMESTAMP,

  PRIMARY KEY (account_id)
);
```

#### `dim_date` - Fiscal and Calendar Date

```sql
CREATE TABLE cjc_aws_workspace_catalog.ssa_ops.dim_date (
  date_key DATE NOT NULL,

  -- Calendar
  cal_year INT,
  cal_quarter INT,
  cal_quarter_name STRING,      -- Q1, Q2, Q3, Q4
  cal_month INT,
  cal_month_name STRING,        -- January, February, etc.
  cal_week INT,
  cal_day_of_week INT,
  cal_day_name STRING,          -- Monday, Tuesday, etc.
  cal_is_weekend BOOLEAN,

  -- Fiscal (Databricks FY ends Jan 31)
  fy_year INT,                  -- FY25, FY26
  fy_quarter INT,
  fy_quarter_name STRING,       -- FYQ1, FYQ2, FYQ3, FYQ4
  fy_month INT,
  fy_week INT,

  -- Relative
  is_current_week BOOLEAN,
  is_current_month BOOLEAN,
  is_current_quarter BOOLEAN,
  is_current_fy BOOLEAN,
  is_ytd BOOLEAN,               -- FY year-to-date
  is_qtd BOOLEAN,               -- FY quarter-to-date
  is_mtd BOOLEAN,               -- Month-to-date

  -- Prior period
  same_day_last_year DATE,
  same_day_last_quarter DATE,
  same_day_last_month DATE,

  PRIMARY KEY (date_key)
);
```

### 3.2 Fact Tables

#### `fact_asq` - All ASQs (All BUs, All Time)

```sql
CREATE TABLE cjc_aws_workspace_catalog.ssa_ops.fact_asq (
  -- Primary key
  asq_id STRING NOT NULL,

  -- Natural key
  asq_number STRING,
  asq_name STRING,

  -- Status
  status STRING,
  is_open BOOLEAN,
  is_overdue BOOLEAN,

  -- Foreign keys
  owner_id STRING,              -- → dim_ssa.ssa_id
  account_id STRING,            -- → dim_account.account_id
  created_date_key DATE,        -- → dim_date.date_key
  assigned_date_key DATE,
  due_date_key DATE,
  completed_date_key DATE,

  -- Raw dates (for calculations)
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
  request_source STRING,        -- CSM, AE, Customer Direct

  -- Effort
  estimated_effort_days DECIMAL(5,2),
  actual_effort_days DECIMAL(5,2),

  -- Quality
  has_quality_closure BOOLEAN,
  has_artifacts BOOLEAN,
  has_follow_up BOOLEAN,
  customer_satisfaction STRING,

  -- Linkage
  linked_uco_id STRING,         -- → fact_uco.uco_id
  linked_opportunity_id STRING,
  linked_case_id STRING,

  -- SLA milestones (calculated)
  days_to_assignment INT,
  days_to_first_note INT,
  days_to_completion INT,
  days_open INT,                -- For open ASQs
  days_until_due INT,           -- Days until/past due date

  -- SLA compliance flags
  review_sla_met BOOLEAN,       -- First touch within 2 days
  assignment_sla_met BOOLEAN,   -- Assigned within 5 days
  response_sla_met BOOLEAN,     -- First note within 5 days
  completion_sla_met BOOLEAN,   -- Completed by due date

  -- Metadata
  synced_at TIMESTAMP,

  PRIMARY KEY (asq_id)
);
```

#### `fact_uco` - UCO Pipeline

```sql
CREATE TABLE cjc_aws_workspace_catalog.ssa_ops.fact_uco (
  -- Primary key
  uco_id STRING NOT NULL,

  -- Identity
  uco_name STRING,

  -- Foreign keys
  account_id STRING,
  owner_id STRING,

  -- Status
  status STRING,                -- Open, Won, Lost, Deferred
  stage STRING,

  -- Financials
  estimated_dbus DECIMAL(15,2),
  estimated_arr DECIMAL(15,2),
  weighted_arr DECIMAL(15,2),

  -- Dates
  created_date TIMESTAMP,
  close_date TIMESTAMP,

  -- Metadata
  synced_at TIMESTAMP,

  PRIMARY KEY (uco_id)
);
```

---

## 4. Metric View Definitions

### 4.1 Core Metrics

#### `mv_asq_operations` - Primary ASQ Operations

This is the main metric view with **no hardcoded filters** - all filtering happens at query time.

```yaml
version: 1.1
comment: |
  Core ASQ operational metrics. Filter by business_unit, manager hierarchy,
  or any dimension at query time. No hardcoded team filters.

source: cjc_aws_workspace_catalog.ssa_ops.fact_asq

joins:
  - name: ssa
    source: cjc_aws_workspace_catalog.ssa_ops.dim_ssa
    on: source.owner_id = ssa.ssa_id
  - name: account
    source: cjc_aws_workspace_catalog.ssa_ops.dim_account
    on: source.account_id = account.account_id
  - name: created_dt
    source: cjc_aws_workspace_catalog.ssa_ops.dim_date
    on: source.created_date_key = created_dt.date_key

# NO GLOBAL FILTER - allows multi-BU queries
# Filter at query time: WHERE `Business Unit` = 'CAN'

dimensions:
  # === ORGANIZATIONAL HIERARCHY ===
  - name: Business Unit
    expr: ssa.business_unit
    comment: "BU code: CAN, US-WEST, US-EAST, EMEA, APJ"
  - name: Region
    expr: ssa.region
    comment: "Region: Americas, EMEA, APJ"
  - name: Geo
    expr: ssa.geo
    comment: "Geography: NA, LATAM, EMEA, APJ"

  # === MANAGER HIERARCHY (multi-level) ===
  - name: Owner
    expr: ssa.ssa_name
    comment: "SSA owner (individual contributor)"
  - name: Manager L1
    expr: ssa.level_1_manager_name
    comment: "Direct manager"
  - name: Manager L2
    expr: ssa.level_2_manager_name
    comment: "Second-level manager (e.g., RVP)"
  - name: Manager L3
    expr: ssa.level_3_manager_name
    comment: "Third-level manager (e.g., AVP)"
  - name: Manager L4
    expr: ssa.level_4_manager_name
    comment: "Fourth-level manager (e.g., VP)"
  - name: Manager L5
    expr: ssa.level_5_manager_name
    comment: "Fifth-level manager (e.g., SVP)"
  - name: Manager L1 ID
    expr: ssa.level_1_manager_id
    comment: "Direct manager ID (for filtering)"
  - name: Manager L2 ID
    expr: ssa.level_2_manager_id
    comment: "Second-level manager ID"

  # === CUSTOMER DIMENSIONS ===
  - name: Account
    expr: account.account_name
  - name: Account Segment
    expr: account.segment
    comment: "ENT, COMM, MM, SMB"
  - name: Account Vertical
    expr: account.vertical
    comment: "FSI, Healthcare, Retail, Tech, etc."
  - name: Account Sub-Vertical
    expr: account.sub_vertical
  - name: Account Region
    expr: account.region
  - name: Account Country
    expr: account.country
  - name: ARR Band
    expr: account.arr_band
    comment: "Customer ARR tier"
  - name: Adoption Tier
    expr: account.adoption_tier
    comment: "Product adoption level"

  # === WORK CLASSIFICATION ===
  - name: ASQ Status
    expr: source.status
  - name: Is Open
    expr: CASE WHEN source.is_open THEN 'Open' ELSE 'Closed' END
  - name: Is Overdue
    expr: CASE WHEN source.is_overdue THEN 'Overdue' ELSE 'On Track' END
  - name: Specialization
    expr: source.specialization
  - name: Support Type
    expr: source.support_type
  - name: Priority
    expr: source.priority
  - name: Complexity
    expr: source.complexity
  - name: Request Source
    expr: source.request_source

  # === TIME DIMENSIONS (Calendar) ===
  - name: Created Year
    expr: created_dt.cal_year
  - name: Created Quarter
    expr: created_dt.cal_quarter_name
  - name: Created Year-Quarter
    expr: CONCAT(created_dt.cal_year, '-', created_dt.cal_quarter_name)
  - name: Created Month
    expr: DATE_TRUNC('MONTH', source.created_date)
  - name: Created Week
    expr: DATE_TRUNC('WEEK', source.created_date)
  - name: Created Date
    expr: source.created_date_key
  - name: Created Day of Week
    expr: created_dt.cal_day_name

  # === TIME DIMENSIONS (Fiscal) ===
  - name: Fiscal Year
    expr: created_dt.fy_year
  - name: Fiscal Quarter
    expr: created_dt.fy_quarter_name
  - name: Fiscal Year-Quarter
    expr: CONCAT('FY', created_dt.fy_year, '-', created_dt.fy_quarter_name)

  # === RELATIVE TIME ===
  - name: Is Current FY
    expr: CASE WHEN created_dt.is_current_fy THEN 'Current FY' ELSE 'Prior FY' END
  - name: Is YTD
    expr: CASE WHEN created_dt.is_ytd THEN 'YTD' ELSE 'Prior' END
  - name: Is QTD
    expr: CASE WHEN created_dt.is_qtd THEN 'QTD' ELSE 'Prior' END

measures:
  # === VOLUME MEASURES ===
  - name: Total ASQs
    expr: COUNT(1)
    comment: "Total number of ASQs"
  - name: Open ASQs
    expr: COUNT(1) FILTER (WHERE source.is_open)
    comment: "Currently open ASQs"
  - name: Closed ASQs
    expr: COUNT(1) FILTER (WHERE NOT source.is_open)
    comment: "Completed or closed ASQs"
  - name: Overdue ASQs
    expr: COUNT(1) FILTER (WHERE source.is_overdue)
    comment: "Past due date, not completed"
  - name: At Risk ASQs
    expr: COUNT(1) FILTER (WHERE source.days_until_due BETWEEN 0 AND 3 AND source.is_open)
    comment: "Due within 3 days"

  # === COMPLETION MEASURES ===
  - name: Completed ASQs
    expr: COUNT(1) FILTER (WHERE source.status IN ('Completed', 'Closed'))
  - name: On-Time Completions
    expr: COUNT(1) FILTER (WHERE source.completion_sla_met)
    comment: "Completed on or before due date"
  - name: Late Completions
    expr: COUNT(1) FILTER (WHERE source.completion_date IS NOT NULL AND NOT source.completion_sla_met)

  # === RATE MEASURES (safe re-aggregation) ===
  - name: On-Time Rate
    expr: |
      COUNT(1) FILTER (WHERE source.completion_sla_met) * 1.0
      / NULLIF(COUNT(1) FILTER (WHERE source.completion_date IS NOT NULL), 0)
    comment: "% of ASQs completed on time"
  - name: Overdue Rate
    expr: |
      COUNT(1) FILTER (WHERE source.is_overdue) * 1.0
      / NULLIF(COUNT(1) FILTER (WHERE source.is_open), 0)
    comment: "% of open ASQs that are overdue"
  - name: Completion Rate
    expr: |
      COUNT(1) FILTER (WHERE NOT source.is_open) * 1.0
      / NULLIF(COUNT(1), 0)
    comment: "% of ASQs completed (closed)"

  # === TURNAROUND MEASURES ===
  - name: Avg Days to Complete
    expr: AVG(source.days_to_completion)
    comment: "Average turnaround time in days"
  - name: Median Days to Complete
    expr: PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY source.days_to_completion)
    comment: "Median turnaround time"
  - name: P90 Days to Complete
    expr: PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY source.days_to_completion)
    comment: "90th percentile turnaround"
  - name: Avg Days to Assignment
    expr: AVG(source.days_to_assignment)
  - name: Avg Days to First Note
    expr: AVG(source.days_to_first_note)
  - name: Avg Days Open
    expr: AVG(source.days_open)
    comment: "For open ASQs, days since creation"

  # === VOLUME DISTRIBUTION ===
  - name: Unique Accounts
    expr: COUNT(DISTINCT source.account_id)
  - name: Unique SSAs
    expr: COUNT(DISTINCT source.owner_id)
  - name: ASQs per SSA
    expr: COUNT(1) * 1.0 / NULLIF(COUNT(DISTINCT source.owner_id), 0)
    comment: "Average load per SSA"
  - name: ASQs per Account
    expr: COUNT(1) * 1.0 / NULLIF(COUNT(DISTINCT source.account_id), 0)
    comment: "Average ASQs per customer"
```

#### `mv_sla_compliance` - SLA Milestone Tracking

```yaml
version: 1.1
comment: |
  SLA compliance metrics across all milestones.
  Track review, assignment, response, and completion SLAs by any dimension.

source: cjc_aws_workspace_catalog.ssa_ops.fact_asq

joins:
  - name: ssa
    source: cjc_aws_workspace_catalog.ssa_ops.dim_ssa
    on: source.owner_id = ssa.ssa_id
  - name: created_dt
    source: cjc_aws_workspace_catalog.ssa_ops.dim_date
    on: source.created_date_key = created_dt.date_key

dimensions:
  # === ORGANIZATIONAL ===
  - name: Business Unit
    expr: ssa.business_unit
  - name: Manager L1
    expr: ssa.level_1_manager_name
  - name: Manager L2
    expr: ssa.level_2_manager_name
  - name: Owner
    expr: ssa.ssa_name

  # === TIME ===
  - name: Created Month
    expr: DATE_TRUNC('MONTH', source.created_date)
  - name: Fiscal Quarter
    expr: created_dt.fy_quarter_name
  - name: Fiscal Year
    expr: created_dt.fy_year

  # === STATUS ===
  - name: ASQ Status
    expr: source.status
  - name: SLA Stage
    expr: |
      CASE
        WHEN source.completion_date IS NOT NULL THEN 'Complete'
        WHEN source.assignment_date IS NOT NULL THEN 'In Progress'
        WHEN source.first_note_date IS NOT NULL THEN 'Under Review'
        ELSE 'Submitted'
      END

measures:
  # === VOLUME ===
  - name: Total ASQs
    expr: COUNT(1)

  # === REVIEW SLA (2 business days to first touch) ===
  - name: Review SLA Met
    expr: COUNT(1) FILTER (WHERE source.review_sla_met)
    comment: "Review started within 2 business days"
  - name: Review SLA Missed
    expr: COUNT(1) FILTER (WHERE NOT source.review_sla_met)
  - name: Review SLA Rate
    expr: |
      COUNT(1) FILTER (WHERE source.review_sla_met) * 1.0
      / NULLIF(COUNT(1), 0)
  - name: Avg Days to Review
    expr: AVG(LEAST(source.days_to_assignment, source.days_to_first_note))

  # === ASSIGNMENT SLA (5 business days) ===
  - name: Assignment SLA Met
    expr: COUNT(1) FILTER (WHERE source.assignment_sla_met)
    comment: "Assigned within 5 business days"
  - name: Assignment SLA Missed
    expr: COUNT(1) FILTER (WHERE source.assignment_date IS NOT NULL AND NOT source.assignment_sla_met)
  - name: Assignment SLA Rate
    expr: |
      COUNT(1) FILTER (WHERE source.assignment_sla_met) * 1.0
      / NULLIF(COUNT(1) FILTER (WHERE source.assignment_date IS NOT NULL), 0)
  - name: Avg Days to Assignment
    expr: AVG(source.days_to_assignment)

  # === RESPONSE SLA (5 days to first note) ===
  - name: Response SLA Met
    expr: COUNT(1) FILTER (WHERE source.response_sla_met)
    comment: "First response within 5 days"
  - name: Response SLA Missed
    expr: COUNT(1) FILTER (WHERE source.first_note_date IS NOT NULL AND NOT source.response_sla_met)
  - name: Response SLA Rate
    expr: |
      COUNT(1) FILTER (WHERE source.response_sla_met) * 1.0
      / NULLIF(COUNT(1) FILTER (WHERE source.first_note_date IS NOT NULL), 0)
  - name: Avg Days to Response
    expr: AVG(source.days_to_first_note)

  # === COMPLETION SLA (by due date) ===
  - name: Completion SLA Met
    expr: COUNT(1) FILTER (WHERE source.completion_sla_met)
    comment: "Completed on or before due date"
  - name: Completion SLA Missed
    expr: COUNT(1) FILTER (WHERE source.completion_date IS NOT NULL AND NOT source.completion_sla_met)
  - name: Completion SLA Rate
    expr: |
      COUNT(1) FILTER (WHERE source.completion_sla_met) * 1.0
      / NULLIF(COUNT(1) FILTER (WHERE source.completion_date IS NOT NULL), 0)

  # === COMBINED SLA SCORE ===
  - name: All SLAs Met
    expr: |
      COUNT(1) FILTER (WHERE
        source.review_sla_met
        AND source.assignment_sla_met
        AND source.response_sla_met
        AND source.completion_sla_met
      )
  - name: Perfect SLA Rate
    expr: |
      COUNT(1) FILTER (WHERE
        source.review_sla_met
        AND source.assignment_sla_met
        AND source.response_sla_met
        AND source.completion_sla_met
      ) * 1.0 / NULLIF(COUNT(1) FILTER (WHERE source.completion_date IS NOT NULL), 0)
    comment: "% of ASQs that met all 4 SLA milestones"
```

### 4.2 Extended Metrics

#### `mv_effort_capacity` - Effort Estimation & Capacity

```yaml
version: 1.1
comment: |
  Effort estimation accuracy and capacity planning metrics.
  Use for workload balancing and estimation calibration.

source: cjc_aws_workspace_catalog.ssa_ops.fact_asq

joins:
  - name: ssa
    source: cjc_aws_workspace_catalog.ssa_ops.dim_ssa
    on: source.owner_id = ssa.ssa_id
  - name: created_dt
    source: cjc_aws_workspace_catalog.ssa_ops.dim_date
    on: source.created_date_key = created_dt.date_key

filter: source.completion_date IS NOT NULL

dimensions:
  # === ORGANIZATIONAL ===
  - name: Business Unit
    expr: ssa.business_unit
  - name: Manager L1
    expr: ssa.level_1_manager_name
  - name: Manager L2
    expr: ssa.level_2_manager_name
  - name: Owner
    expr: ssa.ssa_name

  # === WORK CLASSIFICATION ===
  - name: Specialization
    expr: source.specialization
  - name: Support Type
    expr: source.support_type
  - name: Complexity
    expr: source.complexity

  # === TIME ===
  - name: Completed Quarter
    expr: |
      CONCAT(EXTRACT(YEAR FROM source.completion_date), '-Q',
             EXTRACT(QUARTER FROM source.completion_date))
  - name: Completed Month
    expr: DATE_TRUNC('MONTH', source.completion_date)
  - name: Fiscal Quarter
    expr: created_dt.fy_quarter_name

  # === ACCURACY BANDS ===
  - name: Accuracy Category
    expr: |
      CASE
        WHEN source.estimated_effort_days IS NULL OR source.actual_effort_days IS NULL THEN 'No Data'
        WHEN source.actual_effort_days <= source.estimated_effort_days * 0.8 THEN 'Under Actual'
        WHEN source.actual_effort_days <= source.estimated_effort_days * 1.2 THEN 'Accurate'
        WHEN source.actual_effort_days <= source.estimated_effort_days * 1.5 THEN 'Slight Over'
        ELSE 'Significant Over'
      END
    comment: "How accurate was the effort estimate"
  - name: Effort Band
    expr: |
      CASE
        WHEN source.actual_effort_days IS NULL THEN 'Unknown'
        WHEN source.actual_effort_days <= 1 THEN '0-1 days'
        WHEN source.actual_effort_days <= 3 THEN '1-3 days'
        WHEN source.actual_effort_days <= 5 THEN '3-5 days'
        WHEN source.actual_effort_days <= 10 THEN '5-10 days'
        ELSE '10+ days'
      END

measures:
  # === VOLUME ===
  - name: Total Completed
    expr: COUNT(1)
  - name: With Effort Data
    expr: COUNT(1) FILTER (WHERE source.actual_effort_days IS NOT NULL)

  # === EFFORT TOTALS ===
  - name: Total Estimated Days
    expr: SUM(source.estimated_effort_days)
  - name: Total Actual Days
    expr: SUM(source.actual_effort_days)
  - name: Total Variance Days
    expr: SUM(source.actual_effort_days - source.estimated_effort_days)
    comment: "Positive = underestimated, Negative = overestimated"

  # === EFFORT AVERAGES ===
  - name: Avg Estimated Days
    expr: AVG(source.estimated_effort_days)
  - name: Avg Actual Days
    expr: AVG(source.actual_effort_days)
  - name: Median Actual Days
    expr: PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY source.actual_effort_days)

  # === ACCURACY METRICS ===
  - name: Avg Effort Ratio
    expr: AVG(source.actual_effort_days / NULLIF(source.estimated_effort_days, 0))
    comment: "Actual / Estimated (1.0 = perfect, >1.0 = underestimated)"
  - name: Median Effort Ratio
    expr: |
      PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY source.actual_effort_days / NULLIF(source.estimated_effort_days, 0)
      )
  - name: Accuracy Rate
    expr: |
      COUNT(1) FILTER (WHERE
        source.actual_effort_days / NULLIF(source.estimated_effort_days, 0) BETWEEN 0.8 AND 1.2
      ) * 1.0
      / NULLIF(COUNT(1) FILTER (WHERE source.estimated_effort_days IS NOT NULL), 0)
    comment: "% within 20% of estimate"
  - name: Under-Estimation Count
    expr: |
      COUNT(1) FILTER (WHERE
        source.actual_effort_days > source.estimated_effort_days * 1.2
      )
  - name: Over-Estimation Count
    expr: |
      COUNT(1) FILTER (WHERE
        source.actual_effort_days < source.estimated_effort_days * 0.8
      )

  # === CAPACITY METRICS ===
  - name: Avg Days per SSA per Month
    expr: |
      SUM(source.actual_effort_days) * 1.0
      / NULLIF(COUNT(DISTINCT source.owner_id), 0)
    comment: "Average effort load per SSA"
  - name: Peak SSA Load
    expr: MAX(source.actual_effort_days)
    comment: "Largest single ASQ effort"
```

#### `mv_customer_engagement` - Account-Level Metrics

```yaml
version: 1.1
comment: |
  Customer engagement patterns and re-engagement tracking.
  Analyze by account segments, verticals, and adoption tiers.

source: cjc_aws_workspace_catalog.ssa_ops.fact_asq

joins:
  - name: account
    source: cjc_aws_workspace_catalog.ssa_ops.dim_account
    on: source.account_id = account.account_id
  - name: ssa
    source: cjc_aws_workspace_catalog.ssa_ops.dim_ssa
    on: source.owner_id = ssa.ssa_id
  - name: created_dt
    source: cjc_aws_workspace_catalog.ssa_ops.dim_date
    on: source.created_date_key = created_dt.date_key

dimensions:
  # === ORGANIZATIONAL ===
  - name: Business Unit
    expr: ssa.business_unit
  - name: Manager L1
    expr: ssa.level_1_manager_name
  - name: Manager L2
    expr: ssa.level_2_manager_name

  # === CUSTOMER DIMENSIONS ===
  - name: Account
    expr: account.account_name
  - name: Account Segment
    expr: account.segment
    comment: "ENT, COMM, MM, SMB"
  - name: Account Vertical
    expr: account.vertical
    comment: "FSI, Healthcare, Retail, Tech, etc."
  - name: Account Sub-Vertical
    expr: account.sub_vertical
  - name: Account Region
    expr: account.region
  - name: Account Country
    expr: account.country
  - name: ARR Band
    expr: account.arr_band
  - name: Adoption Tier
    expr: account.adoption_tier
    comment: "Advanced, Growing, Early, Basic"

  # === ENGAGEMENT TIERS ===
  - name: Engagement Tier
    expr: |
      CASE
        WHEN COUNT(1) >= 10 THEN 'Strategic'
        WHEN COUNT(1) >= 5 THEN 'High Engagement'
        WHEN COUNT(1) >= 2 THEN 'Repeat Customer'
        ELSE 'Single Engagement'
      END
    comment: "Based on total ASQ count"

  # === TIME ===
  - name: Created Year
    expr: created_dt.cal_year
  - name: Fiscal Year
    expr: created_dt.fy_year
  - name: Fiscal Quarter
    expr: created_dt.fy_quarter_name

  # === PRODUCT ADOPTION FLAGS ===
  - name: Has AI/ML
    expr: |
      CASE WHEN account.has_model_serving OR account.has_mlflow
           OR account.has_vector_search THEN 'Yes' ELSE 'No' END
  - name: Has Modern Platform
    expr: |
      CASE WHEN account.has_unity_catalog AND account.has_serverless_sql
           THEN 'Yes' ELSE 'No' END

measures:
  # === VOLUME ===
  - name: Total ASQs
    expr: COUNT(1)
  - name: Unique Accounts
    expr: COUNT(DISTINCT source.account_id)

  # === ENGAGEMENT METRICS ===
  - name: Single-ASQ Accounts
    expr: |
      COUNT(DISTINCT source.account_id) FILTER (WHERE
        source.account_id IN (
          SELECT account_id FROM cjc_aws_workspace_catalog.ssa_ops.fact_asq
          GROUP BY account_id HAVING COUNT(1) = 1
        )
      )
  - name: Repeat Customers
    expr: |
      COUNT(DISTINCT source.account_id) FILTER (WHERE
        source.account_id IN (
          SELECT account_id FROM cjc_aws_workspace_catalog.ssa_ops.fact_asq
          GROUP BY account_id HAVING COUNT(1) >= 2
        )
      )
    comment: "Accounts with 2+ ASQs"
  - name: High Engagement Accounts
    expr: |
      COUNT(DISTINCT source.account_id) FILTER (WHERE
        source.account_id IN (
          SELECT account_id FROM cjc_aws_workspace_catalog.ssa_ops.fact_asq
          GROUP BY account_id HAVING COUNT(1) >= 5
        )
      )
    comment: "Accounts with 5+ ASQs"

  # === RATES ===
  - name: Repeat Customer Rate
    expr: |
      COUNT(DISTINCT source.account_id) FILTER (WHERE
        source.account_id IN (
          SELECT account_id FROM cjc_aws_workspace_catalog.ssa_ops.fact_asq
          GROUP BY account_id HAVING COUNT(1) >= 2
        )
      ) * 1.0 / NULLIF(COUNT(DISTINCT source.account_id), 0)
  - name: High Engagement Rate
    expr: |
      COUNT(DISTINCT source.account_id) FILTER (WHERE
        source.account_id IN (
          SELECT account_id FROM cjc_aws_workspace_catalog.ssa_ops.fact_asq
          GROUP BY account_id HAVING COUNT(1) >= 5
        )
      ) * 1.0 / NULLIF(COUNT(DISTINCT source.account_id), 0)

  # === DISTRIBUTION ===
  - name: ASQs per Account
    expr: COUNT(1) * 1.0 / NULLIF(COUNT(DISTINCT source.account_id), 0)
  - name: Unique SSAs per Account
    expr: COUNT(DISTINCT source.owner_id) * 1.0 / NULLIF(COUNT(DISTINCT source.account_id), 0)
    comment: "Average SSAs touching each account"
  - name: Accounts per SSA
    expr: COUNT(DISTINCT source.account_id) * 1.0 / NULLIF(COUNT(DISTINCT source.owner_id), 0)

  # === FINANCIAL CORRELATION ===
  - name: Total Account ARR
    expr: SUM(DISTINCT account.arr)
    comment: "Sum of ARR for engaged accounts"
  - name: Avg Account ARR
    expr: AVG(DISTINCT account.arr)
  - name: High ARR Accounts
    expr: COUNT(DISTINCT source.account_id) FILTER (WHERE account.arr >= 1000000)
    comment: "Accounts with $1M+ ARR"
```

#### `mv_pipeline_impact` - UCO Linkage & Business Impact

```yaml
version: 1.1
comment: |
  UCO linkage and business impact metrics.
  Track how ASQ work influences pipeline and revenue.

source: cjc_aws_workspace_catalog.ssa_ops.fact_asq

joins:
  - name: ssa
    source: cjc_aws_workspace_catalog.ssa_ops.dim_ssa
    on: source.owner_id = ssa.ssa_id
  - name: uco
    source: cjc_aws_workspace_catalog.ssa_ops.fact_uco
    on: source.linked_uco_id = uco.uco_id
  - name: account
    source: cjc_aws_workspace_catalog.ssa_ops.dim_account
    on: source.account_id = account.account_id
  - name: created_dt
    source: cjc_aws_workspace_catalog.ssa_ops.dim_date
    on: source.created_date_key = created_dt.date_key

dimensions:
  # === ORGANIZATIONAL ===
  - name: Business Unit
    expr: ssa.business_unit
  - name: Manager L1
    expr: ssa.level_1_manager_name
  - name: Manager L2
    expr: ssa.level_2_manager_name
  - name: Owner
    expr: ssa.ssa_name

  # === UCO STATUS ===
  - name: UCO Status
    expr: COALESCE(uco.status, 'No UCO Link')
  - name: UCO Stage
    expr: COALESCE(uco.stage, 'N/A')
  - name: Linkage Status
    expr: |
      CASE
        WHEN source.linked_uco_id IS NOT NULL AND uco.status = 'Won' THEN 'Won UCO'
        WHEN source.linked_uco_id IS NOT NULL AND uco.status = 'Open' THEN 'Open UCO'
        WHEN source.linked_uco_id IS NOT NULL THEN 'Linked UCO'
        ELSE 'No UCO Link'
      END

  # === CUSTOMER ===
  - name: Account
    expr: account.account_name
  - name: Account Segment
    expr: account.segment
  - name: Account Vertical
    expr: account.vertical

  # === TIME ===
  - name: Completed Quarter
    expr: |
      CONCAT(EXTRACT(YEAR FROM source.completion_date), '-Q',
             EXTRACT(QUARTER FROM source.completion_date))
  - name: Fiscal Year
    expr: created_dt.fy_year
  - name: Fiscal Quarter
    expr: created_dt.fy_quarter_name

  # === WORK ===
  - name: Specialization
    expr: source.specialization
  - name: Support Type
    expr: source.support_type

measures:
  # === VOLUME ===
  - name: Total ASQs
    expr: COUNT(1)
  - name: Linked ASQs
    expr: COUNT(1) FILTER (WHERE source.linked_uco_id IS NOT NULL)
  - name: Unlinked ASQs
    expr: COUNT(1) FILTER (WHERE source.linked_uco_id IS NULL)

  # === LINKAGE RATES ===
  - name: Linkage Rate
    expr: |
      COUNT(1) FILTER (WHERE source.linked_uco_id IS NOT NULL) * 1.0
      / NULLIF(COUNT(1), 0)
    comment: "% of ASQs linked to UCOs"
  - name: Won UCO Rate
    expr: |
      COUNT(1) FILTER (WHERE uco.status = 'Won') * 1.0
      / NULLIF(COUNT(1) FILTER (WHERE source.linked_uco_id IS NOT NULL), 0)
    comment: "% of linked UCOs that are won"

  # === PIPELINE VALUE ===
  - name: Total Linked DBUs
    expr: SUM(uco.estimated_dbus)
    comment: "Total DBUs from linked UCOs"
  - name: Total Linked ARR
    expr: SUM(uco.estimated_arr)
    comment: "Total ARR from linked UCOs"
  - name: Total Weighted ARR
    expr: SUM(uco.weighted_arr)
    comment: "Probability-weighted ARR"
  - name: Won ARR
    expr: SUM(uco.estimated_arr) FILTER (WHERE uco.status = 'Won')
    comment: "ARR from won UCOs"

  # === PER-ASQ VALUE ===
  - name: Avg DBUs per Linked ASQ
    expr: AVG(uco.estimated_dbus)
  - name: Avg ARR per Linked ASQ
    expr: AVG(uco.estimated_arr)
  - name: Avg ARR per ASQ
    expr: |
      SUM(uco.estimated_arr) * 1.0 / NULLIF(COUNT(1), 0)
    comment: "Total linked ARR / Total ASQs"

  # === UCO COUNTS ===
  - name: Unique UCOs
    expr: COUNT(DISTINCT source.linked_uco_id)
  - name: Open UCOs
    expr: COUNT(DISTINCT source.linked_uco_id) FILTER (WHERE uco.status = 'Open')
  - name: Won UCOs
    expr: COUNT(DISTINCT source.linked_uco_id) FILTER (WHERE uco.status = 'Won')
  - name: Lost UCOs
    expr: COUNT(DISTINCT source.linked_uco_id) FILTER (WHERE uco.status = 'Lost')
```

### 4.3 Advanced Metrics

#### `mv_team_comparison` - Cross-Team Benchmarking

```yaml
version: 1.1
comment: |
  Cross-team and cross-BU comparison metrics.
  Use for benchmarking and best practice identification.

source: cjc_aws_workspace_catalog.ssa_ops.fact_asq

joins:
  - name: ssa
    source: cjc_aws_workspace_catalog.ssa_ops.dim_ssa
    on: source.owner_id = ssa.ssa_id
  - name: created_dt
    source: cjc_aws_workspace_catalog.ssa_ops.dim_date
    on: source.created_date_key = created_dt.date_key

dimensions:
  # === COMPARISON LEVELS ===
  - name: Business Unit
    expr: ssa.business_unit
  - name: Region
    expr: ssa.region
  - name: Geo
    expr: ssa.geo
  - name: Manager L2
    expr: ssa.level_2_manager_name
    comment: "Compare teams at RVP level"
  - name: Manager L3
    expr: ssa.level_3_manager_name
    comment: "Compare teams at AVP level"

  # === TIME ===
  - name: Fiscal Year
    expr: created_dt.fy_year
  - name: Fiscal Quarter
    expr: created_dt.fy_quarter_name
  - name: Is Current FY
    expr: CASE WHEN created_dt.is_current_fy THEN 'Current FY' ELSE 'Prior FY' END

measures:
  # === VOLUME (for percentile calc) ===
  - name: Total ASQs
    expr: COUNT(1)
  - name: Completed ASQs
    expr: COUNT(1) FILTER (WHERE source.completion_date IS NOT NULL)
  - name: Unique SSAs
    expr: COUNT(DISTINCT source.owner_id)
  - name: Unique Accounts
    expr: COUNT(DISTINCT source.account_id)

  # === PERFORMANCE ===
  - name: On-Time Rate
    expr: |
      COUNT(1) FILTER (WHERE source.completion_sla_met) * 1.0
      / NULLIF(COUNT(1) FILTER (WHERE source.completion_date IS NOT NULL), 0)
  - name: Avg Days to Complete
    expr: AVG(source.days_to_completion)
  - name: Perfect SLA Rate
    expr: |
      COUNT(1) FILTER (WHERE
        source.review_sla_met
        AND source.assignment_sla_met
        AND source.response_sla_met
        AND source.completion_sla_met
      ) * 1.0 / NULLIF(COUNT(1) FILTER (WHERE source.completion_date IS NOT NULL), 0)

  # === PRODUCTIVITY ===
  - name: ASQs per SSA
    expr: COUNT(1) * 1.0 / NULLIF(COUNT(DISTINCT source.owner_id), 0)
  - name: Accounts per SSA
    expr: COUNT(DISTINCT source.account_id) * 1.0 / NULLIF(COUNT(DISTINCT source.owner_id), 0)
  - name: Total Effort Days
    expr: SUM(source.actual_effort_days)
  - name: Avg Effort per ASQ
    expr: AVG(source.actual_effort_days)

  # === BUSINESS IMPACT ===
  - name: Linkage Rate
    expr: |
      COUNT(1) FILTER (WHERE source.linked_uco_id IS NOT NULL) * 1.0
      / NULLIF(COUNT(1), 0)
  - name: Repeat Customer Rate
    expr: |
      COUNT(DISTINCT source.account_id) FILTER (WHERE
        source.account_id IN (
          SELECT account_id FROM cjc_aws_workspace_catalog.ssa_ops.fact_asq
          GROUP BY account_id HAVING COUNT(1) >= 2
        )
      ) * 1.0 / NULLIF(COUNT(DISTINCT source.account_id), 0)
```

#### `mv_aging_backlog` - Backlog & Aging Analysis

```yaml
version: 1.1
comment: |
  Backlog aging and queue health metrics.
  Track open ASQs by age bands and identify bottlenecks.

source: cjc_aws_workspace_catalog.ssa_ops.fact_asq

joins:
  - name: ssa
    source: cjc_aws_workspace_catalog.ssa_ops.dim_ssa
    on: source.owner_id = ssa.ssa_id

filter: source.is_open = true

dimensions:
  # === ORGANIZATIONAL ===
  - name: Business Unit
    expr: ssa.business_unit
  - name: Manager L1
    expr: ssa.level_1_manager_name
  - name: Manager L2
    expr: ssa.level_2_manager_name
  - name: Owner
    expr: ssa.ssa_name

  # === AGING BANDS ===
  - name: Age Band
    expr: |
      CASE
        WHEN source.days_open <= 7 THEN '0-7 days'
        WHEN source.days_open <= 14 THEN '8-14 days'
        WHEN source.days_open <= 30 THEN '15-30 days'
        WHEN source.days_open <= 60 THEN '31-60 days'
        WHEN source.days_open <= 90 THEN '61-90 days'
        ELSE '90+ days'
      END
  - name: Urgency Band
    expr: |
      CASE
        WHEN source.days_until_due < 0 THEN 'Overdue'
        WHEN source.days_until_due <= 3 THEN 'Due Soon (0-3 days)'
        WHEN source.days_until_due <= 7 THEN 'Due This Week'
        WHEN source.days_until_due <= 14 THEN 'Due Next Week'
        ELSE 'Not Urgent'
      END

  # === STATUS ===
  - name: ASQ Status
    expr: source.status
  - name: Is Overdue
    expr: CASE WHEN source.is_overdue THEN 'Overdue' ELSE 'On Track' END

  # === WORK ===
  - name: Specialization
    expr: source.specialization
  - name: Priority
    expr: source.priority

measures:
  # === BACKLOG COUNTS ===
  - name: Total Open
    expr: COUNT(1)
  - name: Overdue Count
    expr: COUNT(1) FILTER (WHERE source.is_overdue)
  - name: At Risk Count
    expr: COUNT(1) FILTER (WHERE source.days_until_due BETWEEN 0 AND 3)
    comment: "Due within 3 days"
  - name: Healthy Count
    expr: COUNT(1) FILTER (WHERE source.days_until_due > 7)

  # === AGING METRICS ===
  - name: Avg Days Open
    expr: AVG(source.days_open)
  - name: Max Days Open
    expr: MAX(source.days_open)
  - name: Median Days Open
    expr: PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY source.days_open)

  # === AGE DISTRIBUTION ===
  - name: Under 7 Days
    expr: COUNT(1) FILTER (WHERE source.days_open <= 7)
  - name: 8-14 Days
    expr: COUNT(1) FILTER (WHERE source.days_open BETWEEN 8 AND 14)
  - name: 15-30 Days
    expr: COUNT(1) FILTER (WHERE source.days_open BETWEEN 15 AND 30)
  - name: 31-60 Days
    expr: COUNT(1) FILTER (WHERE source.days_open BETWEEN 31 AND 60)
  - name: Over 60 Days
    expr: COUNT(1) FILTER (WHERE source.days_open > 60)

  # === HEALTH RATES ===
  - name: Overdue Rate
    expr: COUNT(1) FILTER (WHERE source.is_overdue) * 1.0 / NULLIF(COUNT(1), 0)
  - name: At Risk Rate
    expr: COUNT(1) FILTER (WHERE source.days_until_due BETWEEN 0 AND 3) * 1.0 / NULLIF(COUNT(1), 0)
  - name: Healthy Rate
    expr: COUNT(1) FILTER (WHERE source.days_until_due > 7) * 1.0 / NULLIF(COUNT(1), 0)

  # === DISTRIBUTION ===
  - name: Unique SSAs with Backlog
    expr: COUNT(DISTINCT source.owner_id)
  - name: Avg Backlog per SSA
    expr: COUNT(1) * 1.0 / NULLIF(COUNT(DISTINCT source.owner_id), 0)
```

#### `mv_quality_closure` - Quality & Closure Metrics

```yaml
version: 1.1
comment: |
  Quality of closure and outcome metrics.
  Track artifacts, follow-ups, and customer satisfaction.

source: cjc_aws_workspace_catalog.ssa_ops.fact_asq

joins:
  - name: ssa
    source: cjc_aws_workspace_catalog.ssa_ops.dim_ssa
    on: source.owner_id = ssa.ssa_id
  - name: account
    source: cjc_aws_workspace_catalog.ssa_ops.dim_account
    on: source.account_id = account.account_id
  - name: created_dt
    source: cjc_aws_workspace_catalog.ssa_ops.dim_date
    on: source.created_date_key = created_dt.date_key

filter: source.completion_date IS NOT NULL

dimensions:
  # === ORGANIZATIONAL ===
  - name: Business Unit
    expr: ssa.business_unit
  - name: Manager L1
    expr: ssa.level_1_manager_name
  - name: Owner
    expr: ssa.ssa_name

  # === QUALITY INDICATORS ===
  - name: Has Quality Closure
    expr: CASE WHEN source.has_quality_closure THEN 'Yes' ELSE 'No' END
  - name: Has Artifacts
    expr: CASE WHEN source.has_artifacts THEN 'Yes' ELSE 'No' END
  - name: Has Follow-Up
    expr: CASE WHEN source.has_follow_up THEN 'Yes' ELSE 'No' END
  - name: Customer Satisfaction
    expr: COALESCE(source.customer_satisfaction, 'Not Rated')

  # === TIME ===
  - name: Completed Quarter
    expr: |
      CONCAT(EXTRACT(YEAR FROM source.completion_date), '-Q',
             EXTRACT(QUARTER FROM source.completion_date))
  - name: Fiscal Year
    expr: created_dt.fy_year

  # === CUSTOMER ===
  - name: Account Segment
    expr: account.segment
  - name: Account Vertical
    expr: account.vertical

measures:
  # === VOLUME ===
  - name: Total Completed
    expr: COUNT(1)

  # === QUALITY COUNTS ===
  - name: With Quality Closure
    expr: COUNT(1) FILTER (WHERE source.has_quality_closure)
  - name: With Artifacts
    expr: COUNT(1) FILTER (WHERE source.has_artifacts)
  - name: With Follow-Up
    expr: COUNT(1) FILTER (WHERE source.has_follow_up)
  - name: Fully Documented
    expr: |
      COUNT(1) FILTER (WHERE
        source.has_quality_closure AND source.has_artifacts
      )

  # === QUALITY RATES ===
  - name: Quality Closure Rate
    expr: COUNT(1) FILTER (WHERE source.has_quality_closure) * 1.0 / NULLIF(COUNT(1), 0)
  - name: Artifact Rate
    expr: COUNT(1) FILTER (WHERE source.has_artifacts) * 1.0 / NULLIF(COUNT(1), 0)
  - name: Follow-Up Rate
    expr: COUNT(1) FILTER (WHERE source.has_follow_up) * 1.0 / NULLIF(COUNT(1), 0)
  - name: Full Documentation Rate
    expr: |
      COUNT(1) FILTER (WHERE source.has_quality_closure AND source.has_artifacts) * 1.0
      / NULLIF(COUNT(1), 0)

  # === SATISFACTION ===
  - name: Satisfied Count
    expr: COUNT(1) FILTER (WHERE source.customer_satisfaction IN ('Satisfied', 'Very Satisfied'))
  - name: Satisfaction Rate
    expr: |
      COUNT(1) FILTER (WHERE source.customer_satisfaction IN ('Satisfied', 'Very Satisfied')) * 1.0
      / NULLIF(COUNT(1) FILTER (WHERE source.customer_satisfaction IS NOT NULL), 0)
```

#### `mv_trend_analysis` - YoY/QoQ Trends

```yaml
version: 1.1
comment: |
  Period-over-period trend metrics with window measures.
  Compare current vs prior year/quarter performance.

source: cjc_aws_workspace_catalog.ssa_ops.fact_asq

joins:
  - name: ssa
    source: cjc_aws_workspace_catalog.ssa_ops.dim_ssa
    on: source.owner_id = ssa.ssa_id
  - name: created_dt
    source: cjc_aws_workspace_catalog.ssa_ops.dim_date
    on: source.created_date_key = created_dt.date_key

dimensions:
  # === ORGANIZATIONAL ===
  - name: Business Unit
    expr: ssa.business_unit
  - name: Manager L2
    expr: ssa.level_2_manager_name

  # === TIME (primary grouping) ===
  - name: Fiscal Year
    expr: created_dt.fy_year
  - name: Fiscal Quarter
    expr: created_dt.fy_quarter_name
  - name: Fiscal Year-Quarter
    expr: CONCAT('FY', created_dt.fy_year, '-', created_dt.fy_quarter_name)
  - name: Calendar Month
    expr: DATE_TRUNC('MONTH', source.created_date)
  - name: Calendar Week
    expr: DATE_TRUNC('WEEK', source.created_date)

measures:
  # === CURRENT PERIOD ===
  - name: Total ASQs
    expr: COUNT(1)
  - name: Completed ASQs
    expr: COUNT(1) FILTER (WHERE source.completion_date IS NOT NULL)
  - name: On-Time Rate
    expr: |
      COUNT(1) FILTER (WHERE source.completion_sla_met) * 1.0
      / NULLIF(COUNT(1) FILTER (WHERE source.completion_date IS NOT NULL), 0)
  - name: Avg Days to Complete
    expr: AVG(source.days_to_completion)
  - name: Unique Accounts
    expr: COUNT(DISTINCT source.account_id)

  # === WINDOW MEASURES (period-over-period) ===
  # Note: These require DBR 17.2+ with window measure support

  # Running totals
  - name: YTD ASQs
    expr: SUM(COUNT(1)) OVER (PARTITION BY created_dt.fy_year ORDER BY created_dt.fy_quarter)
    comment: "Cumulative ASQs for fiscal year"

  # Moving averages
  - name: 3-Month Moving Avg
    expr: |
      AVG(COUNT(1)) OVER (
        ORDER BY DATE_TRUNC('MONTH', source.created_date)
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
      )
    comment: "3-month rolling average ASQ count"

  # Period comparison
  - name: Prior Quarter ASQs
    expr: |
      LAG(COUNT(1), 1) OVER (
        PARTITION BY ssa.business_unit
        ORDER BY created_dt.fy_quarter
      )
  - name: Prior Year ASQs
    expr: |
      LAG(COUNT(1), 4) OVER (
        PARTITION BY ssa.business_unit
        ORDER BY created_dt.fy_quarter
      )
    comment: "Same quarter, prior year"

  # Growth rates
  - name: QoQ Growth
    expr: |
      (COUNT(1) - LAG(COUNT(1), 1) OVER (ORDER BY created_dt.fy_quarter)) * 1.0
      / NULLIF(LAG(COUNT(1), 1) OVER (ORDER BY created_dt.fy_quarter), 0)
    comment: "Quarter-over-quarter growth rate"
  - name: YoY Growth
    expr: |
      (COUNT(1) - LAG(COUNT(1), 4) OVER (ORDER BY created_dt.fy_quarter)) * 1.0
      / NULLIF(LAG(COUNT(1), 4) OVER (ORDER BY created_dt.fy_quarter), 0)
    comment: "Year-over-year growth rate"
```

---

## 5. Multi-BU Configuration System

### 5.1 Configuration Variables

To make metric views portable across BUs, use SQL variables:

```sql
-- sql/config/bu_config.sql

-- Business Unit Configuration
SET VAR catalog = 'cjc_aws_workspace_catalog';
SET VAR schema = 'ssa_ops';

-- Filter configurations by BU
-- Uncomment the one you want, or set at runtime

-- Canada (CAN)
SET VAR business_unit_filter = "ssa.business_unit = 'CAN'";
SET VAR manager_filter = "ssa.level_2_manager_id = '0053f000000pKoTAAU'";

-- US West
-- SET VAR business_unit_filter = "ssa.business_unit = 'US-WEST'";
-- SET VAR manager_filter = "ssa.level_2_manager_id = 'XXXXX'";

-- US East
-- SET VAR business_unit_filter = "ssa.business_unit = 'US-EAST'";
-- SET VAR manager_filter = "ssa.level_2_manager_id = 'XXXXX'";

-- EMEA
-- SET VAR business_unit_filter = "ssa.business_unit = 'EMEA'";
-- SET VAR manager_filter = "ssa.level_2_manager_id = 'XXXXX'";

-- ALL BUs (no filter)
-- SET VAR business_unit_filter = "1=1";
-- SET VAR manager_filter = "1=1";
```

### 5.2 Query Patterns by Granularity

```sql
-- === INDIVIDUAL SSA ===
SELECT
  `Owner`,
  MEASURE(`Total ASQs`) AS total,
  MEASURE(`On-Time Rate`) AS on_time_rate
FROM cjc_aws_workspace_catalog.ssa_ops.mv_asq_operations
WHERE `Business Unit` = 'CAN'
  AND `Owner` = 'John Smith'
GROUP BY ALL;

-- === TEAM (L1 Manager) ===
SELECT
  `Manager L1`,
  `Owner`,
  MEASURE(`Total ASQs`) AS total,
  MEASURE(`On-Time Rate`) AS on_time_rate
FROM cjc_aws_workspace_catalog.ssa_ops.mv_asq_operations
WHERE `Manager L1` = 'Christopher Chalcraft'
GROUP BY ALL;

-- === RVP ROLLUP (L2 Manager) ===
SELECT
  `Manager L2`,
  `Manager L1`,
  MEASURE(`Total ASQs`) AS total,
  MEASURE(`On-Time Rate`) AS on_time_rate
FROM cjc_aws_workspace_catalog.ssa_ops.mv_asq_operations
WHERE `Manager L2 ID` = '0053f000000pKoTAAU'
GROUP BY ALL;

-- === BU COMPARISON ===
SELECT
  `Business Unit`,
  MEASURE(`Total ASQs`) AS total,
  MEASURE(`On-Time Rate`) AS on_time_rate,
  MEASURE(`Avg Days to Complete`) AS avg_days
FROM cjc_aws_workspace_catalog.ssa_ops.mv_asq_operations
WHERE `Fiscal Year` = 2026
GROUP BY ALL
ORDER BY total DESC;

-- === VERTICAL ANALYSIS ===
SELECT
  `Account Vertical`,
  `Account Segment`,
  MEASURE(`Total ASQs`) AS total,
  MEASURE(`Repeat Customer Rate`) AS repeat_rate
FROM cjc_aws_workspace_catalog.ssa_ops.mv_customer_engagement
WHERE `Business Unit` = 'CAN'
GROUP BY ALL;

-- === CROSS-BU BENCHMARKING ===
SELECT
  `Business Unit`,
  `Manager L2`,
  MEASURE(`On-Time Rate`) AS on_time_rate,
  MEASURE(`Perfect SLA Rate`) AS perfect_sla,
  MEASURE(`ASQs per SSA`) AS productivity
FROM cjc_aws_workspace_catalog.ssa_ops.mv_team_comparison
WHERE `Fiscal Year` = 2026
GROUP BY ALL
ORDER BY on_time_rate DESC;
```

### 5.3 Filter Hierarchy Examples

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          FILTER HIERARCHY                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Level 0: Global (All Data)                                               │
│   └── Level 1: Region (Americas, EMEA, APJ)                                │
│       └── Level 2: Geo (NA, LATAM, EMEA, APJ)                              │
│           └── Level 3: Business Unit (CAN, US-WEST, US-EAST, etc.)         │
│               └── Level 4: Manager L4 (VP)                                 │
│                   └── Level 5: Manager L3 (AVP)                            │
│                       └── Level 6: Manager L2 (RVP)                        │
│                           └── Level 7: Manager L1 (Direct Mgr)             │
│                               └── Level 8: Owner (Individual SSA)          │
│                                                                             │
│   Cross-cutting Dimensions (combine with any level):                       │
│   • Account Segment (ENT, COMM, MM, SMB)                                   │
│   • Account Vertical (FSI, Healthcare, Retail, Tech, etc.)                 │
│   • Specialization (AI/ML, Data Eng, Platform)                             │
│   • Time (FY, Quarter, Month, Week)                                        │
│   • ARR Band ($0-100K, $100K-500K, $500K-1M, $1M+)                         │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 6. Implementation Plan

### Phase 1: Foundation (Week 1-2)

| Task | Description | Files |
|------|-------------|-------|
| 1.1 | Create dimensional model DDL | `sql/tables/00_schema.sql`, `sql/tables/01_dim_*.sql`, `sql/tables/02_fact_*.sql` |
| 1.2 | Build SSA hierarchy CTE | `sql/transforms/ssa_hierarchy.sql` |
| 1.3 | Create dim_date with fiscal calendar | `sql/tables/01_dim_date.sql` |
| 1.4 | Update logfood export for new schema | `sql/sync/01_export_to_delta.sql` |
| 1.5 | Deploy base tables and verify sync | Validate data flow |
| 1.6 | Add DAB resources for tables | `infra/resources/tables.yml` |

### Phase 2: Core Metrics (Week 3-4)

| Task | Description | Files |
|------|-------------|-------|
| 2.1 | Deploy `mv_asq_operations` | `sql/metric-views/mv_asq_operations.sql` |
| 2.2 | Deploy `mv_sla_compliance` | `sql/metric-views/mv_sla_compliance.sql` |
| 2.3 | Deploy `mv_effort_capacity` | `sql/metric-views/mv_effort_capacity.sql` |
| 2.4 | Create validation queries | `sql/tests/validate_core_metrics.sql` |
| 2.5 | Test multi-BU filtering | Query tests with different BU filters |
| 2.6 | Update Lakebase sync | `src/jobs/sync_to_lakebase.py` |

### Phase 3: Extended Metrics (Week 5-6)

| Task | Description | Files |
|------|-------------|-------|
| 3.1 | Deploy `mv_customer_engagement` | `sql/metric-views/mv_customer_engagement.sql` |
| 3.2 | Deploy `mv_pipeline_impact` | `sql/metric-views/mv_pipeline_impact.sql` |
| 3.3 | Deploy `mv_team_comparison` | `sql/metric-views/mv_team_comparison.sql` |
| 3.4 | Deploy `mv_aging_backlog` | `sql/metric-views/mv_aging_backlog.sql` |
| 3.5 | Deploy `mv_quality_closure` | `sql/metric-views/mv_quality_closure.sql` |
| 3.6 | Deploy `mv_trend_analysis` | `sql/metric-views/mv_trend_analysis.sql` |

### Phase 4: Testing & Validation (Week 7-8)

| Task | Description | Files |
|------|-------------|-------|
| 4.1 | Unit tests for all measures | `tests/metric-views/*.test.ts` |
| 4.2 | Data quality tests | `sql/tests/data_quality.sql` |
| 4.3 | Cross-BU comparison tests | `sql/tests/cross_bu_validation.sql` |
| 4.4 | Metric comparison (old vs new) | `sql/tests/metric_comparison.sql` |
| 4.5 | Performance benchmarks | Load test with/without materialization |
| 4.6 | Hierarchy rollup validation | Verify L1→L5 aggregations |

### Phase 5: Materialization & AI/BI (Week 9-10)

| Task | Description | Files |
|------|-------------|-------|
| 5.1 | Enable materialization on core views | Add materialization YAML blocks |
| 5.2 | Create AI/BI dashboard datasets | Dashboard configuration |
| 5.3 | Configure Genie integration | Test natural language queries |
| 5.4 | Create BU-specific dashboards | One dashboard per BU |
| 5.5 | Document metric definitions | `docs/metrics-reference.md` |

### Phase 6: Migration & Cleanup (Week 11-12)

| Task | Description | Files |
|------|-------------|-------|
| 6.1 | Parallel run validation | Compare old/new in production |
| 6.2 | Update app to new schema | `src/db/collections/*.ts` |
| 6.3 | Deprecate legacy views | Add deprecation notices |
| 6.4 | Train other BU teams | Documentation + sessions |
| 6.5 | Final validation | End-to-end testing |
| 6.6 | Update checkpoint | `tasks/checkpoint.md` |

---

## 7. File Structure

```
ssa-ops/
├── sql/
│   ├── config/                       # NEW: Configuration
│   │   └── bu_config.sql             # BU-specific variables
│   ├── tables/                       # NEW: Dimensional model
│   │   ├── 00_schema.sql
│   │   ├── 01_dim_ssa.sql            # With hierarchy
│   │   ├── 01_dim_account.sql        # With segments
│   │   ├── 01_dim_date.sql           # Fiscal + calendar
│   │   ├── 02_fact_asq.sql           # Core fact
│   │   └── 02_fact_uco.sql           # Pipeline fact
│   ├── transforms/                   # NEW: Data transforms
│   │   ├── ssa_hierarchy.sql         # Recursive CTE for hierarchy
│   │   └── fiscal_calendar.sql       # Generate dim_date
│   ├── metric-views/                 # NEW: Metric view definitions
│   │   ├── mv_asq_operations.sql     # Core ASQ metrics
│   │   ├── mv_sla_compliance.sql     # SLA tracking
│   │   ├── mv_effort_capacity.sql    # Effort/capacity
│   │   ├── mv_customer_engagement.sql # Account metrics
│   │   ├── mv_pipeline_impact.sql    # UCO linkage
│   │   ├── mv_team_comparison.sql    # Benchmarking
│   │   ├── mv_aging_backlog.sql      # Backlog health
│   │   ├── mv_quality_closure.sql    # Quality metrics
│   │   └── mv_trend_analysis.sql     # YoY/QoQ trends
│   ├── sync/                         # UPDATED: New sync logic
│   │   ├── 01_export_to_delta.sql
│   │   └── 02_sync_to_lakebase.sql
│   ├── tests/                        # UPDATED: Validation
│   │   ├── validate_core_metrics.sql
│   │   ├── validate_extended_metrics.sql
│   │   ├── data_quality.sql
│   │   ├── cross_bu_validation.sql
│   │   └── metric_comparison.sql
│   └── views/                        # DEPRECATED: Legacy views
│       └── (keep for reference)
├── infra/resources/
│   ├── tables.yml                    # NEW: Table definitions
│   └── metric_views.yml              # NEW: Metric view resources
├── tests/
│   └── metric-views/                 # NEW: TypeScript tests
│       ├── asq_operations.test.ts
│       ├── sla_compliance.test.ts
│       ├── cross_bu.test.ts
│       └── measure_calculations.test.ts
├── docs/
│   ├── PLAN-metric-views.md          # THIS FILE
│   ├── architecture.md               # UPDATED
│   ├── metrics-reference.md          # NEW: Full metric catalog
│   ├── bu-onboarding.md              # NEW: Guide for other BUs
│   └── data-dictionary.md            # UPDATED
└── justfile                          # UPDATED: New commands
```

---

## 8. Justfile Commands

```just
# === METRIC VIEWS ===

# Deploy all metric views
deploy-metric-views:
    @echo "Deploying metric views..."
    for f in sql/metric-views/*.sql
        echo "  Deploying $f..."
        databricks sql execute -f $f
    end
    @echo "✓ Metric views deployed"

# Deploy a specific metric view
deploy-mv name:
    @echo "Deploying {{name}}..."
    databricks sql execute -f sql/metric-views/{{name}}.sql
    @echo "✓ {{name}} deployed"

# Describe a metric view
describe-mv name:
    databricks sql execute -q "DESCRIBE EXTENDED cjc_aws_workspace_catalog.ssa_ops.{{name}}"

# Query metric view (CAN BU)
query-mv-can name:
    databricks sql execute -q "SELECT * FROM cjc_aws_workspace_catalog.ssa_ops.{{name}} WHERE \`Business Unit\` = 'CAN' LIMIT 100"

# Query metric view (all BUs)
query-mv-all name:
    databricks sql execute -q "SELECT \`Business Unit\`, COUNT(1) as cnt FROM cjc_aws_workspace_catalog.ssa_ops.{{name}} GROUP BY ALL"

# Validate core metrics
validate-core:
    @echo "Validating core metrics..."
    databricks sql execute -f sql/tests/validate_core_metrics.sql

# Validate extended metrics
validate-extended:
    @echo "Validating extended metrics..."
    databricks sql execute -f sql/tests/validate_extended_metrics.sql

# Run cross-BU validation
validate-cross-bu:
    @echo "Validating cross-BU consistency..."
    databricks sql execute -f sql/tests/cross_bu_validation.sql

# Compare old vs new metrics
compare-metrics:
    @echo "Comparing legacy views to metric views..."
    databricks sql execute -f sql/tests/metric_comparison.sql

# Run all metric view tests
test-metric-views:
    pnpm test tests/metric-views/

# === TABLES ===

# Create dimensional model tables
create-tables:
    @echo "Creating dimensional model..."
    databricks sql execute -f sql/tables/00_schema.sql
    databricks sql execute -f sql/tables/01_dim_date.sql
    databricks sql execute -f sql/tables/01_dim_ssa.sql
    databricks sql execute -f sql/tables/01_dim_account.sql
    databricks sql execute -f sql/tables/02_fact_asq.sql
    databricks sql execute -f sql/tables/02_fact_uco.sql
    @echo "✓ Tables created"

# Refresh SSA hierarchy
refresh-hierarchy:
    @echo "Refreshing SSA hierarchy..."
    databricks sql execute -f sql/transforms/ssa_hierarchy.sql

# Refresh fiscal calendar
refresh-calendar:
    @echo "Refreshing fiscal calendar..."
    databricks sql execute -f sql/transforms/fiscal_calendar.sql

# === BU CONFIGURATION ===

# Set BU filter to Canada
set-bu-can:
    @echo "Setting BU filter to CAN..."
    @echo "Update sql/config/bu_config.sql to use CAN filters"

# Set BU filter to All
set-bu-all:
    @echo "Setting BU filter to ALL..."
    @echo "Update sql/config/bu_config.sql to use no filters"
```

---

## 9. Testing Strategy

### 9.1 Unit Tests (Measure Calculations)

```typescript
// tests/metric-views/asq_operations.test.ts

describe("ASQ Operations Metrics", () => {
  describe("Volume Measures", () => {
    it("Open ASQs + Closed ASQs = Total ASQs", async () => {
      const result = await query(`
        SELECT
          MEASURE(\`Open ASQs\`) AS open,
          MEASURE(\`Closed ASQs\`) AS closed,
          MEASURE(\`Total ASQs\`) AS total
        FROM cjc_aws_workspace_catalog.ssa_ops.mv_asq_operations
      `);

      expect(result.open + result.closed).toBe(result.total);
    });
  });

  describe("Rate Measures", () => {
    it("On-Time Rate = On-Time Completions / Completed ASQs", async () => {
      const result = await query(`
        SELECT
          MEASURE(\`On-Time Completions\`) AS on_time,
          MEASURE(\`Completed ASQs\`) AS completed,
          MEASURE(\`On-Time Rate\`) AS rate
        FROM cjc_aws_workspace_catalog.ssa_ops.mv_asq_operations
      `);

      const expected = completed > 0 ? result.on_time / result.completed : null;
      expect(result.rate).toBeCloseTo(expected, 4);
    });
  });

  describe("Multi-BU Filtering", () => {
    it("BU filter reduces total count", async () => {
      const all = await query(`
        SELECT MEASURE(\`Total ASQs\`) AS total
        FROM cjc_aws_workspace_catalog.ssa_ops.mv_asq_operations
      `);

      const can = await query(`
        SELECT MEASURE(\`Total ASQs\`) AS total
        FROM cjc_aws_workspace_catalog.ssa_ops.mv_asq_operations
        WHERE \`Business Unit\` = 'CAN'
      `);

      expect(can.total).toBeLessThanOrEqual(all.total);
      expect(can.total).toBeGreaterThan(0);
    });

    it("Manager hierarchy filters correctly", async () => {
      const l2 = await query(`
        SELECT MEASURE(\`Total ASQs\`) AS total
        FROM cjc_aws_workspace_catalog.ssa_ops.mv_asq_operations
        WHERE \`Manager L2\` = 'Christopher Chalcraft'
      `);

      const l1 = await query(`
        SELECT \`Manager L1\`, MEASURE(\`Total ASQs\`) AS total
        FROM cjc_aws_workspace_catalog.ssa_ops.mv_asq_operations
        WHERE \`Manager L2\` = 'Christopher Chalcraft'
        GROUP BY ALL
      `);

      const l1Sum = l1.reduce((sum, row) => sum + row.total, 0);
      expect(l1Sum).toBe(l2.total);
    });
  });
});
```

### 9.2 Data Quality Tests

```sql
-- sql/tests/data_quality.sql

-- === PRIMARY KEY TESTS ===

SELECT 'fact_asq_pk_not_null' AS test,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  COUNT(*) AS violations
FROM cjc_aws_workspace_catalog.ssa_ops.fact_asq
WHERE asq_id IS NULL;

SELECT 'fact_asq_pk_unique' AS test,
  CASE WHEN COUNT(*) = COUNT(DISTINCT asq_id) THEN 'PASS' ELSE 'FAIL' END AS status,
  COUNT(*) - COUNT(DISTINCT asq_id) AS violations
FROM cjc_aws_workspace_catalog.ssa_ops.fact_asq;

-- === REFERENTIAL INTEGRITY ===

SELECT 'fk_owner_exists' AS test,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  COUNT(*) AS violations
FROM cjc_aws_workspace_catalog.ssa_ops.fact_asq f
LEFT JOIN cjc_aws_workspace_catalog.ssa_ops.dim_ssa s ON f.owner_id = s.ssa_id
WHERE f.owner_id IS NOT NULL AND s.ssa_id IS NULL;

SELECT 'fk_account_exists' AS test,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  COUNT(*) AS violations
FROM cjc_aws_workspace_catalog.ssa_ops.fact_asq f
LEFT JOIN cjc_aws_workspace_catalog.ssa_ops.dim_account a ON f.account_id = a.account_id
WHERE f.account_id IS NOT NULL AND a.account_id IS NULL;

-- === DATE SANITY ===

SELECT 'completion_after_creation' AS test,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  COUNT(*) AS violations
FROM cjc_aws_workspace_catalog.ssa_ops.fact_asq
WHERE completion_date < created_date;

SELECT 'assignment_after_creation' AS test,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  COUNT(*) AS violations
FROM cjc_aws_workspace_catalog.ssa_ops.fact_asq
WHERE assignment_date < created_date;

-- === HIERARCHY COMPLETENESS ===

SELECT 'ssa_has_manager' AS test,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARN' END AS status,
  COUNT(*) AS violations
FROM cjc_aws_workspace_catalog.ssa_ops.dim_ssa
WHERE is_active AND level_1_manager_id IS NULL;

SELECT 'ssa_has_business_unit' AS test,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  COUNT(*) AS violations
FROM cjc_aws_workspace_catalog.ssa_ops.dim_ssa
WHERE is_active AND business_unit IS NULL;

-- === COMPLETENESS BY BU ===

SELECT
  business_unit,
  COUNT(*) AS total_ssas,
  SUM(CASE WHEN level_2_manager_id IS NOT NULL THEN 1 ELSE 0 END) AS with_l2_manager,
  SUM(CASE WHEN level_3_manager_id IS NOT NULL THEN 1 ELSE 0 END) AS with_l3_manager
FROM cjc_aws_workspace_catalog.ssa_ops.dim_ssa
WHERE is_active
GROUP BY business_unit;
```

### 9.3 Cross-BU Validation

```sql
-- sql/tests/cross_bu_validation.sql

-- Verify each BU has reasonable data

SELECT
  `Business Unit`,
  MEASURE(`Total ASQs`) AS total,
  MEASURE(`Unique SSAs`) AS ssas,
  MEASURE(`Unique Accounts`) AS accounts,
  MEASURE(`On-Time Rate`) AS on_time_rate
FROM cjc_aws_workspace_catalog.ssa_ops.mv_asq_operations
WHERE `Fiscal Year` = 2026
GROUP BY `Business Unit`
ORDER BY total DESC;

-- Verify hierarchy rollup is consistent

WITH l1_totals AS (
  SELECT
    `Manager L2`,
    SUM(MEASURE(`Total ASQs`)) AS l1_sum
  FROM cjc_aws_workspace_catalog.ssa_ops.mv_asq_operations
  GROUP BY `Manager L2`, `Manager L1`
),
l2_totals AS (
  SELECT
    `Manager L2`,
    MEASURE(`Total ASQs`) AS l2_total
  FROM cjc_aws_workspace_catalog.ssa_ops.mv_asq_operations
  GROUP BY `Manager L2`
)
SELECT
  l2.`Manager L2`,
  l1_totals.l1_sum,
  l2.l2_total,
  CASE WHEN l1_totals.l1_sum = l2.l2_total THEN 'PASS' ELSE 'FAIL' END AS status
FROM l2_totals l2
JOIN (
  SELECT `Manager L2`, SUM(l1_sum) AS l1_sum
  FROM l1_totals
  GROUP BY `Manager L2`
) l1_totals ON l2.`Manager L2` = l1_totals.`Manager L2`;
```

---

## 10. Portability Guide

### 10.1 Deploying to Another BU

1. **Clone the repository**
   ```bash
   git clone https://github.com/databricks/ssa-ops.git
   cd ssa-ops
   ```

2. **Update configuration**
   ```sql
   -- sql/config/bu_config.sql
   SET VAR business_unit_filter = "ssa.business_unit = 'US-WEST'";
   SET VAR manager_filter = "ssa.level_2_manager_id = 'YOUR_MANAGER_ID'";
   ```

3. **Update databricks.yml target**
   ```yaml
   targets:
     us-west:
       workspace:
         profile: us-west-workspace
       variables:
         catalog: "us_west_catalog"
         schema: "ssa_ops"
   ```

4. **Deploy**
   ```bash
   databricks bundle deploy -t us-west
   just create-tables
   just deploy-metric-views
   ```

### 10.2 Required Permissions

| Resource | Permission | Why |
|----------|------------|-----|
| Catalog | `USE CATALOG` | Access the catalog |
| Schema | `USE SCHEMA`, `CREATE TABLE` | Create tables and views |
| Source tables | `SELECT` | Read source data |
| Warehouse | `CAN USE` | Execute queries |

### 10.3 Data Requirements

For metric views to work, you need:

1. **User table** with manager hierarchy (or build via recursive CTE)
2. **ASQ data** from Salesforce
3. **Account data** with segments/verticals
4. **UCO data** (optional, for pipeline metrics)

---

## 11. Success Criteria

| Criteria | Measurement | Target |
|----------|-------------|--------|
| **Correctness** | All validation tests pass | 100% |
| **Multi-BU** | Works for CAN, US-WEST, EMEA | 3+ BUs |
| **Hierarchy** | L1-L5 rollups consistent | 100% match |
| **Performance** | Dashboard queries | < 2s |
| **Coverage** | Metric views deployed | 9 views |
| **Testing** | Measure unit test coverage | 80%+ |
| **Documentation** | Complete metrics catalog | All measures documented |

---

## 12. Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| DBR 17.2 not available | Check workspace version; fall back to v0.1 syntax |
| Incomplete manager hierarchy | Build hierarchy via recursive CTE from User table |
| Missing BU data in User table | Work with People Analytics to add BU field |
| Cross-catalog access issues | Sync all data to single catalog first |
| Window measures not working | Use standard aggregations; add window later |
| Performance on large datasets | Enable materialization; add date filters |

---

## 13. Next Steps

1. **Review this plan** - Confirm approach, priorities, and BU requirements
2. **Validate hierarchy data** - Check User table has manager chain
3. **Start Phase 1** - Create dimensional model tables
4. **Iterate** - Build and test incrementally
5. **Pilot with CAN** - Full deployment and validation
6. **Expand to other BUs** - Train teams, deploy copies

---

*Plan created: 2026-03-19*
*Updated: 2026-03-19 (Multi-BU expansion)*
*Author: Isaac (AI Assistant)*
*Branch: ssa-metric-views*
