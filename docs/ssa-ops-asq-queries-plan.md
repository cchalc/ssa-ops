# SSA-Ops ASQ Queries Implementation Plan

**Created:** 2026-03-21
**Source Workspace:** `/Users/christopher.chalcraft/cowork/tickets`
**Target Workspace:** `/Users/christopher.chalcraft/cowork/dev/ssa-ops`

---

## Overview

This plan provides ready-to-deploy ASQ evaluation queries for the ssa-ops project. The queries use **corrected column names** based on actual schema inspection and are designed to be parameterized for any region or SSA manager.

### Key Discoveries

During development, we discovered:

1. **Column Names Differ from Documentation:**
   - `Title` instead of `request_name`
   - `created_date` instead of `assignment_date`
   - `usecase_id`/`usecase_name` instead of `use_case_id`/`use_case_name`

2. **Some Tables Have No Data:**
   - `main.gtm_gold.account_segmentation` - **EMPTY** (no account tier data available)
   - This means L400+/BU+1 classification is not currently possible

3. **CAN Region Has 0 Open ASQs** (as of 2026-03-21)
   - RCT, MFG, FINS have the most open ASQs

4. **Manager Hierarchy Works:**
   - `main.gtm_silver.individual_hierarchy_field` with `manager_level_1_id` filter

---

## Queries to Deploy

### 1. `cjc-asq-evaluation`
**Purpose:** Complete ASQ evaluation with hygiene rules and urgency classification
**Parameters:** `{{ region }}` (e.g., CAN, RCT, FINS, MFG)
**File:** `sql/logfood/cjc-asq-evaluation.sql`

**Features:**
- Hygiene status (5-rule framework: RULE1-RULE5 or COMPLIANT)
- Urgency classification (CRITICAL, HIGH, MEDIUM, NORMAL)
- Days open/overdue tracking
- Notes status detection
- Clickable Salesforce links

### 2. `cjc-asq-by-manager`
**Purpose:** ASQs filtered by SSA manager hierarchy
**Parameters:** `{{ manager_id }}` (e.g., 0053f000000pKoTAAU)
**File:** `sql/logfood/cjc-asq-by-manager.sql`

**Features:**
- Uses `individual_hierarchy_field` for team filtering
- Same hygiene/urgency metrics as evaluation
- Groups by assigned SSA

### 3. `cjc-asq-with-ucos`
**Purpose:** ASQs with linked Use Case Opportunities
**Parameters:** `{{ region }}`
**File:** `sql/logfood/cjc-asq-with-ucos.sql`

**Features:**
- Links ASQs to UCOs on same account
- Shows pipeline value (estimated monthly DBUs)
- UCO stage classification (EARLY, SCOPING, NEAR_WIN, LIVE)
- Competitive status and primary competitor

### 4. `cjc-asq-hygiene-summary`
**Purpose:** Summary of hygiene violations by SSA
**Parameters:** `{{ region }}`
**File:** `sql/logfood/cjc-asq-hygiene-summary.sql`

**Features:**
- Aggregated hygiene metrics per SSA
- Compliance percentage
- Counts by rule violation type

### 5. `cjc-asq-team-capacity`
**Purpose:** Team workload distribution for managers
**Parameters:** `{{ manager_id }}`
**File:** `sql/logfood/cjc-asq-team-capacity.sql`

**Features:**
- Total ASQs and effort days per SSA
- Workload classification (HEAVY, MODERATE, LIGHT)
- Overdue and critical counts

### 6. `cjc-uco-competitive`
**Purpose:** Competitive analysis for UCOs linked to ASQ accounts
**Parameters:** `{{ region }}`
**File:** `sql/logfood/cjc-uco-competitive.sql`

**Features:**
- Competitor breakdown by stage
- Total DBU at risk
- Example accounts per competitor

### 7. `cjc-asq-region-summary`
**Purpose:** Overview of ASQs across all regions
**Parameters:** None (shows all regions)
**File:** `sql/logfood/cjc-asq-region-summary.sql`

**Features:**
- Total ASQs per region
- Overdue percentage
- Average/max days open
- No-notes count

---

## Schema Reference

### main.gtm_silver.approval_request_detail

**Key Columns (Verified):**
```
approval_request_id       -- Primary key, use in SF links
approval_request_name     -- AR-XXXXXXXXX format
Title                     -- Request title (NOT request_name!)
status                    -- New, Submitted, Under Review, In Progress, etc.
account_id, account_name  -- Customer account
owner_user_id, owner_user_name  -- Assigned SSA
created_date              -- Request creation date (NOT assignment_date!)
target_end_date           -- Due date
request_description       -- Full description
request_status_notes      -- Status updates
estimated_effort_in_days  -- Effort estimate
region_level_1            -- CAN, RCT, FINS, MFG, etc.
business_unit             -- AMER Enterprise, EMEA, APJ, etc.
technical_specialization  -- Technical area
support_type              -- Type of support
snapshot_date             -- Always filter by MAX(snapshot_date)
```

### main.gtm_silver.use_case_detail

**Key Columns (Verified):**
```
usecase_id                -- Primary key (NOT use_case_id!)
usecase_name              -- UCO name (NOT use_case_name!)
account_id                -- Links to ASQ account
stage                     -- U1 - Identified, U2 - Qualifying, U3 - Scoping, etc.
estimated_monthly_dollar_dbus  -- Pipeline value
competitor_status         -- Competitive status
primary_competitor        -- Competitor name
is_active_ind             -- Filter for active UCOs
snapshot_date             -- Always filter by MAX(snapshot_date)
```

### main.gtm_silver.individual_hierarchy_field

**Key Columns:**
```
user_id                   -- SFDC User ID
user_name                 -- User name
manager_level_1_id        -- Direct manager's user ID
```

### Tables with NO DATA (Avoid)
- `main.gtm_gold.account_segmentation` - Empty, no account tier data
- Account tier (L400+/BU+1) classification not currently available

---

## 5-Rule Hygiene Framework

| Rule | Trigger | Severity |
|------|---------|----------|
| **RULE1_MISSING_NOTES** | Assigned >7 days, no status notes | High |
| **RULE3_STALE** | Open 30-90 days + past/no due date | High |
| **RULE4_EXPIRED** | Due date expired >7 days ago | Critical |
| **RULE5_EXCESSIVE** | Open >90 days | Critical |
| **COMPLIANT** | No violations | Good |

---

## Urgency Classification

| Level | Condition |
|-------|-----------|
| **CRITICAL** | >14 days overdue |
| **HIGH** | 7-14 days overdue |
| **MEDIUM** | 1-7 days overdue OR stale notes |
| **NORMAL** | On track |

---

## Known Manager IDs

| Manager | SFDC User ID |
|---------|--------------|
| Christopher Chalcraft (CJC) | `0053f000000pKoTAAU` |

---

## Implementation Steps for ssa-ops

### Step 1: Copy SQL Files
```fish
cp -r /Users/christopher.chalcraft/cowork/tickets/sql/logfood/*.sql \
      /Users/christopher.chalcraft/cowork/dev/ssa-ops/sql/logfood/
```

### Step 2: Test Queries Locally
```fish
cd /Users/christopher.chalcraft/cowork/dev/ssa-ops
uv run python3 -c "
from databricks.sdk import WorkspaceClient
from pathlib import Path

w = WorkspaceClient(profile='logfood')
query = Path('sql/logfood/cjc-asq-region-summary.sql').read_text()
r = w.statement_execution.execute_statement(
    warehouse_id='927ac096f9833442',
    statement=query,
    wait_timeout='30s'
)
for row in r.result.data_array[:10]:
    print(row)
"
```

### Step 3: Save to Logfood (Optional)
Use the Databricks SQL UI to save queries, or use the SDK:
```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import CreateQueryRequestQuery

w = WorkspaceClient(profile='logfood')
result = w.queries.create(
    query=CreateQueryRequestQuery(
        display_name='cjc-asq-evaluation',
        warehouse_id='927ac096f9833442',
        query_text=open('sql/logfood/cjc-asq-evaluation.sql').read(),
        description='Complete ASQ evaluation with hygiene rules',
    )
)
print(f'Created: {result.id}')
```

### Step 4: Create Metric Views (Future)
If needed, wrap these queries as Unity Catalog Metric Views for multi-BU access.

---

## Python Runner Script

A Python script is available at:
```
/Users/christopher.chalcraft/cowork/tickets/scripts/run_asq_final.py
```

Usage:
```fish
cd /Users/christopher.chalcraft/cowork/tickets
uv run scripts/run_asq_final.py --region RCT --limit 10
uv run scripts/run_asq_final.py --region FINS --format csv > fins_asqs.csv
```

---

## Files to Copy to ssa-ops

```
tickets/
├── sql/logfood/
│   ├── cjc-asq-evaluation.sql
│   ├── cjc-asq-by-manager.sql
│   ├── cjc-asq-with-ucos.sql
│   ├── cjc-asq-hygiene-summary.sql
│   ├── cjc-asq-team-capacity.sql
│   ├── cjc-uco-competitive.sql
│   └── cjc-asq-region-summary.sql
├── scripts/
│   └── run_asq_final.py
├── pyproject.toml
└── .envrc
```

---

## Verification Checklist

After implementing in ssa-ops:

- [ ] All 7 queries execute without errors
- [ ] `cjc-asq-region-summary` returns data for RCT, FINS, MFG
- [ ] `cjc-asq-evaluation` with `{{ region }}` = 'RCT' returns rows
- [ ] `cjc-asq-by-manager` with `{{ manager_id }}` = '0053f000000pKoTAAU' works
- [ ] Hygiene status correctly identifies RULE1-RULE5 violations
- [ ] Urgency classification correctly flags CRITICAL/HIGH items
- [ ] Salesforce links are clickable and valid
- [ ] UCO linkage shows pipeline value

---

## Future Enhancements

1. **Account Tier Classification** - Requires `account_segmentation` table to be populated
2. **Chatter Integration** - FeedItem table for ASQ comments
3. **Calendar Activity** - Integrate with `account_meetings_daily` for engagement tracking
4. **Asset Reuse Tracking** - Requires new Salesforce field or tracking system

---

*Generated from tickets workspace for ssa-ops implementation session*
