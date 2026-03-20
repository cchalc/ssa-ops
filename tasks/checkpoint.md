# Session Checkpoint - 2026-03-19 (Updated)

## Context
SSA Activity Dashboard connecting SSA work (ASQs) to GTM business metrics.
- **Architecture:** Use existing GTM Silver/Gold tables on logfood, NOT custom dimensional model
- **Key Insight:** SSA work impacts consumption—join ASQ data to account_obt for correlation

## Critical Change: Use GTM Tables Directly

**Previous approach (REJECTED):** Custom star schema with fact_asq, dim_ssa, dim_date, etc.

**New approach (CORRECT):** Query GTM Silver/Gold tables directly on logfood:

| Source | Table | Purpose |
|--------|-------|---------|
| GTM Silver | `main.gtm_silver.approval_request_detail` | ASQ data (current snapshot) |
| GTM Silver | `main.gtm_silver.approval_request_detail_history` | ASQ historical snapshots |
| GTM Silver | `main.gtm_silver.individual_hierarchy_field` | FE manager hierarchy (L1-L7) |
| GTM Silver | `main.gtm_silver.use_case_detail` | UCO/use case data |
| GTM Gold | `main.gtm_gold.account_obt` | Account consumption at FQ granularity |
| GTM Gold | `main.gtm_gold.individual_obt` | Individual metrics at FQ granularity |
| GTM Gold | `main.gtm_gold.account_period_over_period` | Daily snapshots for trends |

---

## GTM Data Model Findings

### Business Unit Mapping (from approval_request_detail)

| business_unit | region_level_1 | Description |
|---------------|----------------|-------------|
| AMER Enterprise & Emerging | CAN | Canada |
| AMER Enterprise & Emerging | RCT, EE & Startup, DNB, CMEG, LATAM | US regions |
| AMER Industries | MFG, FINS, HLS, PS | Industry verticals |
| EMEA | SEMEA, UKI, Central, BeNo, Emerging | Europe |
| APJ | ANZ, India, Asean + GCR, Korea, Japan | Asia-Pacific |

### Key ASQ Columns (approval_request_detail)

```
approval_request_id, approval_request_name  -- identifiers
owner_user_id, owner_user_name              -- SSA owner
account_id, account_name                    -- customer
status                                      -- Complete, Approved, Rejected, In Progress, New, Assigned
support_type                                -- Platform Administration, Production Architecture Review & Design, etc.
technical_specialization                    -- Data Science, Data Engineering, Platform, etc.
business_unit, region_level_1               -- BU/region
estimated_effort_in_days, actual_effort_in_days  -- effort tracking
created_date, target_end_date, actual_completion_date  -- SLA dates
snapshot_date                               -- for historical queries, use MAX()
```

### Joining ASQ to Consumption Metrics

```sql
-- Correlate SSA work with account consumption
SELECT
  a.approval_request_name,
  a.account_name,
  a.status,
  a.business_unit,
  ao.dbu_dollars_qtd,
  ao.spend_tier
FROM main.gtm_silver.approval_request_detail a
LEFT JOIN main.gtm_gold.account_obt ao
  ON a.account_id = ao.account_id
  AND ao.fiscal_year_quarter = "FY'26 Q4"
WHERE a.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
```

### GTM Naming Conventions

- Tables: `[account/individual]_[metric]_[timegrain]` (e.g., `account_consumption_daily`)
- OBTs: `[account/individual]_obt`
- Metric Views: `mv_[domain]_[focus]` (e.g., `mv_asq_operations`)
- Targets: `target_[metric]`
- Forecasts: `forecast_[metric]`

### Fiscal Calendar

- **FY ends January 31** (Databricks fiscal year)
- Current: FY'26 Q4 (Feb 1 - Apr 30, 2026)
- OBTs aggregated at fiscal quarter level
- Use Period-over-Period tables for daily granularity

### Refresh Cadences

- Consumption actuals: Once daily ~11am PST
- OBTs & Materialized Views: Every 1 hour
- GTM Silver/Gold Workflows: Every 2 hours
- Pipe Gen, Clari, Use Cases: Every 2 hours

---

## Revised Implementation Phases

### ✅ Phase 1: GTM Data Model Research (COMPLETE)

- Read GTM Gold Documentation, Functions Guide, FAQ
- Updated CLAUDE.md with GTM reference sections
- Queried logfood to explore actual table structures
- Discovered business unit mapping
- Verified ASQ → account_obt join works
- Identified FE hierarchy table

### ✅ Phase 2: Core Metric Views (COMPLETE)

Create metric views using GTM Silver/Gold sources directly.

#### Files to Create
```
sql/metric-views/mv_asq_operations.sql   # Core ASQ metrics
sql/metric-views/mv_sla_compliance.sql   # SLA milestone tracking
sql/metric-views/mv_effort_capacity.sql  # Effort estimation & capacity
```

#### Source Tables (on logfood)
- `main.gtm_silver.approval_request_detail` - ASQ facts
- `main.gtm_silver.individual_hierarchy_field` - SSA hierarchy
- `main.gtm_gold.account_obt` - Account consumption

#### Parameterization
- `${catalog}` / `${schema}` for deployment target
- Filter dimensions (business_unit, manager hierarchy) at query time
- No hardcoded BU filters - views are portable

#### mv_asq_operations Dimensions
- Business Unit, Region, Geo
- Owner (SSA), Manager L1-L5 (from hierarchy table)
- Account, Segment, Vertical
- Specialization, Support Type, Priority
- Fiscal Year/Quarter, Calendar Year/Quarter/Month
- Is Open, Is Overdue, Is At Risk

#### mv_asq_operations Measures
- Total ASQs, Open ASQs, Closed ASQs, Overdue ASQs, At Risk ASQs
- On-Time Rate, Overdue Rate, Completion Rate
- Avg/Median/P90 Days to Complete
- Unique Accounts, Unique SSAs, ASQs per SSA
- Linked ASQs (UCO), Linkage Rate

### ✅ Phase 3: Extended Metrics (COMPLETE)

Additional views for comprehensive analysis:
```
sql/metric-views/mv_consumption_impact.sql   # ASQ work correlated with consumption
sql/metric-views/mv_pipeline_impact.sql      # UCO linkage & pipe gen
sql/metric-views/mv_team_comparison.sql      # Cross-BU benchmarking
sql/metric-views/mv_trend_analysis.sql       # YoY/QoQ with fiscal periods
```

### ✅ Phase 4: Testing & Validation (COMPLETE)

```
sql/tests/validate_metric_views.sql      # Null checks, join validation
sql/tests/data_quality.sql               # Duplicates, orphan records
sql/tests/cross_bu_validation.sql        # Multi-BU consistency
tests/metric-views/*.test.ts             # TypeScript unit tests
```

#### Test Categories
1. **Data Validity** - No nulls in key columns, proper joins
2. **No Duplicates** - Unique grain per row
3. **Join Integrity** - No orphan records, referential integrity
4. **Cross-BU** - Consistent hierarchy rollups
5. **Performance** - Query latency benchmarks

### ✅ Phase 5: Materialization & Performance (COMPLETE)

1. Created `sql/metric-views/materialization_config.sql`
2. Added `sql/tests/benchmark_performance.sql`
3. Added justfile commands: `enable-materialization`, `refresh-mv`
4. Configured refresh schedules (every 6 hours for core, 12 hours for extended)

### ✅ Phase 6: DAB Workflows & Lakebase Sync (COMPLETE)

1. Created `infra/resources/metric_views.yml` with 3 workflows:
   - `ssa_metric_view_validation` - Daily validation tests
   - `ssa_metric_view_performance` - Weekly performance benchmarks
   - `ssa_metric_view_deploy` - Manual deployment job
2. Updated `databricks.yml` to include metric_views.yml
3. Added justfile commands: `dab-deploy-metric-views`, `dab-run-deploy`, `dab-run-validation`
4. Existing Lakebase sync jobs in `sync_jobs.yml` remain for app data sync

---

## Current Branch & PR

**Branch:** `ssa-metric-views`
**PR:** https://github.com/cchalc/ssa-ops/pull/2

---

## File Structure (Revised)

```
ssa-ops/
├── docs/
│   ├── gtmhub_docs.md              # Links to GTM internal docs
│   └── PLAN-metric-views.md        # Full plan document
├── sql/
│   ├── metric-views/               # 🔲 Phase 2-3
│   │   ├── mv_asq_operations.sql   # Created (needs review)
│   │   ├── mv_sla_compliance.sql
│   │   ├── mv_effort_capacity.sql
│   │   ├── mv_consumption_impact.sql
│   │   ├── mv_pipeline_impact.sql
│   │   ├── mv_team_comparison.sql
│   │   └── mv_trend_analysis.sql
│   ├── tests/                      # 🔲 Phase 4
│   │   ├── validate_core_metrics.sql
│   │   ├── data_quality.sql
│   │   └── cross_bu_validation.sql
│   └── sync/                       # 🔲 Phase 6
│       └── sync_to_lakebase.sql
├── tests/
│   └── metric-views/               # 🔲 Phase 4
│       └── *.test.ts
├── databricks.yml                  # 🔲 Phase 6 - DAB config
├── justfile                        # Updated
└── tasks/
    ├── checkpoint.md               # This file
    └── lessons.md
```

---

## Quick Resume Commands

```bash
# Check current state
jj status
jj log --limit 5

# Continue Phase 2 - Review/update mv_asq_operations.sql
# Ensure it uses GTM Silver/Gold sources directly
# Then create mv_sla_compliance.sql and mv_effort_capacity.sql

# Deploy metric views
just deploy-metric-views
```

---

## Key References

- **GTM Docs:** `docs/gtmhub_docs.md` (links to Google Docs)
- **CLAUDE.md:** Contains GTM data model reference
- **Plan Document:** `docs/PLAN-metric-views.md`
- **PR:** https://github.com/cchalc/ssa-ops/pull/2
