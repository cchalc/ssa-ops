# SSA Metrics Implementation Tasks

See [implementation guide](/Users/christopher.chalcraft/cowork/tickets/tasks/ssa-ops-implementation-guide.md) for full context.

## Phase 1: Foundation & Data Model Enhancement

### 1.1 Fact Table Enhancements
- [x] Enhance `fact_uco.sql` with competitive and velocity fields
  - [x] Add `competitor_status` from UseCase__c
  - [x] Add `current_stage_days_count` for velocity tracking
  - [x] Add `last_stage_modified_date` for milestone tracking
  - [x] Add `competitor_category` (normalized: Microsoft, Snowflake, etc.)
  - [x] Add `is_competitive` flag
- [x] Add account segmentation to `dim_account.sql`
  - [x] Add `account_tier` (L100-L500) from account_segmentation
  - [x] Add `is_bu_plus_one` flag
  - [x] Add `is_priority_account` computed flag

### 1.2 Create New Metric Views
- [x] `sql/metric-views/mv_focus_discipline.sql` - 80% L400+ effort tracking
- [x] `sql/metric-views/mv_uco_velocity.sql` - Time-to-production metrics
- [x] `sql/metric-views/mv_competitive_analysis.sql` - Win/loss analysis

### 1.3 Update Infrastructure
- [x] Add new metric views to `infra/resources/metric_views.yml`
- [x] Add justfile commands for charter metrics

## Phase 2: Logfood Queries (cjc- prefix)

_These queries are embedded in the metric views. Logfood queries can be saved separately if needed for ad-hoc analysis._

- [x] ARR from production UCOs (U5/U6) → `mv_pipeline_impact`
- [x] Win/loss rate by SSA → `mv_competitive_analysis`
- [x] Competitor tracking → `mv_competitive_analysis`
- [x] Stage transitions/velocity → `mv_uco_velocity`
- [x] 80% L400+ target → `mv_focus_discipline`
- [x] Tier breakdown → `mv_focus_discipline`

## Phase 3: Sync Pipeline

_Sync files already exist. May need updates for new fields._

- [x] `sql/sync/00_export_raw_tables.sql` exists
- [x] `sql/sync/01_export_to_delta.sql` exists
- [x] `sql/sync/02_sync_to_lakebase.sql` exists
- [x] Justfile sync commands exist

## Phase 4: Tests & Validation

### 4.1 Unit Tests
- [x] Add tests for new metric views in `sql/tests/validate_metric_views.sql`
  - [x] Account Segmentation Coverage (TEST 12)
  - [x] Account Tier Distribution (TEST 13)
  - [x] UCO Data Availability (TEST 14)
  - [x] ASQ-UCO Linkage Rate (TEST 15)
  - [x] UCO Stage Distribution (TEST 16)
  - [x] Competitive Win/Loss Data (TEST 17)
  - [x] Focus & Discipline Data Quality (TEST 18)

### 4.2 Integration Tests
- [ ] Test sync pipeline end-to-end
- [ ] Validate metric view results against charter metrics

## Phase 5: Documentation

- [x] Update `docs/data-dictionary.md` with new fields
- [x] Update `docs/metrics-tree.md` with new views
- [ ] Update `docs/architecture.md` with enhanced data flow
- [x] Create `docs/charter-metrics.md` mapping implementation to charter
- [x] Update `tasks/lessons.md` with any learnings

## Charter Metrics Coverage

| # | Charter Metric | Status | Implementation |
|---|----------------|--------|----------------|
| 1 | ARR Influenced | ✅ Done | mv_pipeline_impact (Production Pipeline, Production ARR) |
| 2 | Competitive Win Rate | ✅ Done | mv_competitive_analysis |
| 3 | Time-to-Production | ✅ Done | mv_uco_velocity |
| 4 | Time-to-Adopt | ❌ External data | Requires product usage correlation |
| 5 | Asset Reuse Rate | ❌ Salesforce changes | Requires schema changes |
| 6 | ASQ Deflection Rate | ❌ Unmeasurable | No baseline available |
| 7 | Product Impact | ❌ External data | Engineering system integration |
| 8 | Customer Risk Reduction | ❌ External data | Churn score correlation |
| 9 | Focus & Discipline (80% L400+) | ✅ Done | mv_focus_discipline |

## Done

- [x] Create implementation plan (this file)
- [x] Create jujutsu branch `ssa-metrics-implementation`
- [x] Review implementation guide
- [x] Create mv_focus_discipline.sql
- [x] Create mv_uco_velocity.sql
- [x] Create mv_competitive_analysis.sql
- [x] Update metric_views.yml with new views
- [x] Enhance dim_account.sql with segmentation
- [x] Enhance fact_uco.sql with velocity/competitive fields
- [x] Add validation tests for new metric views
- [x] Create docs/charter-metrics.md
- [x] Update docs/data-dictionary.md
- [x] Update docs/metrics-tree.md
- [x] Add justfile charter metric commands

## Phase 6: ASQ Logfood Queries Integration

### 6.1 Query Files (cjc- prefix)
- [x] `sql/logfood/cjc-asq-evaluation.sql` - Complete ASQ evaluation with hygiene/urgency
- [x] `sql/logfood/cjc-asq-by-manager.sql` - ASQs filtered by manager hierarchy
- [x] `sql/logfood/cjc-asq-with-ucos.sql` - ASQs linked to UCOs
- [x] `sql/logfood/cjc-asq-hygiene-summary.sql` - Hygiene violations by SSA
- [x] `sql/logfood/cjc-asq-team-capacity.sql` - Team workload distribution
- [x] `sql/logfood/cjc-uco-competitive.sql` - Competitive analysis for UCOs
- [x] `sql/logfood/cjc-asq-region-summary.sql` - Region overview (no params)

### 6.2 Field Corrections Applied
- [x] Changed `individual_hierarchy_field` → `individual_hierarchy_salesforce`
- [x] Changed `manager_level_1_id` → `line_manager_id`
- [x] Changed UCO stage values from long form to short form (U1, U2, etc.)
- [x] Changed `competitor_status` → `competitors` (competitor_status is empty)
- [x] Changed LEFT JOIN to INNER JOIN in UCO queries (reduces result size)

### 6.3 Validation Tests
- [x] Create `sql/tests/validate_asq_queries.sql` (12 tests)
- [x] Run tests against logfood workspace
- [x] All tests passing

### 6.4 Documentation
- [x] Copy implementation plan to `docs/ssa-ops-asq-queries-plan.md`
- [x] Create `sql/logfood/README.md` with query usage guide
- [x] Update `docs/data-dictionary.md` with logfood query references

## Remaining Work

- [ ] Update docs/architecture.md with enhanced data flow diagram
- [ ] Deploy to dev and test metric views

## Done (ASQ Integration)

- [x] Copy 7 ASQ queries from tickets workspace
- [x] Fix field names to match GTM data model
- [x] Validate all queries against logfood data
- [x] Create sql/logfood/README.md
- [x] Create sql/tests/validate_asq_queries.sql (12 tests)
- [x] Update docs/data-dictionary.md with query references
- [x] Update tasks/lessons.md with corrections

## Review Notes

### Implementation Approach
- Used Databricks Metric Views (native aggregation) rather than SQL views
- All metric views follow pattern: NO HARDCODED FILTERS - filtering at query time
- Joins use GTM Silver for snapshots, GTM Gold for curated data
- Account segmentation joined from `main.gtm_gold.rpt_account_dim`
- UCO velocity uses `days_in_stage` from use_case_detail
- Manager hierarchy uses `individual_hierarchy_salesforce`

### Key Design Decisions (CORRECTED)
1. **Priority Account Definition**: A+/A tier, Focus Account, OR `is_strategic_account_ind = TRUE`
2. **Competitor Normalization**: `competitors` field mapped to categories (Microsoft, Snowflake, AWS, etc.)
3. **Effort Default**: When actual/estimated effort missing, default to 5 days
4. **Stage Categories**: UCO stages are U1-U6, Lost, Disqualified
5. **Win/Loss Definition**: Win = stage U6 (Live), Loss = stage Lost

### Data Source Corrections Applied
| Original Assumption | Actual Data |
|---------------------|-------------|
| `account_segmentation` table | `rpt_account_dim` table |
| Account tiers L100-L500 | A+, A, B, C, Focus Account |
| `is_bu_plus_one` flag | `is_strategic_account_ind`, `is_focus_account_ind` |
| `competitor_status` field | `competitors` field |
| `current_stage_days_count` | `days_in_stage` |
| `use_case_stage` = '1-Identified' | `stage` = 'U1' |
| `use_case_status` = 'Closed Won' | `stage` = 'U6' |
| `individual_hierarchy_field` | `individual_hierarchy_salesforce` |
| `manager_level_1_name` | `line_manager_name` |

### Files Created/Modified
- `sql/metric-views/mv_focus_discipline.sql` (new)
- `sql/metric-views/mv_uco_velocity.sql` (new)
- `sql/metric-views/mv_competitive_analysis.sql` (new)
- `sql/tables/01_dim_account.sql` (enhanced)
- `sql/tables/02_fact_uco.sql` (enhanced)
- `sql/tests/validate_metric_views.sql` (enhanced)
- `infra/resources/metric_views.yml` (enhanced)
- `docs/charter-metrics.md` (new)
- `docs/data-dictionary.md` (enhanced)
- `docs/metrics-tree.md` (enhanced)
- `justfile` (enhanced with charter metric commands)
