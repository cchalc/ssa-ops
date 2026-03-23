# SSA Activity Dashboard - Data Dictionary

## Overview

This document describes all data sources, fields, and views used in the SSA Activity Dashboard.

---

## Source Tables

### Primary Sources

| Table | Catalog.Schema | Description |
|-------|----------------|-------------|
| `approvalrequest__c` | `stitch.salesforce` | ASQ (Approval/Support Request) records |
| `user` | `stitch.salesforce` | Salesforce user records (for owner info) |
| `approved_usecase__c` | `stitch.salesforce` | UCO (Use Case Opportunity) records |
| `account` | `stitch.salesforce` | Customer account records |

### Secondary Sources

| Table | Catalog.Schema | Description |
|-------|----------------|-------------|
| `core_usecase_curated` | `main.gtm_gold` | Enriched UCO data with ARR estimates |
| `account_product_adoption` | `main.gtm_gold` | Product adoption flags by account |
| `rpt_account_dim` | `main.gtm_gold` | Account tiers (A+/A/B/C/Focus Account) and strategic flags |
| `account_obt` | `main.gtm_gold` | Account financials (ARR, DBU consumption, spend_tier) |
| `paid_usage_metering` | `main.fin_live_gold` | DBU consumption data |
| `dim_workday_attributes_latest` | `main.metric_store` | Org hierarchy (for team filtering) |

### GTM Silver Sources (Snapshot Data)

| Table | Catalog.Schema | Description |
|-------|----------------|-------------|
| `approval_request_detail` | `main.gtm_silver` | ASQ snapshot with all fields |
| `use_case_detail` | `main.gtm_silver` | UCO current state with stage, competitors |
| `use_case_detail_history` | `main.gtm_silver` | UCO historical snapshots |
| `individual_hierarchy_salesforce` | `main.gtm_silver` | Manager hierarchy (line_manager, 2nd_line_manager) |

---

## Key Identifiers

### Team Filter

All views filter to CJC team using:
```sql
WHERE u.ManagerId = '0053f000000pKoTAAU'  -- Christopher Chalcraft's SF User ID
```

### ASQ Statuses

| Status | Description | Category |
|--------|-------------|----------|
| `Submitted` | New request, awaiting review | Open |
| `Under Review` | Being evaluated for assignment | Open |
| `In Progress` | Actively being worked | Open |
| `On Hold` | Paused, awaiting customer/info | Open |
| `Completed` | Work finished, documented | Closed |
| `Closed` | Administratively closed | Closed |

---

## Views Reference

### cjc_asq_completed_metrics

Closed ASQ analysis with turnaround time.

| Field | Type | Description |
|-------|------|-------------|
| `ASQ_Number` | string | Salesforce ASQ name (e.g., ASQ-12345) |
| `ASQ_Title` | string | Request title/description |
| `Owner_Name` | string | SSA who completed the ASQ |
| `Days_Total` | int | Total days from creation to completion |
| `Days_In_Progress` | int | Days from assignment to completion |
| `Days_To_Assign` | int | Days from creation to assignment |
| `Completion_Quarter` | string | Format: "2024-Q1" |
| `Delivered_On_Time` | int | 1 if completed by due date, 0 otherwise |
| `Quality_Closure` | int | 1 if has notes + effort logged |

### cjc_asq_sla_metrics

SLA tracking per ASQ.

| Field | Type | Description |
|-------|------|-------------|
| `Days_To_Review` | int | Days until moved to Under Review |
| `Days_To_Assignment` | int | Days until assigned (In Progress) |
| `Days_To_First_Response` | int | Days until first note added |
| `Review_SLA_Met` | int | 1 if review < 2 days |
| `Assignment_SLA_Met` | int | 1 if assignment < 5 days |
| `Response_SLA_Met` | int | 1 if response < 5 days |
| `SLA_Stage` | string | Current SLA milestone |

### cjc_team_summary

Team-level executive metrics (single row).

| Field | Type | Description |
|-------|------|-------------|
| `Total_Open_ASQs` | int | Count of open ASQs |
| `Overdue_ASQs` | int | ASQs past due date |
| `Missing_Notes_ASQs` | int | ASQs without recent notes |
| `Completed_QTD` | int | Completions this quarter |
| `Avg_Turnaround_Days` | decimal | Average days to complete |
| `Team_Capacity_Status` | string | GREEN/YELLOW/RED |
| `Team_Members_Green` | int | SSAs with capacity |
| `Team_Members_Yellow` | int | SSAs at capacity |
| `Team_Members_Red` | int | SSAs over capacity |

### cjc_asq_effort_accuracy

Estimate vs actual comparison.

| Field | Type | Description |
|-------|------|-------------|
| `Estimated_Days` | int | Initial effort estimate |
| `Actual_Days` | int | Logged actual effort |
| `Effective_Actual_Days` | int | Actual or derived from dates |
| `Effort_Ratio` | decimal | actual / estimate ratio |
| `Accuracy_Category` | string | Under/Accurate/Slight Over/Significant Over |
| `Variance_Days` | int | actual - estimate |

### cjc_asq_reengagement

Account engagement tracking.

| Field | Type | Description |
|-------|------|-------------|
| `Account_Name` | string | Customer account |
| `Total_ASQs` | int | Lifetime ASQ count |
| `Unique_SSAs` | int | Different SSAs who worked account |
| `ASQs_YTD` | int | ASQs created this year |
| `ASQs_QTD` | int | ASQs created this quarter |
| `Engagement_Tier` | string | High/Repeat/Single |
| `Is_Repeat_Customer` | int | 1 if 2+ ASQs |

### cjc_asq_uco_linkage

UCO linkage tracking.

| Field | Type | Description |
|-------|------|-------------|
| `Linked_UCO_Id` | string | Associated UCO record ID |
| `UCO_Number` | string | UCO name |
| `UCO_Status` | string | UCO status (Active, Won, etc.) |
| `Estimated_DBUs` | decimal | DBU estimate from UCO |
| `estimated_arr_usd` | decimal | ARR estimate |
| `Has_UCO_Link` | int | 1 if ASQ has UCO |
| `Linkage_Status` | string | Strong Link/Linked/No UCO Link |

### cjc_asq_product_adoption

Product adoption by ASQ accounts.

| Field | Type | Description |
|-------|------|-------------|
| `has_model_serving` | int | Account uses Model Serving |
| `has_feature_store` | int | Account uses Feature Store |
| `has_mlflow` | int | Account uses MLflow |
| `has_vector_search` | int | Account uses Vector Search |
| `has_dlt` | int | Account uses Delta Live Tables |
| `has_serverless_sql` | int | Account uses Serverless SQL |
| `has_unity_catalog` | int | Account uses Unity Catalog |
| `AI_ML_Score` | int | Sum of AI/ML adoption flags (0-4) |
| `Modern_Platform_Score` | int | Sum of platform flags (0-3) |
| `Adoption_Tier` | string | Advanced/Growing/Early/Basic |
| `SSA_Influence_Status` | string | SSA Influenced/Engaged/No SSA |

---

## Derived Metrics

### Capacity Estimation

Estimated days per ASQ based on specialization:

| Specialization Pattern | Estimated Days |
|-----------------------|----------------|
| `%ML%` or `%AI%` | 10 |
| `%Delta%` | 5 |
| `%SQL%` | 3 |
| Support Type = Deep Dive | 8 |
| Support Type = Technical Review | 3 |
| Default | 5 |

### Capacity Thresholds

| Total Estimated Days | Status |
|---------------------|--------|
| ≤ 5 | GREEN |
| ≤ 10 | YELLOW |
| > 10 | RED |

### SLA Targets

| Milestone | Target |
|-----------|--------|
| Submitted → Under Review | < 2 days |
| Under Review → In Progress | < 3 days |
| In Progress → First Note | < 5 days |

---

## Data Freshness

| Source | Update Frequency |
|--------|-----------------|
| Salesforce (stitch) | ~4 hours |
| GTM Gold tables | Daily |
| Finance tables | Daily |

Views are calculated at query time - no materialization required.

---

## Access Requirements

Views are created in `home_christopher_chalcraft.cjc_views` schema.

Required grants:
- `SELECT` on `stitch.salesforce.*`
- `SELECT` on `main.gtm_gold.*`
- `SELECT` on `main.gtm_silver.*`
- `SELECT` on `main.fin_live_gold.*` (for consumption)

---

## Metric Views (Databricks Native)

These metric views are deployed to the fevm-cjc workspace for analytics.

### mv_asq_operations

Core ASQ operational metrics with 30+ measures.

| Field | Type | Description |
|-------|------|-------------|
| `Business Unit` | string | BU: AMER Enterprise & Emerging, AMER Industries, EMEA, APJ |
| `Region` | string | Region: CAN, RCT, FINS, etc. |
| `Owner` | string | SSA owner name |
| `Manager L1-L5` | string | 5-level manager hierarchy |
| `Account` | string | Customer account name |
| `Specialization` | string | Technical focus area |
| `Total ASQs` | int | Total ASQ count |
| `Open ASQs` | int | Currently open ASQs |
| `Overdue ASQs` | int | Past due date, not completed |
| `On-Time Rate` | decimal | % completed on time |
| `Avg Days to Complete` | decimal | Average turnaround time |

### mv_focus_discipline

Focus & Discipline metrics for 80% priority target (Charter Metric #9).

| Field | Type | Description |
|-------|------|-------------|
| `Account Tier` | string | A+, A, B, C, Focus Account |
| `Is Strategic` | string | Yes/No - Strategic account flag |
| `Is Focus Account` | string | Yes/No - Focus account flag |
| `Priority Status` | string | Priority or Non-Priority |
| `Total Effort Days` | decimal | Total SSA effort |
| `Priority Effort Days` | decimal | Effort on A+/A/Focus/Strategic accounts |
| `Priority Effort Rate` | decimal | % effort on priority accounts |
| `Meeting 80% Goal` | int | 1 if rate >= 80%, 0 otherwise |

### mv_uco_velocity

UCO stage velocity for Time-to-Production (Charter Metric #3).

| Field | Type | Description |
|-------|------|-------------|
| `UCO Stage` | string | U1-U6, Lost, Disqualified |
| `Stage Category` | string | Early, Engaged, Tech Win, Production, Go Live, Lost |
| `Avg Days in Stage` | decimal | Average days in current stage (days_in_stage) |
| `Production Rate` | decimal | % UCOs reaching U5+ |
| `Stalled UCOs` | int | UCOs with >30 days in stage |
| `Production+ UCOs` | int | UCOs at U5 or U6 |
| `Loss Rate` | decimal | % UCOs that reached Lost stage |

### mv_competitive_analysis

Competitive win/loss analysis (Charter Metric #2).

| Field | Type | Description |
|-------|------|-------------|
| `Competitor` | string | competitors field (may have multiple, semicolon-separated) |
| `Competitor Category` | string | Microsoft Fabric, Snowflake, AWS, Google BigQuery, etc. |
| `Competitor Type` | string | Microsoft, Snowflake, AWS, Google Cloud, Other, No Competitor |
| `Win Rate` | decimal | U6 (Live) / (U6 + Lost) |
| `Competitive Win Rate` | decimal | Win rate on deals with competitor |
| `Microsoft Displacement Wins` | int | Wins against Fabric/Synapse/Power BI |
| `Snowflake Displacement Wins` | int | Wins against Snowflake |

### mv_pipeline_impact

UCO linkage & pipeline generation.

| Field | Type | Description |
|-------|------|-------------|
| `UCO Stage` | string | UCO pipeline stage |
| `Linked ASQs` | int | ASQs linked to UCOs |
| `Linkage Rate` | decimal | % ASQs with UCO linkage |
| `Total Pipeline` | decimal | Total UCO pipeline amount |
| `Production Pipeline` | decimal | Pipeline at U5+U6 |
| `Won Pipeline` | decimal | Pipeline from won UCOs |
| `Pipeline per Effort Day` | decimal | Efficiency metric |

---

## Account Segmentation

### Account Tiers (from rpt_account_dim)

| Tier | Description | Priority |
|------|-------------|----------|
| A+ | Top strategic accounts | Yes |
| A | Strategic accounts | Yes |
| Focus Account | BU-prioritized focus accounts | Yes |
| Focus Account - HLS | Healthcare focus accounts | Yes |
| Focus Account - Retail | Retail focus accounts | Yes |
| B | Important accounts | No |
| C | Standard accounts | No |

### Strategic Account Flag

Accounts with `is_strategic_account_ind = TRUE` are also considered priority accounts, regardless of tier.

### Focus Account Flag

Accounts with `is_focus_account_ind = TRUE` or `account_tier LIKE 'Focus Account%'`.

### Priority Account Definition

An account is "priority" if ANY of these are true:
- `account_tier IN ('A+', 'A')`
- `account_tier LIKE 'Focus Account%'`
- `is_strategic_account_ind = TRUE`

### Spend Tiers (from account_obt)

| Spend Tier | Description |
|------------|-------------|
| Scaling | High-spend, scaling accounts |
| Ramping | Growing consumption |
| Greenfield Prospect | New prospect |
| Greenfield PAYG | Pay-as-you-go greenfield |

---

## UCO Stages

| Stage | Description | Milestone |
|-------|-------------|-----------|
| U1 | Use case identified | - |
| U2 | Being qualified | - |
| U3 | SSA actively engaged | SSA Engagement |
| U4 | Technical validation | Tech Win |
| U5 | Moving to production | Production |
| U6 | In production | Go Live / Win |
| Lost | Deal lost | Loss |
| Disqualified | Disqualified from pipeline | - |

### Key Fields

- **stage:** Current UCO stage (U1-U6, Lost, Disqualified)
- **days_in_stage:** Days in current stage
- **days_in_pipeline:** Total days in pipeline
- **stuck_in_stage:** Boolean flag for stalled UCOs
- **competitors:** Competitor information (may be semicolon-separated)
- **implementation_status:** Green, Yellow, Red

### Key Milestones

- **Tech Win (U4):** Technical decision made in favor of Databricks
- **Production (U5):** Customer implementing in production
- **Go Live (U6):** Use case fully operational (counts as "Win")
- **Loss:** Lost stage indicates lost deal (for win rate calculation)

---

## ASQ Logfood Queries

Parameterized SQL queries for ASQ operations analysis. Located in `sql/logfood/`.

### Query Summary

| Query | Parameters | Purpose |
|-------|------------|---------|
| `cjc-asq-region-summary` | None | Overview of ASQs by region |
| `cjc-asq-evaluation` | `{{ region }}` | Full evaluation with hygiene/urgency |
| `cjc-asq-by-manager` | `{{ manager_id }}` | ASQs for manager's direct reports |
| `cjc-asq-hygiene-summary` | `{{ region }}` | Hygiene violations by SSA |
| `cjc-asq-team-capacity` | `{{ manager_id }}` | Team workload distribution |
| `cjc-asq-with-ucos` | `{{ region }}` | ASQs linked to UCOs |
| `cjc-uco-competitive` | `{{ region }}` | Competitive analysis |

### 5-Rule Hygiene Framework

| Rule | Trigger | Severity |
|------|---------|----------|
| `RULE1_MISSING_NOTES` | Assigned >7 days, no status notes | High |
| `RULE3_STALE` | Open 30-90 days + past/no due date | High |
| `RULE4_EXPIRED` | Due date expired >7 days ago | Critical |
| `RULE5_EXCESSIVE` | Open >90 days | Critical |
| `COMPLIANT` | No violations | Good |

### Urgency Classification

| Level | Condition |
|-------|-----------|
| `CRITICAL` | >14 days overdue |
| `HIGH` | 7-14 days overdue |
| `MEDIUM` | 1-7 days overdue OR stale notes |
| `NORMAL` | On track |

### Usage

See [sql/logfood/README.md](../sql/logfood/README.md) for detailed usage instructions.

---

## Charter Metrics Reference

See [charter-metrics.md](./charter-metrics.md) for full charter metric implementation details.
