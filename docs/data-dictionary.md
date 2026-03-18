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
| `paid_usage_metering` | `main.fin_live_gold` | DBU consumption data |
| `dim_workday_attributes_latest` | `main.metric_store` | Org hierarchy (for team filtering) |

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
- `SELECT` on `main.fin_live_gold.*` (for consumption)
