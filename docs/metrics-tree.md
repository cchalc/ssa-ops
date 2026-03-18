# SSA Activity Dashboard - Metrics Tree

This document maps SQL views to dashboard sections and SSA performance metrics.

```
SSA ACTIVITY DASHBOARD
│
├── A. EXECUTIVE SUMMARY ─────────────────────────────────────────────────────
│   │
│   │   ┌─────────────────────────────────────────────────────────────────┐
│   └──▶│ VIEW: cjc_team_summary                                          │
│       │                                                                 │
│       │ KPIs:                                                           │
│       │   • Total_Open_ASQs ──────────▶ "Total Open ASQs (team)"       │
│       │   • Overdue_ASQs ─────────────▶ "Overdue ASQs count"           │
│       │   • Missing_Notes_ASQs ───────▶ "ASQs Missing Notes"           │
│       │   • Team_Capacity_Status ─────▶ "Team Capacity (G/Y/R)"        │
│       │   • Completed_QTD ────────────▶ "ASQs Completed This Quarter"  │
│       │   • Avg_Turnaround_Days ──────▶ "Avg Turnaround Time (Days)"   │
│       └─────────────────────────────────────────────────────────────────┘
│
├── B. ASQ LIFECYCLE ─────────────────────────────────────────────────────────
│   │
│   │   ┌─────────────────────────────────────────────────────────────────┐
│   ├──▶│ VIEW: cjc_asq_sla_metrics                                       │
│   │   │                                                                 │
│   │   │ Charts:                                                         │
│   │   │   • Status__c ────────────────▶ Status Distribution (pie)      │
│   │   │   • SLA_Stage ────────────────▶ SLA Stage Breakdown            │
│   │   │   • Created_Week ─────────────▶ Weekly Throughput (line)       │
│   │   │   • Review_SLA_Met ───────────▶ SLA Performance (stacked bar)  │
│   │   │   • Assignment_SLA_Met                                          │
│   │   │   • Response_SLA_Met                                            │
│   │   └─────────────────────────────────────────────────────────────────┘
│   │
│   │   ┌─────────────────────────────────────────────────────────────────┐
│   └──▶│ VIEW: cjc_asq_completed_metrics                                 │
│       │                                                                 │
│       │ Charts:                                                         │
│       │   • Specialization__c ────────▶ Specialization Coverage (bar)  │
│       │   • Support_Type__c ──────────▶ Support Type Distribution      │
│       │   • Days_In_Progress ─────────▶ ASQ Aging Analysis (histogram) │
│       │   • Completion_Week ──────────▶ Weekly Completions (line)      │
│       │   • Delivered_On_Time ────────▶ On-Time Delivery Rate          │
│       └─────────────────────────────────────────────────────────────────┘
│
├── C. INDIVIDUAL SSA PERFORMANCE ────────────────────────────────────────────
│   │
│   │   ┌─────────────────────────────────────────────────────────────────┐
│   │   │ VIEW: cjc_asq_person_metrics (existing)                         │
│   │   │                                                                 │
│   │   │ Metrics:                                                        │
│   │   │   • Total_Open_ASQs ──────────▶ ASQs by SSA (table)            │
│   │   │   • Overdue_Count ────────────▶ Overdue per SSA                │
│   │   │   • Missing_Notes ────────────▶ Notes Compliance               │
│   │   │   • Pct_Missing_Notes ────────▶ % with recent notes            │
│   │   └─────────────────────────────────────────────────────────────────┘
│   │
│   │   ┌─────────────────────────────────────────────────────────────────┐
│   ├──▶│ VIEW: cjc_asq_effort_accuracy                                   │
│   │   │                                                                 │
│   │   │ Metrics:                                                        │
│   │   │   • Effort_Ratio ─────────────▶ Effort Accuracy (actual/est)   │
│   │   │   • Accuracy_Category ────────▶ Estimate Quality Distribution  │
│   │   │   • Variance_Days ────────────▶ Avg Variance vs Team Benchmark │
│   │   │   • Owner_Name GROUP BY ──────▶ Per-SSA Accuracy Heatmap       │
│   │   └─────────────────────────────────────────────────────────────────┘
│   │
│   │   ┌─────────────────────────────────────────────────────────────────┐
│   └──▶│ VIEW: cjc_asq_completed_metrics                                 │
│       │                                                                 │
│       │ Metrics:                                                        │
│       │   • Days_In_Progress ─────────▶ Avg Effort (Days) per SSA      │
│       │   • Quality_Closure ──────────▶ Closure Quality Rate           │
│       │   • COUNT(*) by Owner ────────▶ Completion Rate per SSA        │
│       └─────────────────────────────────────────────────────────────────┘
│
├── D. CUSTOMER ENGAGEMENT ───────────────────────────────────────────────────
│   │
│   │   ┌─────────────────────────────────────────────────────────────────┐
│   └──▶│ VIEW: cjc_asq_reengagement                                      │
│       │                                                                 │
│       │ Metrics:                                                        │
│       │   • COUNT(DISTINCT Account) ──▶ Unique Accounts Supported      │
│       │   • Is_Repeat_Customer ───────▶ Re-engagement Rate             │
│       │   • Total_ASQs (TOP 10) ──────▶ Top 10 Accounts by Volume      │
│       │   • Engagement_Tier ──────────▶ Engagement Distribution        │
│       │   • ASQs_QTD, ASQs_YTD ───────▶ Account Activity Trends        │
│       └─────────────────────────────────────────────────────────────────┘
│
├── E. UCO LINKAGE & INFLUENCE ───────────────────────────────────────────────
│   │
│   │   ┌─────────────────────────────────────────────────────────────────┐
│   ├──▶│ VIEW: cjc_asq_uco_linkage                                       │
│   │   │                                                                 │
│   │   │ Metrics:                                                        │
│   │   │   • Has_UCO_Link ─────────────▶ UCO Linkage Rate (% ASQs)      │
│   │   │   • Linkage_Status ───────────▶ Link Quality Distribution      │
│   │   │   • Estimated_DBUs ───────────▶ DBU Influence                  │
│   │   │   • estimated_arr_usd ────────▶ ARR Influence                  │
│   │   └─────────────────────────────────────────────────────────────────┘
│   │
│   │   ┌─────────────────────────────────────────────────────────────────┐
│   └──▶│ VIEW: cjc_asq_uco_summary                                       │
│       │                                                                 │
│       │ Metrics:                                                        │
│       │   • UCO_Linkage_Rate_Pct ─────▶ Linkage Rate by SSA            │
│       │   • Total_Linked_DBUs ────────▶ DBUs per SSA                   │
│       │   • Total_Linked_ARR ─────────▶ ARR Influence per SSA          │
│       └─────────────────────────────────────────────────────────────────┘
│
├── F. PRODUCT ADOPTION INFLUENCE ────────────────────────────────────────────
│   │
│   │   ┌─────────────────────────────────────────────────────────────────┐
│   ├──▶│ VIEW: cjc_asq_product_adoption                                  │
│   │   │                                                                 │
│   │   │ Metrics:                                                        │
│   │   │   • has_model_serving ────────▶ Model Serving Adoption         │
│   │   │   • has_feature_store ────────▶ Feature Store Adoption         │
│   │   │   • has_mlflow ───────────────▶ MLflow Adoption                │
│   │   │   • has_dlt ──────────────────▶ Lakeflow (DLT) Adoption        │
│   │   │   • has_serverless_sql ───────▶ Serverless SQL Migration       │
│   │   │   • has_unity_catalog ────────▶ Unity Catalog Adoption         │
│   │   │   • AI_ML_Score ──────────────▶ AI/ML Maturity Score           │
│   │   │   • Adoption_Tier ────────────▶ Account Sophistication         │
│   │   │   • SSA_Influence_Status ─────▶ SSA Impact Classification      │
│   │   └─────────────────────────────────────────────────────────────────┘
│   │
│   │   ┌─────────────────────────────────────────────────────────────────┐
│   └──▶│ VIEW: cjc_product_adoption_summary                              │
│       │                                                                 │
│       │ Metrics:                                                        │
│       │   • Unique_Accounts_Supported ▶ Total Reach                    │
│       │   • Tier_Advanced/Growing ────▶ Sophistication Distribution    │
│       │   • Total_DBU_Consumption ────▶ SSA-Influenced Consumption     │
│       │   • Total_Model_Serving_DBU ──▶ AI Workload Growth             │
│       └─────────────────────────────────────────────────────────────────┘
│
└── G. OPERATIONAL HEALTH (SLAs) ─────────────────────────────────────────────
    │
    │   ┌─────────────────────────────────────────────────────────────────┐
    └──▶│ VIEW: cjc_asq_sla_metrics                                       │
        │                                                                 │
        │ SLA Targets:                                                    │
        │   • Days_To_Review ───────────▶ Approval Workflow (< 2 days)   │
        │   • Days_To_Assignment ───────▶ Assignment SLA (< 5 days)      │
        │   • Days_To_First_Response ───▶ First Response (< 5 days)      │
        │                                                                 │
        │ Compliance Rates:                                               │
        │   • AVG(Review_SLA_Met) ──────▶ % Approval On-Time             │
        │   • AVG(Assignment_SLA_Met) ──▶ % Assignment On-Time           │
        │   • AVG(Response_SLA_Met) ────▶ % Response On-Time             │
        └─────────────────────────────────────────────────────────────────┘


SSA PERFORMANCE METRICS MAPPING
═══════════════════════════════════════════════════════════════════════════════

┌───────────────────────────────────────────────────────────────────────────────┐
│ CHARTER KPI                    │ SOURCE VIEW              │ KEY FIELDS        │
├───────────────────────────────────────────────────────────────────────────────┤
│ ASQ Volume & Throughput        │ cjc_team_summary         │ Total_Open_ASQs,  │
│                                │ cjc_asq_completed_metrics│ Completed_QTD     │
├───────────────────────────────────────────────────────────────────────────────┤
│ On-Time Delivery               │ cjc_asq_completed_metrics│ Delivered_On_Time │
│                                │ cjc_asq_sla_metrics      │ *_SLA_Met flags   │
├───────────────────────────────────────────────────────────────────────────────┤
│ Estimation Accuracy            │ cjc_asq_effort_accuracy  │ Effort_Ratio,     │
│                                │                          │ Accuracy_Category │
├───────────────────────────────────────────────────────────────────────────────┤
│ Documentation Quality          │ cjc_asq_completed_metrics│ Quality_Closure   │
│                                │ cjc_asq_person_metrics   │ Missing_Notes     │
├───────────────────────────────────────────────────────────────────────────────┤
│ Customer Engagement            │ cjc_asq_reengagement     │ Is_Repeat_Customer│
│                                │                          │ Engagement_Tier   │
├───────────────────────────────────────────────────────────────────────────────┤
│ Business Impact (UCO)          │ cjc_asq_uco_linkage      │ Has_UCO_Link,     │
│                                │ cjc_asq_uco_summary      │ Total_Linked_ARR  │
├───────────────────────────────────────────────────────────────────────────────┤
│ Product Adoption Influence     │ cjc_asq_product_adoption │ AI_ML_Score,      │
│                                │ cjc_product_adoption_sum │ Adoption_Tier     │
├───────────────────────────────────────────────────────────────────────────────┤
│ Team Capacity                  │ cjc_team_summary         │ Team_Capacity_Stat│
│                                │ cjc_asq_capacity         │ Capacity_Status   │
└───────────────────────────────────────────────────────────────────────────────┘


VIEW DEPENDENCIES
═══════════════════════════════════════════════════════════════════════════════

stitch.salesforce.approvalrequest__c ─┬─▶ All cjc_asq_* views
                                      │
stitch.salesforce.user ───────────────┘

stitch.salesforce.approved_usecase__c ──┬─▶ cjc_asq_uco_linkage
main.gtm_gold.core_usecase_curated ─────┘

main.gtm_gold.account_product_adoption ───▶ cjc_asq_product_adoption

cjc_asq_uco_linkage ──────────────────────▶ cjc_asq_uco_summary
cjc_asq_product_adoption ─────────────────▶ cjc_product_adoption_summary
