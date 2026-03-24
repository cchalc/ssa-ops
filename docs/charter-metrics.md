# SSA Charter Metrics Implementation

This document maps SSA charter metrics to their implementation in ssa-ops.

## Charter Metrics Overview

| # | Metric | Status | Implementation | Metric View |
|---|--------|--------|----------------|-------------|
| 1 | ARR Influenced | ✅ Deployed | Production UCOs (U5/U6) with ARR | `mv_pipeline_impact` |
| 2 | Competitive Win Rate | ✅ Deployed | Win/loss on SSA-engaged UCOs | `mv_competitive_analysis` |
| 3 | Time-to-Production | ✅ Deployed | Days in stage, milestone rates | `mv_uco_velocity` |
| 4 | Time-to-Adopt | ✅ Deployed | U3→U4 stage transition time | `mv_time_to_adopt` |
| 5 | Asset Reuse Rate | ✅ Deployed | Pattern application across accounts | `mv_asset_reuse` |
| 6 | ASQ Deflection Rate | ⚠️ Proxy | Self-service health (engagement gaps) | `mv_self_service_health` |
| 7 | Product Impact | ✅ Deployed | UCO product + adoption flags + DBUs | `mv_product_impact` |
| 8 | Customer Risk Reduction | ✅ Deployed | Competitive wins + mitigation ASQs | `mv_customer_risk_reduction` |
| 9 | Focus & Discipline (80% L400+) | ✅ Deployed | Effort on L400+/BU+1 accounts | `mv_focus_discipline` |

**Legend:** ✅ Deployed & Validated | ⚠️ Proxy Metric

**Deployment Location:** `home_christopher_chalcraft.cjc_views` on logfood workspace

---

## Metric 1: ARR Influenced

**Definition:** Total estimated ARR from UCOs at production stages (U5/U6) where SSA engagement occurred.

**Implementation:**
- Source: `mv_pipeline_impact`
- Key Measures:
  - `Production Pipeline` - Pipeline value at U5+U6
  - `Production UCOs` - Count of UCOs at production
  - `Won Pipeline` - Pipeline from won UCOs

**Sample Query:**
```sql
SELECT
  `Manager L1`,
  MEASURE(`Total ASQs`),
  MEASURE(`Production UCOs`),
  MEASURE(`Production Pipeline`),
  MEASURE(`Won Pipeline`)
FROM mv_pipeline_impact
WHERE `Business Unit` = 'AMER Enterprise & Emerging'
  AND `Fiscal Year` = 2026
GROUP BY ALL;
```

---

## Metric 2: Competitive Win Rate

**Definition:** Win rate on UCOs where a competitor was identified, tracking displacement of Power BI, Synapse, Fabric.

**Implementation:**
- Source: `mv_competitive_analysis`
- Key Measures:
  - `Competitive Win Rate` - Wins / (Wins + Losses) for competitive deals
  - `Microsoft Displacement Wins` - Wins against Microsoft stack
  - `Microsoft Displacement Pipeline` - Pipeline won from Microsoft

**Sample Query:**
```sql
SELECT
  `Competitor Category`,
  MEASURE(`Competitive UCOs`),
  MEASURE(`Competitive Wins`),
  MEASURE(`Competitive Win Rate`),
  MEASURE(`Won Competitive Pipeline`)
FROM mv_competitive_analysis
WHERE `Competitor Category` != 'No Competitor'
GROUP BY ALL
ORDER BY `Competitive UCOs` DESC;
```

**Competitor Categories:**
- Microsoft Power BI
- Microsoft Synapse
- Microsoft Fabric
- Microsoft Azure
- Snowflake
- AWS
- Google Cloud
- Other

---

## Metric 3: Time-to-Production

**Definition:** How quickly SSA-engaged UCOs progress from scoping (U3) to production (U5).

**Implementation:**
- Source: `mv_uco_velocity`
- Key Measures:
  - `Avg Days in Stage` - Average days UCOs spend in current stage
  - `Production Rate` - Percentage of UCOs reaching U5+
  - `Stalled UCOs` - UCOs with >30 days in same stage

**UCO Stage Pipeline:**
```
U1 (Identified) → U2 (Qualifying) → U3 (Scoping) → U4 (Confirming) → U5 (Onboarding) → U6 (Live)
                                    ↑ SSA Engaged    ↑ Tech Win        ↑ Production     ↑ Go Live
```

**Sample Query:**
```sql
SELECT
  `Owner`,
  MEASURE(`Total UCOs`),
  MEASURE(`Production+ UCOs`),
  MEASURE(`Production Rate`),
  MEASURE(`Avg Days in Stage`),
  MEASURE(`Stalled Rate`)
FROM mv_uco_velocity
WHERE `Manager L1` = 'Christopher Chalcraft'
GROUP BY ALL;
```

---

## Metric 4: Time-to-Adopt

**Definition:** Speed of new capability adoption - how quickly SSA-engaged UCOs move from Evaluating (U3) to Tech Win (U4).

**Implementation:**
- Source: `mv_time_to_adopt`
- Key Measures:
  - `Avg Days to Adopt` - Average U3→U4 transition time
  - `Median Days to Adopt` - Median U3→U4 time (less skewed by outliers)
  - `Fast Adoption Rate` - Percentage of adoptions under 14 days
  - `Adoption Rate` - Percentage of UCOs reaching U4

**UCO Adoption Path:**
```
U3 (Evaluating)  → U4 (Tech Win)  → U5 (Production) → U6 (Go Live)
     ↑                  ↑
 SSA Engaged      TIME-TO-ADOPT
```

**Sample Query:**
```sql
SELECT
  `Owner`,
  MEASURE(`Total UCOs`),
  MEASURE(`Adopted UCOs`),
  MEASURE(`Avg Days to Adopt`),
  MEASURE(`Fast Adoption Rate`)
FROM mv_time_to_adopt
WHERE `Manager L1` = 'Christopher Chalcraft'
  AND `Fiscal Year` = 2026
GROUP BY ALL
ORDER BY `Avg Days to Adopt`;
```

---

## Metric 5: Asset Reuse Rate

**Definition:** How often SSA patterns/approaches get reused across multiple accounts.

**Implementation:**
- Source: `mv_asset_reuse`
- Key Measures:
  - `Pattern Reuse Rate` - Patterns applied to 2+ accounts / Total patterns
  - `Reused Patterns` - Count of patterns used on multiple accounts
  - `Avg Accounts per Pattern` - Average reach of each pattern

**Pattern Definition:** SSA + Technical Specialization combination

**Sample Query:**
```sql
SELECT
  `Owner`,
  MEASURE(`Total Patterns`),
  MEASURE(`Reused Patterns`),
  MEASURE(`Pattern Reuse Rate`),
  MEASURE(`Max Pattern Reach`)
FROM mv_asset_reuse
WHERE `Manager L1` = 'Christopher Chalcraft'
GROUP BY ALL
ORDER BY `Pattern Reuse Rate` DESC;
```

---

## Metric 6: ASQ Deflection Rate (Proxy)

**Definition:** Since "potential ASQs" cannot be measured, this tracks self-service health as a proxy for deflection.

**Implementation:**
- Source: `mv_self_service_health`
- Key Measures:
  - `Self-Service Rate` - (One-Time + Self-Sufficient accounts) / Total
  - `One-Time Accounts` - Accounts with single ASQ (enabled after one engagement)
  - `Avg Days Between ASQs` - Higher = more self-sufficient

**Self-Service Tiers:**
```
One-Time (Enabled)       - Single ASQ, then self-sufficient
Highly Self-Sufficient   - >180 days between ASQs
Self-Sufficient          - 90-180 days between ASQs
Regular Engagement       - 30-90 days between ASQs
Frequent Dependency      - <30 days between ASQs
```

**Sample Query:**
```sql
SELECT
  `Manager L1`,
  MEASURE(`Total Accounts`),
  MEASURE(`One-Time Accounts`),
  MEASURE(`Self-Sufficient Accounts`),
  MEASURE(`Self-Service Rate`)
FROM mv_self_service_health
WHERE `Business Unit` = 'AMER Enterprise & Emerging'
GROUP BY ALL;
```

---

## Metric 7: Product Impact

**Definition:** Influence on compensation-tied product adoption (Lakeflow, Serverless, Model Serving, Unity Catalog).

**Implementation:**
- Source: `mv_product_impact`
- Key Measures:
  - `Lakeflow Influenced Accounts` - Accounts with Lakeflow adoption
  - `Serverless Influenced Accounts` - Accounts with Serverless adoption
  - `Model Serving Influenced Accounts` - Accounts with Model Serving
  - `Total Product DBUs` - DBU consumption in target products

**Sample Query:**
```sql
SELECT
  `Owner`,
  MEASURE(`Engaged Accounts`),
  MEASURE(`Lakeflow Adoption Rate`),
  MEASURE(`Serverless Adoption Rate`),
  MEASURE(`Model Serving Adoption Rate`),
  MEASURE(`Total Product DBUs`)
FROM mv_product_impact
WHERE `Manager L1` = 'Christopher Chalcraft'
GROUP BY ALL
ORDER BY `Total Product DBUs` DESC;
```

---

## Metric 8: Customer Risk Reduction

**Definition:** Competitive displacement wins and risk mitigation from SSA engagements.

**Implementation:**
- Source: `mv_customer_risk_reduction`
- Key Measures:
  - `Competitive Win Rate` - Wins / (Wins + Losses) for competitive UCOs
  - `Microsoft Displacement Wins` - Wins against Fabric/Synapse/Power BI
  - `Risk Resolution Rate` - Completed risk ASQs / Total risk ASQs

**Risk Contexts:**
- Migration ASQs
- Competitive/Displacement scenarios
- Churn risk or mitigation-tagged ASQs
- Active compete UCOs

**Sample Query:**
```sql
SELECT
  `Owner`,
  MEASURE(`Competitive UCOs`),
  MEASURE(`Competitive Wins`),
  MEASURE(`Competitive Win Rate`),
  MEASURE(`Microsoft Displacement Wins`),
  MEASURE(`Snowflake Displacement Wins`)
FROM mv_customer_risk_reduction
WHERE `Manager L1` = 'Christopher Chalcraft'
GROUP BY ALL
ORDER BY `Competitive Wins` DESC;
```

---

## Metric 9: Focus & Discipline (80% L400+)

**Definition:** 80% of SSA effort should be on L400+, L500, or BU+1 prioritized accounts.

**Implementation:**
- Source: `mv_focus_discipline`
- Key Measures:
  - `Priority Effort Rate` - Effort on priority accounts / Total effort
  - `Meeting 80% Goal` - 1 if rate >= 80%, 0 otherwise
  - `Priority ASQs` - Count of ASQs on L400+/BU+1 accounts

**Account Tier Hierarchy:**
```
L500 (Strategic)     - Top-tier strategic accounts
L400 (Important)     - Key growth accounts
  ↑ PRIORITY ↑
L300 (Standard)      - Standard accounts
L200 (Developing)    - Developing accounts
L100 (Emerging)      - Emerging accounts

BU+1 Flag           - BU-prioritized accounts (any tier)
```

**Sample Query:**
```sql
SELECT
  `Owner`,
  MEASURE(`Total Effort Days`),
  MEASURE(`Priority Effort Days`),
  MEASURE(`Priority Effort Rate`),
  MEASURE(`Meeting 80% Goal`)
FROM mv_focus_discipline
WHERE `Manager L1` = 'Christopher Chalcraft'
  AND `Fiscal Year` = 2026
GROUP BY ALL
ORDER BY `Priority Effort Rate` DESC;
```

**Goal Tracking:**
```sql
-- Team summary: are we meeting the 80% target?
SELECT
  `Manager L1`,
  SUM(MEASURE(`Meeting 80% Goal`)) AS ssas_meeting_goal,
  COUNT(DISTINCT `Owner`) AS total_ssas,
  SUM(MEASURE(`Meeting 80% Goal`)) * 1.0 / COUNT(DISTINCT `Owner`) AS team_compliance_rate
FROM mv_focus_discipline
WHERE `Fiscal Year` = 2026
GROUP BY ALL;
```

---

## Metric Views Summary

### Core Operations
| View | Purpose | Charter Metrics |
|------|---------|-----------------|
| `mv_asq_operations` | Core ASQ metrics (30+ measures) | Base metrics |
| `mv_sla_compliance` | SLA milestone tracking | Operational SLAs |
| `mv_effort_capacity` | Effort estimation & capacity | Capacity planning |

### Pipeline & Revenue
| View | Purpose | Charter Metrics |
|------|---------|-----------------|
| `mv_pipeline_impact` | UCO linkage & pipeline | #1 ARR Influenced |
| `mv_consumption_impact` | DBU/consumption linkage | Revenue correlation |

### Charter-Specific
| View | Purpose | Charter Metrics |
|------|---------|-----------------|
| `mv_focus_discipline` | 80% L400+ effort | #9 Focus & Discipline |
| `mv_uco_velocity` | Time-to-production | #3 Time-to-Production |
| `mv_competitive_analysis` | Win/loss analysis | #2 Competitive Win Rate |
| `mv_time_to_adopt` | U3→U4 stage transition time | #4 Time-to-Adopt |
| `mv_asset_reuse` | Pattern application tracking | #5 Asset Reuse Rate |
| `mv_self_service_health` | Self-service proxy | #6 ASQ Deflection (proxy) |
| `mv_product_impact` | Product adoption influence | #7 Product Impact |
| `mv_customer_risk_reduction` | Competitive wins & risk | #8 Customer Risk Reduction |

### Analysis
| View | Purpose | Charter Metrics |
|------|---------|-----------------|
| `mv_team_comparison` | Cross-team benchmarking | Peer comparison |
| `mv_trend_analysis` | YoY/QoQ trends | Trend tracking |

---

## Data Sources

### GTM Silver (Snapshot Data)
- `approval_request_detail` - ASQ current state
- `use_case_detail` - UCO current state
- `use_case_detail_history` - UCO historical snapshots
- `individual_hierarchy_field` - Manager hierarchy

### GTM Gold (Curated Data)
- `account_obt` - Account financials (ARR, DBU)
- `account_segmentation` - L100-L500 tiers, BU+1 flags
- `core_usecase_curated` - Enriched UCO data

---

## Validation

Run validation tests to ensure data quality:

```bash
just test-metric-views
```

Tests include:
- Account segmentation coverage (>80%)
- Account tier distribution
- UCO data availability
- ASQ-UCO linkage rate
- Win/loss data quality
- Focus & discipline calculation

---

## Refresh Cadence

| Data | Frequency | Notes |
|------|-----------|-------|
| ASQ snapshot | Daily | Via approval_request_detail |
| UCO snapshot | Daily | Via use_case_detail |
| Account OBT | Hourly | Materialized view |
| Account Segmentation | Daily | Updated with account changes |
