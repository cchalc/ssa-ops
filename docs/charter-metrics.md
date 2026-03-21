# SSA Charter Metrics Implementation

This document maps SSA charter metrics to their implementation in ssa-ops.

## Charter Metrics Overview

| # | Metric | Status | Implementation | Metric View |
|---|--------|--------|----------------|-------------|
| 1 | ARR Influenced | ✅ Implemented | Production UCOs (U5/U6) with ARR | `mv_pipeline_impact` |
| 2 | Competitive Win Rate | ✅ Implemented | Win/loss on SSA-engaged UCOs | `mv_competitive_analysis` |
| 3 | Time-to-Production | ✅ Implemented | Days in stage, milestone rates | `mv_uco_velocity` |
| 4 | Time-to-Adopt | ❌ External data | Requires product usage correlation | - |
| 5 | Asset Reuse Rate | ❌ Salesforce changes | Requires schema changes | - |
| 6 | ASQ Deflection Rate | ❌ Unmeasurable | No baseline available | - |
| 7 | Product Impact | ❌ External data | Engineering system integration | - |
| 8 | Customer Risk Reduction | ❌ External data | Churn score correlation | - |
| 9 | Focus & Discipline (80% L400+) | ✅ Implemented | Effort on L400+/BU+1 accounts | `mv_focus_discipline` |

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
