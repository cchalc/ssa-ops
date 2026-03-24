# Research: Unblocking Charter Metrics 4-8

**Created:** 2026-03-23
**Author:** Research Agent (Ralph Loop Iteration 1)
**Status:** Research Complete - Ready for Implementation Planning

---

## Executive Summary

This document analyzes the 5 blocked charter metrics and proposes data sources, logic, and metric view definitions to unblock them. The key insight is that several of these metrics can be approximated or proxy-measured using existing data sources.

| # | Metric | Original Status | Proposed Status | Data Source |
|---|--------|-----------------|-----------------|-------------|
| 4 | Time-to-Adopt | ❌ Blocked | ✅ Implementable | UCO stage history + use_case_detail_history |
| 5 | Asset Reuse Rate | ❌ Blocked | ⚠️ Partial | FE-IP projects + ASQ cross-reference |
| 6 | ASQ Deflection Rate | ❌ Blocked | ⚠️ Proxy Metric | Account engagement frequency patterns |
| 7 | Product Impact | ❌ Blocked | ✅ Implementable | UCO product_category + consumption change |
| 8 | Customer Risk Reduction | ❌ Blocked | ✅ Implementable | UCO competitive flags + mitigation tags |

---

## Metric 4: Time-to-Adopt

### Original Definition
> Speed of new capability adoption - days from first ASQ to product feature usage

### Problem
The original interpretation required product telemetry data (e.g., "when did customer first use Model Serving after SSA engagement"). This data is not accessible.

### Proposed Reinterpretation
**Time-to-Adopt = Days for UCO to progress from U3 (Evaluating) → U4 (Confirming)**

This measures how quickly SSA-engaged use cases move from "evaluating" to "confirmed technical win" - which is the primary SSA impact zone.

### Data Sources
- `main.gtm_silver.use_case_detail` - Current UCO state with `days_in_stage`
- `main.gtm_silver.use_case_detail_history` - Historical snapshots for stage transition dates

### Implementation Logic

```sql
-- Calculate U3→U4 transition time for SSA-linked UCOs
WITH stage_transitions AS (
  SELECT
    usecase_id,
    stage,
    snapshot_date,
    LAG(stage) OVER (PARTITION BY usecase_id ORDER BY snapshot_date) AS prev_stage,
    LAG(snapshot_date) OVER (PARTITION BY usecase_id ORDER BY snapshot_date) AS prev_snapshot
  FROM main.gtm_silver.use_case_detail_history
),
u3_to_u4 AS (
  SELECT
    usecase_id,
    snapshot_date AS u4_date,
    prev_snapshot AS u3_date,
    DATEDIFF(snapshot_date, prev_snapshot) AS days_to_adopt
  FROM stage_transitions
  WHERE stage = 'U4' AND prev_stage = 'U3'
)
SELECT
  asq.owner_user_name AS ssa,
  AVG(t.days_to_adopt) AS avg_time_to_adopt,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.days_to_adopt) AS median_time_to_adopt
FROM u3_to_u4 t
JOIN main.gtm_silver.approval_request_detail asq
  ON t.usecase_id = asq.linked_uco_id
GROUP BY asq.owner_user_name;
```

### Proposed Metric View: `mv_time_to_adopt`

**Dimensions:**
- Business Unit, Region, Owner, Manager L1-L2
- UCO Product Category
- Fiscal Year/Quarter

**Measures:**
- `Avg Days U3→U4` (Time-to-Adopt)
- `Median Days U3→U4`
- `P90 Days U3→U4`
- `UCOs Adopted (reached U4+)`
- `Adoption Rate` (U4+ / Total U3 engaged)
- `Accelerated Adoptions` (< median days)
- `Slow Adoptions` (> P90 days)

---

## Metric 5: Asset Reuse Rate

### Original Definition
> How often SSA-created assets get reused across engagements

### Problem
No asset registry exists in Salesforce. Assets (notebooks, architectures, templates) are not tracked systematically.

### Proposed Reinterpretation
**Asset Reuse Rate = FE-IP Project involvement across multiple accounts/ASQs**

Based on the FE-IP project data (see `fe-ip-projects-owners.md`), SSAs are involved in multi-account projects. We can measure:
1. Projects that span multiple accounts (reusable patterns)
2. SSA involvement in similar technical_specialization ASQs (pattern application)
3. Cross-pollination: SSAs working on similar use cases across different accounts

### Data Sources
- FE-IP Project registry (to be created as a table)
- `main.gtm_silver.approval_request_detail` - ASQ patterns by specialization
- ASQ `request_description` field - similarity analysis

### Implementation Logic

```sql
-- Pattern: SSA applying similar solutions across accounts
WITH ssa_patterns AS (
  SELECT
    owner_user_name AS ssa,
    technical_specialization,
    COUNT(DISTINCT account_id) AS accounts_with_pattern,
    COUNT(*) AS total_asqs
  FROM main.gtm_silver.approval_request_detail
  WHERE status IN ('Complete', 'Completed', 'Closed')
    AND snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
  GROUP BY owner_user_name, technical_specialization
  HAVING COUNT(DISTINCT account_id) >= 2  -- Same pattern applied to 2+ accounts
)
SELECT
  ssa,
  SUM(accounts_with_pattern) AS reused_patterns,
  SUM(total_asqs) AS total_asqs,
  SUM(CASE WHEN accounts_with_pattern >= 2 THEN total_asqs ELSE 0 END) * 1.0 / SUM(total_asqs) AS reuse_rate
FROM ssa_patterns
GROUP BY ssa;
```

### Proposed Metric View: `mv_asset_reuse`

**Dimensions:**
- Business Unit, Region, Owner, Manager L1-L2
- Technical Specialization
- Support Type
- Fiscal Year/Quarter

**Measures:**
- `Total Patterns Applied` (specialization × account combinations)
- `Multi-Account Patterns` (patterns applied to 2+ accounts)
- `Pattern Reuse Rate` (multi-account patterns / total)
- `Unique Specializations per SSA`
- `Cross-Account Reach` (distinct accounts per pattern)
- `Repeat Pattern ASQs` (ASQs that match a previously-applied pattern)

### Future Enhancement
Create a proper asset registry table:
```sql
CREATE TABLE asset_registry (
  asset_id STRING,
  asset_name STRING,
  asset_type STRING,  -- notebook, architecture, template, demo
  created_by_ssa STRING,
  created_from_asq STRING,
  created_date DATE,
  reuse_count INT,
  account_ids ARRAY<STRING>  -- accounts where used
);
```

---

## Metric 6: ASQ Deflection Rate

### Original Definition
> Tickets avoided via self-service assets: (Total potential asks - Actual ASQs) / Total potential asks

### Problem
No baseline for "potential asks" exists. This is inherently unmeasurable.

### Proposed Reinterpretation
**ASQ Deflection Proxy = Self-Service Engagement Score**

Instead of measuring deflected tickets, measure indicators of successful self-service:
1. Repeat customer rate trending down (fewer repeat asks = better self-enablement)
2. Time between ASQs for same account increasing (longer gaps = self-sufficient)
3. ASQ complexity increasing over time (simple asks being self-served)

### Data Sources
- `main.gtm_silver.approval_request_detail` - Historical ASQ patterns
- Account engagement frequency analysis

### Implementation Logic

```sql
-- Proxy: Are accounts becoming more self-sufficient?
WITH account_engagement AS (
  SELECT
    account_id,
    account_name,
    MIN(created_date) AS first_asq_date,
    MAX(created_date) AS last_asq_date,
    COUNT(*) AS total_asqs,
    COUNT(DISTINCT YEAR(created_date) || QUARTER(created_date)) AS quarters_engaged,
    AVG(DATEDIFF(
      created_date,
      LAG(created_date) OVER (PARTITION BY account_id ORDER BY created_date)
    )) AS avg_days_between_asqs
  FROM main.gtm_silver.approval_request_detail
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
  GROUP BY account_id, account_name
),
account_trajectory AS (
  SELECT
    *,
    CASE
      WHEN avg_days_between_asqs IS NULL THEN 'One-Time'
      WHEN avg_days_between_asqs < 30 THEN 'Frequent Dependency'
      WHEN avg_days_between_asqs < 90 THEN 'Regular Engagement'
      WHEN avg_days_between_asqs < 180 THEN 'Self-Sufficient'
      ELSE 'Highly Self-Sufficient'
    END AS self_service_tier
  FROM account_engagement
)
SELECT
  self_service_tier,
  COUNT(*) AS accounts,
  SUM(total_asqs) AS total_asqs,
  AVG(avg_days_between_asqs) AS avg_engagement_gap
FROM account_trajectory
GROUP BY self_service_tier;
```

### Proposed Metric View: `mv_self_service_health`

**Dimensions:**
- Business Unit, Region, Manager L1-L2
- Account Segment, Vertical
- Self-Service Tier
- Fiscal Year/Quarter

**Measures:**
- `One-Time Accounts` (single ASQ = potentially self-served after)
- `Self-Sufficient Accounts` (>90 days between ASQs)
- `Frequent Dependency Accounts` (<30 days between ASQs)
- `Avg Days Between ASQs` (higher = more self-sufficient)
- `Self-Service Rate` (One-Time + Self-Sufficient / Total)
- `Deflection Proxy Score` (composite)

---

## Metric 7: Product Impact

### Original Definition
> Resolved gaps, PRs, PMF signals - Count of product tickets/PRs filed by SSAs

### Problem
No integration with engineering systems (GitHub PRs, JIRA tickets).

### Proposed Reinterpretation
**Product Impact = UCO Product Adoption correlated with SSA engagement**

Measure whether accounts engaged by SSAs show meaningful adoption of key products:
1. Product consumption growth after ASQ engagement
2. UCO linkage to specific product categories (Lakeflow, Serverless, etc.)
3. Compensation-tied product adoption metrics

### Data Sources
- `main.gtm_silver.use_case_detail` - `use_case_product` field
- `main.gtm_gold.account_obt` - Product consumption metrics
- `main.gtm_gold.account_product_adoption` - Product adoption flags

### Implementation Logic

```sql
-- Product Impact: ASQs tied to product adoption
WITH product_focus AS (
  SELECT
    asq.owner_user_name AS ssa,
    asq.account_id,
    uco.use_case_product AS product_category,
    uco.stage,
    ao.dbu_serverless_compute_qtd,
    ao.dbu_dlt_qtd,
    ao.dbu_model_serving_qtd,
    pa.has_serverless_sql,
    pa.has_dlt,
    pa.has_model_serving,
    pa.has_unity_catalog
  FROM main.gtm_silver.approval_request_detail asq
  JOIN main.gtm_silver.use_case_detail uco ON asq.account_id = uco.account_id
  LEFT JOIN main.gtm_gold.account_obt ao ON asq.account_id = ao.account_id
  LEFT JOIN main.gtm_gold.account_product_adoption pa ON asq.account_id = pa.account_id
  WHERE asq.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
    AND uco.stage IN ('U4', 'U5', 'U6')  -- Confirmed product usage
)
SELECT
  ssa,
  product_category,
  COUNT(DISTINCT account_id) AS accounts_influenced,
  SUM(CASE WHEN has_serverless_sql THEN 1 ELSE 0 END) AS serverless_adopters,
  SUM(CASE WHEN has_dlt THEN 1 ELSE 0 END) AS lakeflow_adopters,
  SUM(CASE WHEN has_model_serving THEN 1 ELSE 0 END) AS model_serving_adopters
FROM product_focus
GROUP BY ssa, product_category;
```

### Proposed Metric View: `mv_product_impact`

**Dimensions:**
- Business Unit, Region, Owner, Manager L1-L2
- UCO Product Category (Lakeflow, Model Serving, Serverless SQL, Unity Catalog, etc.)
- Account Adoption Tier
- Fiscal Year/Quarter

**Measures:**
- `Product UCOs` (UCOs at U4+ with specific product)
- `Product Adoption Count` (accounts with product flag)
- `Serverless Influenced` (ASQ accounts with serverless adoption)
- `Lakeflow Influenced` (ASQ accounts with DLT adoption)
- `Model Serving Influenced` (ASQ accounts with model serving)
- `Unity Catalog Influenced` (ASQ accounts with UC adoption)
- `Product DBU Impact` (DBU consumption in specific product areas)
- `Adoption Acceleration Rate` (% of engaged accounts adopting target products)

---

## Metric 8: Customer Risk Reduction

### Original Definition
> TCO improvement, migration velocity, churn risk - Reduction in churn score post-SSA engagement

### Problem
No direct churn score data available.

### Proposed Reinterpretation
**Customer Risk Reduction = Competitive displacement + Mitigation-tagged ASQs**

Based on the user's guidance, this metric should track:
1. Compete scenarios where Databricks won (displaced competitor)
2. ASQs tagged as "mitigation" or "churn risk" in request type/description
3. UCOs with competitive context that reached production

### Data Sources
- `main.gtm_silver.use_case_detail` - `competitor_status`, `primary_competitor`
- `main.gtm_silver.approval_request_detail` - `support_type`, `request_description`
- `mv_competitive_analysis` (existing metric view)

### Implementation Logic

```sql
-- Customer Risk Reduction: Competitive wins + Mitigation ASQs
WITH competitive_context AS (
  SELECT
    asq.owner_user_name AS ssa,
    asq.account_id,
    asq.account_name,
    asq.support_type,
    asq.request_description,
    uco.primary_competitor,
    uco.competitor_status,
    uco.stage,
    -- Flag risk-related ASQs
    CASE
      WHEN LOWER(asq.support_type) LIKE '%migration%' THEN 'Migration'
      WHEN LOWER(asq.support_type) LIKE '%competitive%' THEN 'Competitive'
      WHEN LOWER(asq.request_description) LIKE '%churn%' THEN 'Churn Risk'
      WHEN LOWER(asq.request_description) LIKE '%mitigation%' THEN 'Mitigation'
      WHEN LOWER(asq.request_description) LIKE '%at risk%' THEN 'At Risk'
      WHEN LOWER(asq.request_description) LIKE '%competitive%' THEN 'Competitive'
      WHEN uco.competitor_status = 'Active' THEN 'Active Compete'
      ELSE NULL
    END AS risk_context
  FROM main.gtm_silver.approval_request_detail asq
  LEFT JOIN main.gtm_silver.use_case_detail uco ON asq.account_id = uco.account_id
  WHERE asq.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
),
risk_reduction AS (
  SELECT
    ssa,
    COUNT(*) AS total_risk_asqs,
    COUNT(CASE WHEN risk_context IS NOT NULL THEN 1 END) AS risk_related_asqs,
    COUNT(CASE WHEN stage IN ('U5', 'U6') AND risk_context IS NOT NULL THEN 1 END) AS risk_resolved,
    COUNT(CASE WHEN primary_competitor LIKE '%Microsoft%' AND stage IN ('U5', 'U6') THEN 1 END) AS microsoft_displacements,
    COUNT(CASE WHEN primary_competitor LIKE '%Snowflake%' AND stage IN ('U5', 'U6') THEN 1 END) AS snowflake_displacements
  FROM competitive_context
  GROUP BY ssa
)
SELECT
  ssa,
  risk_related_asqs,
  risk_resolved,
  risk_resolved * 1.0 / NULLIF(risk_related_asqs, 0) AS risk_reduction_rate,
  microsoft_displacements,
  snowflake_displacements
FROM risk_reduction;
```

### Proposed Metric View: `mv_customer_risk_reduction`

**Dimensions:**
- Business Unit, Region, Owner, Manager L1-L2
- Risk Context (Migration, Competitive, Churn Risk, Mitigation, At Risk)
- Primary Competitor
- Fiscal Year/Quarter

**Measures:**
- `Risk-Related ASQs` (ASQs with risk/compete context)
- `Competitive ASQs` (ASQs with active competitor)
- `Migration ASQs` (ASQs supporting migrations)
- `Churn/Mitigation ASQs` (explicitly tagged)
- `Risk Resolved Count` (risk ASQs with UCO at U5+U6)
- `Risk Reduction Rate` (resolved / total risk)
- `Competitive Wins` (UCOs won against competitors)
- `Microsoft Displacements` (wins against MS stack)
- `Snowflake Displacements` (wins against Snowflake)
- `Risk Pipeline Protected` (ARR/DBU at risk that reached production)

---

## Implementation Roadmap

### Phase 1: Quick Wins (Week 1)
1. **mv_product_impact** - Uses existing data, straightforward joins
2. **mv_customer_risk_reduction** - Leverages mv_competitive_analysis patterns

### Phase 2: Historical Analysis (Week 2)
3. **mv_time_to_adopt** - Requires use_case_detail_history analysis

### Phase 3: Pattern Analysis (Week 3)
4. **mv_asset_reuse** - Pattern matching across ASQs
5. **mv_self_service_health** - Account engagement trajectory analysis

### File Structure

```
sql/metric-views/
├── mv_time_to_adopt.sql          # Charter Metric #4
├── mv_asset_reuse.sql            # Charter Metric #5
├── mv_self_service_health.sql    # Charter Metric #6 (proxy)
├── mv_product_impact.sql         # Charter Metric #7
├── mv_customer_risk_reduction.sql # Charter Metric #8
```

---

## Summary

| Metric | Proposed Approach | Confidence | Effort | SQL File |
|--------|-------------------|------------|--------|----------|
| Time-to-Adopt | UCO U3→U4 transition time | HIGH | Medium | `mv_time_to_adopt.sql` ✅ |
| Asset Reuse Rate | Pattern application across accounts | MEDIUM | Medium | `mv_asset_reuse.sql` ✅ |
| ASQ Deflection Rate | Self-service proxy (engagement gaps) | LOW | Medium | `mv_self_service_health.sql` ✅ |
| Product Impact | Product adoption correlation | HIGH | Low | `mv_product_impact.sql` ✅ |
| Customer Risk Reduction | Competitive + mitigation flags | HIGH | Low | `mv_customer_risk_reduction.sql` ✅ |

---

## Implementation Status

**All 5 metric views deployed to logfood (home_christopher_chalcraft.cjc_views):**

```
sql/metric-views/
├── mv_time_to_adopt.sql          # Charter Metric #4 ✅ DEPLOYED
├── mv_asset_reuse.sql            # Charter Metric #5 ✅ DEPLOYED
├── mv_self_service_health.sql    # Charter Metric #6 (proxy) ✅ DEPLOYED
├── mv_product_impact.sql         # Charter Metric #7 ✅ DEPLOYED
├── mv_customer_risk_reduction.sql # Charter Metric #8 ✅ DEPLOYED
```

## Completion Status

1. ✅ Research complete - data sources identified
2. ✅ SQL metric view files created
3. ✅ Deployed to logfood workspace (`home_christopher_chalcraft.cjc_views`)
4. ✅ Validated with Q1 FY26 insights queries
5. ✅ charter-metrics.md updated
6. ✅ Sample queries tested

### Deployment Notes
- **Workspace**: logfood (adb-2548836972759138.18.azuredatabricks.net)
- **Catalog/Schema**: home_christopher_chalcraft.cjc_views
- **Deployment Date**: 2026-03-23/24

### fevm-cjc Sync (BLOCKED)
fevm-cjc data sync is blocked by permission issue:
```
PERMISSION_DENIED: Principal 4208779108861901 is not part of org: 7474645166465249
```
See `tasks/TODO-fevm-data-sync.md` for the bookmarked plan.
