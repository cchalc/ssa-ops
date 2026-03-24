# Ralph Loop Session: Unblocking Charter Metrics 4-8

**Session Date:** 2026-03-23
**Iterations:** 3
**Status:** ✅ Complete - Ready for Deployment

## Quick Start

```fish
cd ~/cowork/dev/ssa-ops
just deploy-charter-metrics  # Deploy metrics 4-8
just test-charter-metrics    # Validate
```

---

## Objective

Research and implement the 5 blocked charter metrics from the metrics tree:
1. Time-to-Adopt (#4)
2. Asset Reuse Rate (#5)
3. ASQ Deflection Rate (#6)
4. Product Impact (#7)
5. Customer Risk Reduction (#8)

---

## Deliverables

### 1. Research Documentation
- **File:** `docs/RESEARCH-blocked-metrics.md`
- **Content:**
  - Data source analysis for each metric
  - Field mappings from GTM Silver/Gold tables
  - Proposed metric definitions with SQL logic
  - Implementation confidence ratings

### 2. SQL Metric Views (5 new files)

| File | Charter Metric | Key Data Sources |
|------|---------------|------------------|
| `sql/metric-views/mv_time_to_adopt.sql` | #4 Time-to-Adopt | `use_case_detail_history` |
| `sql/metric-views/mv_asset_reuse.sql` | #5 Asset Reuse | `approval_request_detail` |
| `sql/metric-views/mv_self_service_health.sql` | #6 ASQ Deflection | `approval_request_detail` |
| `sql/metric-views/mv_product_impact.sql` | #7 Product Impact | `use_case_detail`, `account_product_adoption`, `account_obt` |
| `sql/metric-views/mv_customer_risk_reduction.sql` | #8 Customer Risk | `use_case_detail` (competitor fields), ASQ descriptions |

### 3. Validation Tests
- **File:** `sql/tests/validate_charter_metrics.sql`
- **Tests:**
  - Data existence checks
  - Rate validity (0-1 ranges)
  - Sample queries by dimension
  - Summary coverage report

### 4. Documentation Updates

| File | Changes |
|------|---------|
| `docs/metrics-tree.md` | Updated status table with 🔄 Ready flags |
| `docs/charter-metrics.md` | Added full documentation for metrics 4-8 |
| `justfile` | Added `deploy-charter-metrics` and `test-charter-metrics` recipes |

---

## Key Design Decisions

### Metric #4: Time-to-Adopt
- **Approach:** U3→U4 stage transition time from `use_case_detail_history`
- **Why:** This is the primary SSA impact zone - accelerating technical validation
- **Measures:** Avg/Median/P90 days, Fast Adoption Rate, Adoption Speed tiers

### Metric #5: Asset Reuse Rate
- **Approach:** Pattern = SSA + Technical Specialization applied across accounts
- **Why:** No asset registry exists; patterns are the closest measurable proxy
- **Measures:** Pattern Reuse Rate, Reused Patterns, Avg Accounts per Pattern

### Metric #6: ASQ Deflection Rate (Proxy)
- **Approach:** Self-service health based on engagement gaps
- **Why:** "Potential ASQs" cannot be measured - this is the best available proxy
- **Measures:** Self-Service Rate, One-Time Accounts, Avg Days Between ASQs
- **Tiers:** One-Time, Self-Sufficient, Regular, Frequent Dependency

### Metric #7: Product Impact
- **Approach:** UCO product categories + account adoption flags + DBU consumption
- **Why:** Directly tied to compensation targets (Lakeflow, Serverless, etc.)
- **Measures:** Adoption rates by product, Influenced accounts, Product DBUs

### Metric #8: Customer Risk Reduction
- **Approach:** Competitive wins + mitigation-tagged ASQs + displacement tracking
- **Why:** Risk context is captured in UCO competitor fields and ASQ descriptions
- **Measures:** Competitive Win Rate, Displacement wins by competitor, Risk Resolution Rate

---

## Deployment Steps

```fish
# 1. Navigate to project
cd ~/cowork/dev/ssa-ops

# 2. Deploy the new charter metrics
just deploy-charter-metrics

# 3. Validate the metrics
just test-charter-metrics

# 4. (Optional) Deploy all metric views
just deploy-metric-views
```

---

## Sample Queries

### Time-to-Adopt by Team
```sql
SELECT `Manager L1`, MEASURE(`Adopted UCOs`), MEASURE(`Avg Days to Adopt`), MEASURE(`Fast Adoption Rate`)
FROM mv_time_to_adopt
WHERE `Business Unit` = 'AMER Enterprise & Emerging'
GROUP BY ALL;
```

### Product Impact by SSA
```sql
SELECT `Owner`, MEASURE(`Lakeflow Adoption Rate`), MEASURE(`Serverless Adoption Rate`), MEASURE(`Total Product DBUs`)
FROM mv_product_impact
WHERE `Manager L1` = 'Christopher Chalcraft'
GROUP BY ALL;
```

### Competitive Wins by Category
```sql
SELECT `Competitor Category`, MEASURE(`Competitive Wins`), MEASURE(`Competitive Win Rate`)
FROM mv_customer_risk_reduction
WHERE `Competitor Category` != 'No Competitor'
GROUP BY ALL;
```

---

## Files Changed

```
docs/RESEARCH-blocked-metrics.md        # NEW - Full research documentation
docs/metrics-tree.md                    # UPDATED - Status table
docs/charter-metrics.md                 # UPDATED - Metrics 4-8 documentation
sql/metric-views/mv_time_to_adopt.sql   # NEW - Charter Metric #4
sql/metric-views/mv_asset_reuse.sql     # NEW - Charter Metric #5
sql/metric-views/mv_self_service_health.sql # NEW - Charter Metric #6
sql/metric-views/mv_product_impact.sql  # NEW - Charter Metric #7
sql/metric-views/mv_customer_risk_reduction.sql # NEW - Charter Metric #8
sql/tests/validate_charter_metrics.sql  # NEW - Validation tests
justfile                                # UPDATED - New recipes
tasks/ralph-loop-charter-metrics.md     # NEW - This session summary
```

---

## Next Steps

1. **Deploy** - Run `just deploy-charter-metrics` to create views in fevm-cjc
2. **Validate** - Run `just test-charter-metrics` to verify data
3. **Iterate** - Refine SQL based on actual data patterns
4. **Dashboard** - Add these metrics to AI/BI dashboards
5. **Genie** - Enable natural language queries
