# Ralph Loop Progress: Blocked Charter Metrics Research

**Task:** Research blocked metric views from the metrics tree and propose implementations

**Status:** ✅ COMPLETE - All 5 blocked metrics researched, implemented, and deployed

---

## Iteration 1 Summary (2026-03-24)

### Research Completed

All 5 blocked charter metrics (4-8) have been:
1. **Researched** - Data sources, field mappings, and implementation logic identified
2. **SQL files created** - Full metric view definitions in `sql/metric-views/`
3. **Deployed to logfood** - Views live in `home_christopher_chalcraft.cjc_views`
4. **Validated** - Q1 FY26 insights extracted for direct reports

---

## Metric-by-Metric Research Summary

### Metric 4: Time-to-Adopt
**Definition:** Speed of new capability adoption (U3→U4 transition time)

**Data Sources:**
- `main.gtm_silver.use_case_detail` - `u3_date_sfdc_original`, `u4_date_sfdc_original`
- `main.gtm_silver.approval_request_detail` - ASQ linkage
- `main.gtm_silver.individual_hierarchy_salesforce` - Manager hierarchy

**Field Mappings:**
| Measure | Expression |
|---------|------------|
| Avg Days to Adopt | `AVG(DATEDIFF(u4_date_sfdc_original, u3_date_sfdc_original))` |
| Adoption Rate | `COUNT(u4_date IS NOT NULL) / COUNT(*)` |
| Fast Adoption Rate | `COUNT(days <= 14) / COUNT(adopted)` |

**Deployed:** `mv_time_to_adopt`

---

### Metric 5: Asset Reuse Rate
**Definition:** How often SSA patterns get reused across accounts

**Data Sources:**
- `main.gtm_silver.approval_request_detail` - `technical_specialization`, `account_id`
- `main.gtm_silver.individual_hierarchy_salesforce` - Manager hierarchy

**Field Mappings:**
| Measure | Expression |
|---------|------------|
| Pattern | `CONCAT(owner_user_id, '|', technical_specialization)` |
| Reused Patterns | Patterns with `COUNT(DISTINCT account_id) >= 2` |
| Pattern Reuse Rate | `Reused Patterns / Total Patterns` |

**Deployed:** `mv_asset_reuse`

---

### Metric 6: ASQ Deflection Rate (Proxy)
**Definition:** Self-service health as proxy for deflection

**Rationale:** "Potential ASQs" cannot be measured, so we track self-sufficiency indicators:
- Accounts with longer gaps between ASQs = more self-sufficient
- One-time accounts = potentially enabled after single engagement

**Data Sources:**
- `main.gtm_silver.approval_request_detail` - Historical ASQ patterns
- `main.gtm_silver.individual_hierarchy_salesforce` - Manager hierarchy

**Field Mappings:**
| Measure | Expression |
|---------|------------|
| Avg Days Between ASQs | `engagement_span_days / (total_asqs - 1)` |
| Self-Service Tier | Based on avg days (>180 = Highly Self-Sufficient, etc.) |
| Self-Service Rate | `(One-Time + Self-Sufficient) / Total` |

**Deployed:** `mv_self_service_health`

---

### Metric 7: Product Impact
**Definition:** Influence on compensation-tied product adoption

**Data Sources:**
- `main.gtm_silver.approval_request_detail` - ASQ data
- `main.gtm_silver.use_case_detail` - `use_case_product` field
- `main.gtm_gold.account_obt` - Product consumption DBUs

**Field Mappings:**
| Measure | Expression |
|---------|------------|
| Has Lakeflow | `dlt_dbu_dollars_qtd > 0` |
| Has Serverless SQL | `dbsql_serverless_dbu_dollars_qtd > 0` |
| Has Model Serving | `genai_gpu_model_serving_dbu_dollars_qtd > 0` |
| Adoption Rate | `Influenced Accounts / Engaged Accounts` |

**Deployed:** `mv_product_impact`

---

### Metric 8: Customer Risk Reduction
**Definition:** Competitive displacement wins + risk mitigation

**Data Sources:**
- `main.gtm_silver.approval_request_detail` - `support_type`, `request_description`
- `main.gtm_silver.use_case_detail` - `primary_competitor`, `competitor_status`
- `main.gtm_gold.core_usecase_curated` - ARR data

**Field Mappings:**
| Measure | Expression |
|---------|------------|
| Risk Context | Parsed from support_type/description (migration, churn, mitigation) |
| Competitor Category | Parsed from primary_competitor (Microsoft, Snowflake, AWS) |
| Competitive Win | `primary_competitor IS NOT NULL AND stage IN ('U5', 'U6')` |
| Win Rate | `Wins / (Wins + Losses)` |

**Deployed:** `mv_customer_risk_reduction`

---

## Documentation Updated

- ✅ `docs/RESEARCH-blocked-metrics.md` - Status updated to DEPLOYED
- ✅ `docs/metrics-tree.md` - All metrics marked ✅ Done
- ✅ `docs/charter-metrics.md` - All metrics marked ✅ Deployed

---

## Remaining Work

### fevm-cjc Data Sync (BLOCKED)
The fevm-cjc workspace sync is blocked by permission issue:
```
PERMISSION_DENIED: Principal 4208779108861901 is not part of org: 7474645166465249
```
See `tasks/TODO-fevm-data-sync.md` for the bookmarked plan.

**Action Required:** Contact fevm-cjc workspace admin to fix serverless permissions.

---

## Completion Promise Check

**Task:** Research blocked metric views and propose definitions
**Status:** ✅ COMPLETE

All 5 metrics have been:
1. Researched with data sources identified
2. Field mappings documented
3. SQL metric views created
4. Deployed to production
5. Validated with live data queries
