# SSA Charter Evaluation Criteria

## Philosophy

Success is defined by **production outcomes**, **competitive wins**, and **reduced future dependency** through reusable leverage — not by volume of engagement or advisory activity.

---

## Charter-Defined KPIs

| # | Metric | Description | Weight |
|---|--------|-------------|--------|
| 1 | **ARR Influenced** | ARR from SSA-led UCOs reaching production | HIGH |
| 2 | **Competitive Win Rate** | Win rate on SSA-engaged opportunities | HIGH |
| 3 | **Time-to-Production** | Reduction in days U3→U5 | MEDIUM |
| 4 | **Focus & Discipline** | 80% effort on L400+ or BU+1 work | HIGH |

---

## Evaluation Scoring

### Positive Signals (+)

| Signal | Points | Detection |
|--------|--------|-----------|
| UCO at U6 (Live/Production) | +15 | UCO stage contains "U6" |
| UCO at U4/U5 (Near Win) | +10 | UCO stage contains "U4" or "U5" |
| Competitive displacement | +10 | Keywords: fabric, synapse, snowflake, etc. |
| UCO has competitor status | +5 | UCO `competitor_status` or `primary_competitor` set |
| Win signal in notes | +5 | Keywords: production, go live, signed, closed won |

### Negative Signals (-)

| Signal | Points | Detection |
|--------|--------|-----------|
| >90 days without production | -10 | `days_open > 90` AND no U5/U6 UCO |
| >30 days without UCO linkage | -5 | `days_open > 30` AND no UCOs on account |
| Risk/churn keywords | -5 | Keywords: churn, cancel, risk, unhappy |

---

## Alignment Categories

| Score | Category | Action |
|-------|----------|--------|
| ≥20 | HIGHLY_ALIGNED | Prioritize, ensure support |
| 10-19 | ALIGNED | Standard attention |
| 0-9 | NEUTRAL | Review for opportunities |
| <0 | MISALIGNED | Evaluate for closure/reassignment |

---

## Competitive Keywords

```python
COMPETITIVE_KEYWORDS = [
    "fabric", "synapse", "snowflake", "redshift", "bigquery",
    "power bi", "tableau", "looker", "compete", "displacement",
    "migrate from", "replacing", "vs databricks", "alternative"
]
```

---

## Focus & Discipline (L400+ Goal)

**Target:** 80% of SSA effort on L400+, L500, or BU+1 accounts

**Current Status:** BLOCKED — `main.gtm_gold.account_segmentation` table has no data

**Workaround:** Manual tracking until table is populated

---

## UCO Stage Reference

| Stage | Description | Charter Value |
|-------|-------------|---------------|
| U1 - Identified | Early discovery | Low |
| U2 - Qualifying | Validation | Low |
| U3 - Scoping | Technical scoping | Medium (SSA engaged) |
| U4 - Confirming | Technical win | HIGH (Near Win) |
| U5 - Onboarding | Implementation | HIGH (Production) |
| U6 - Live | In production | HIGHEST (Go Live) |

---

## Weekly Charter Review

1. Identify ASQs >60 days without U4+ UCO
2. Review competitive opportunities for proper support
3. Flag misaligned ASQs for manager discussion
4. Celebrate production outcomes (U6)
