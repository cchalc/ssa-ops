# SSA-Ops Query Map

Complete reference for all logfood queries, their relationships, and how they map to GTM data and SSA metrics.

---

## Query Dependency Graph

```
┌─────────────────────────────────────────────────────────────────────┐
│                         GTM SOURCE TABLES                          │
├─────────────────────────────────────────────────────────────────────┤
│  main.gtm_silver.approval_request_detail (ASQs)                    │
│  main.gtm_silver.use_case_detail (UCOs)                            │
│  main.gtm_silver.individual_hierarchy_salesforce (AE hierarchy)    │
│  main.gtm_gold.account_obt (account metrics)                       │
│  main.gtm_gold.rpt_account_dim (account segmentation)              │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      FOUNDATION QUERIES                             │
├─────────────────────────────────────────────────────────────────────┤
│  cjc-direct-reports-config.sql ──► SSA team definition             │
│  cjc-asq-region-summary.sql ────► Overview (no params)             │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                    ┌──────────────┼──────────────┐
                    ▼              ▼              ▼
┌───────────────────────┐ ┌───────────────────┐ ┌───────────────────────┐
│   HYGIENE QUERIES     │ │  CAPACITY QUERIES │ │   UCO/COMPETE QUERIES │
├───────────────────────┤ ├───────────────────┤ ├───────────────────────┤
│ cjc-asq-evaluation    │ │ cjc-asq-team-     │ │ cjc-asq-with-ucos     │
│ cjc-asq-hygiene-      │ │     capacity      │ │ cjc-uco-competitive   │
│     summary           │ │ cjc-ssa-profiles  │ │ cjc-unassigned-asqs   │
│ cjc-asq-by-manager    │ │                   │ │                       │
└───────────────────────┘ └───────────────────┘ └───────────────────────┘
                    │              │              │
                    └──────────────┼──────────────┘
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    COMPOSITE QUERY                                  │
├─────────────────────────────────────────────────────────────────────┤
│  cjc-asq-assignment-match.sql                                       │
│  ├── Loads direct reports config                                    │
│  ├── Calculates current capacity                                    │
│  ├── Gets 2-year specializations                                    │
│  ├── Finds unassigned ASQs                                          │
│  └── Scores and ranks SSA matches                                   │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         OUTPUTS                                     │
├─────────────────────────────────────────────────────────────────────┤
│  /asq-triage report ──► reports/can-asq-assignment-*.md            │
│  /build-ssa-profile ──► SSA specialization profiles                │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Query Reference

### Foundation Queries

| Query | Params | Tables Used | Output |
|-------|--------|-------------|--------|
| `cjc-direct-reports-config.sql` | None | (hardcoded) | SSA team list |
| `cjc-asq-region-summary.sql` | None | approval_request_detail | Region overview |

### Hygiene Queries (5-Rule Framework)

| Query | Params | Tables Used | Output |
|-------|--------|-------------|--------|
| `cjc-asq-evaluation.sql` | `{{ region }}` | approval_request_detail | Full ASQ eval with hygiene/urgency |
| `cjc-asq-hygiene-summary.sql` | `{{ region }}` | approval_request_detail | Violations by SSA |
| `cjc-asq-by-manager.sql` | `{{ manager_id }}` | approval_request_detail, individual_hierarchy | ASQs for manager's team |

**Hygiene Rules:**
- RULE1: Assigned >7 days, no notes
- RULE3: Open 30-90 days, past/no due date
- RULE4: Due date expired >7 days
- RULE5: Open >90 days

### Capacity Queries

| Query | Params | Tables Used | Output |
|-------|--------|-------------|--------|
| `cjc-asq-team-capacity.sql` | `{{ manager_id }}` | approval_request_detail | Workload by SSA |
| `cjc-ssa-profiles.sql` | `{{ user_ids }}` | approval_request_detail | 2-year history, specializations |

**Capacity Status:**
- LIGHT: <15 effort days
- MODERATE: 15-30 effort days
- HEAVY: 30-50 effort days
- OVERLOADED: >50 effort days

### UCO & Competitive Queries

| Query | Params | Tables Used | Output |
|-------|--------|-------------|--------|
| `cjc-asq-with-ucos.sql` | `{{ region }}` | approval_request_detail, use_case_detail | ASQ-UCO linkage |
| `cjc-uco-competitive.sql` | `{{ region }}` | use_case_detail | Competitive analysis |
| `cjc-unassigned-asqs.sql` | `{{ region }}`, `{{ days }}` | approval_request_detail, use_case_detail | Unassigned with UCO context |

**UCO Stages:**
- U1-U2: Early pipeline
- U3: Scoping
- U4: Confirming (tech win)
- U5: Onboarding
- U6: Live (production)

### Assignment Query

| Query | Params | Tables Used | Output |
|-------|--------|-------------|--------|
| `cjc-asq-assignment-match.sql` | `{{ region }}` | All above | Scored SSA-ASQ matches |

**Scoring Algorithm:**
- Exact specialization match: +20
- Category match: +15
- LIGHT workload: +15
- MODERATE workload: +5
- HEAVY workload: -5
- OVERLOADED: excluded

---

## GTM Table Reference

### Silver Layer (Snapshots)

| Table | Key Fields | Snapshot |
|-------|------------|----------|
| `approval_request_detail` | approval_request_name, owner_user_id, status, technical_specialization, created_date, target_end_date | Daily |
| `use_case_detail` | usecase_id, account_id, stage, monthly_total_dollar_dbus, competitors | Daily |
| `individual_hierarchy_salesforce` | user_id, user_name, line_manager_id (AEs only) | Daily |

### Gold Layer (Aggregated)

| Table | Key Fields | Grain |
|-------|------------|-------|
| `account_obt` | account_id, dbu_dollars_qtd, spend_tier | Quarterly |
| `rpt_account_dim` | account_id, is_strategic_account_ind, is_focus_account_ind | Current |

---

## SSA Metrics Mapping

| Charter Metric | Query | Field/Calculation |
|----------------|-------|-------------------|
| Production Outcomes | `cjc-asq-with-ucos` | UCO stage = U5/U6 |
| Competitive Wins | `cjc-uco-competitive` | competitors != 'No Competitor' |
| Time-to-Production | `cjc-ssa-profiles` | avg completion days |
| Focus & Discipline | (not available) | account_segmentation empty |
| ASQ Hygiene | `cjc-asq-evaluation` | hygiene_status field |
| Team Capacity | `cjc-asq-team-capacity` | planned_effort sum |

---

## Common Patterns

### Snapshot Pattern

Always filter to latest snapshot:
```sql
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM table)
```

### Parameter Substitution

```python
sql = open("query.sql").read()
sql = sql.replace("'{{ region }}'", "'CAN'")
sql = sql.replace("{{ days }}", "30")
```

### Handling Empty DBU Values

```sql
TRY_CAST(monthly_total_dollar_dbus AS DOUBLE)
```

---

## Workspace Locations

| Location | Contents |
|----------|----------|
| `/Users/christopher.chalcraft@databricks.com/ssa-ops/sql/` | All cjc-* queries |
| `main.gtm_silver.*` | Source tables |
| `main.gtm_gold.*` | Aggregated metrics |

---

*See `tasks/checkpoint.md` for current state and `tasks/lessons.md` for field name corrections.*
