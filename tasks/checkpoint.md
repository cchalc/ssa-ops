# SSA-Ops Checkpoint — 2026-03-22

Resume context for ASQ triage and SSA metrics implementation.

## Quick Start

```bash
cd /Users/christopher.chalcraft/cowork/dev/ssa-ops
direnv allow && uv sync
```

**Test connection:**
```bash
uv run python3 -c "
from databricks.sdk import WorkspaceClient
w = WorkspaceClient(profile='logfood')
print('Connected:', w.current_user.me().user_name)
"
```

---

## What's Built

### Skills (local to ssa-ops)

| Skill | Location | Purpose |
|-------|----------|---------|
| `/asq-triage` | `skills/asq-triage/SKILL.md` | Hygiene checks, charter eval, assignment suggestions |
| `/build-ssa-profile` | `skills/build-ssa-profile/SKILL.md` | 2-year SSA work history, specialization analysis |

### SQL Queries

**Local:** `sql/logfood/cjc-*.sql`
**Workspace:** `/Users/christopher.chalcraft@databricks.com/ssa-ops/sql/`

See `docs/query-map.md` for full query reference.

### Reports Generated

| Report | Purpose |
|--------|---------|
| `reports/can-asq-analysis-2026-03-22.md` | Initial CAN ASQ analysis |
| `reports/can-asq-assignment-2026-03-22.md` | Direct reports assignment recommendations |

---

## Current State

### Direct Reports (CJC's Team)

| Name | User ID | Status | Specialization |
|------|---------|--------|----------------|
| Allan Cao | `0058Y00000CPeiKQAT` | LIGHT | Data Governance |
| Harsha Pasala | `0058Y00000CP6yKQAT` | LIGHT | Data Engineering |
| Mathieu Pelletier | `0058Y00000CPn0bQAD` | MODERATE | Geospatial |
| Volodymyr Vragov | `005Vp000002lC2zIAE` | MODERATE | Data Science/ML |
| Réda Khouani | `0053f000000Wi00AAC` | HEAVY | Data Analytics |
| Scott McKean | `005Vp0000016p45IAA` | HEAVY | Data Science/ML |

**Config:** `management/README.md`

### Unassigned ASQs (CAN, Last 30 Days)

- **33 unassigned** ASQs identified
- Most linked to U6 (production) UCOs
- Assignment recommendations generated in `reports/can-asq-assignment-2026-03-22.md`

---

## Key Learnings (from tasks/lessons.md)

### GTM Field Names

| Correct | Incorrect |
|---------|-----------|
| `stage` = U1-U6 | `use_case_stage` = '1-Identified' |
| `competitors` | `competitor_status` (empty) |
| `monthly_total_dollar_dbus` | `total_monthly_dbu_dollars` |
| `user_id`, `user_name` | `individual_id`, `individual_name` |

### SSA Hierarchy

- GTM hierarchy (`individual_hierarchy_salesforce`) only has AE/sales roles
- SSA managers NOT in hierarchy table
- Direct reports must be configured locally

### Databricks SDK

- `wait_timeout` must be 5-50 seconds (not 60s)
- Use `TRY_CAST()` for DBU fields (may contain empty strings)

---

## Next Steps

1. **Weekly triage:** Run `/asq-triage --region CAN --assign`
2. **Profile updates:** Run `/build-ssa-profile --team CAN` monthly
3. **Calendar integration:** Available via `mcp__google__calendar_event_list`
4. **Slack notifications:** Implement `--slack` flag in asq-triage

---

## Resuming Work

To continue from this checkpoint:

```bash
# 1. Read this checkpoint
cat tasks/checkpoint.md

# 2. Review query map
cat docs/query-map.md

# 3. Check current state
uv run python3 -c "..."  # Run any cjc-* query

# 4. Run triage
/asq-triage --region CAN --assign --dry-run
```

---

*Last updated: 2026-03-22*
