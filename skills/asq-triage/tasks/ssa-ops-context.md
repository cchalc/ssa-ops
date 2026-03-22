# ASQ Triage Skill — Context for SSA-Ops Session

## Summary

A new skill `asq-triage` has been created and tested in `skills/asq-triage/`. It pulls ASQ data from Databricks logfood, applies hygiene rules and charter evaluation, and generates manager-ready reports.

## Location

```
~/cowork/dev/ssa-ops/skills/asq-triage/
├── SKILL.md                          # Skill definition (invoke via /asq-triage)
├── scripts/
│   ├── asq_manager.py                # Main entry point
│   ├── query_asqs.py                 # Query logfood for ASQs
│   ├── evaluate_charter.py           # Charter alignment scoring
│   ├── generate_report.py            # Markdown report generator
│   ├── analyze_can_simple.py         # Simplified CAN analysis
│   ├── build_ssa_profiles.py         # Build profiles from historical ASQs
│   └── assign_with_profiles.py       # Profile-based assignment recommendations
├── data/
│   └── ssa_profiles.json             # SSA profiles (2 years of history)
├── sql/
│   └── ssa_profiles_view.sql         # Databricks view definition
├── references/
│   ├── hygiene-rules.md              # 5-rule framework docs
│   └── charter-criteria.md           # Charter scoring criteria
└── tasks/
    ├── automation-plan.md            # Full implementation plan
    └── ssa-ops-context.md            # This file
```

**User-scope skill alias:** `~/.claude/skills/asq-triage/SKILL.md` (enables `/asq-triage` from any session)

## Quick Start

```bash
cd ~/cowork/dev/ssa-ops

# Hygiene report for CAN region
uv run skills/asq-triage/scripts/asq_manager.py --region CAN --hygiene

# Full charter evaluation
uv run skills/asq-triage/scripts/asq_manager.py --region CAN --hygiene --charter-eval

# Filter by manager hierarchy
uv run skills/asq-triage/scripts/asq_manager.py --manager-id 0053f000000pKoTAAU --hygiene

# Export JSON
uv run skills/asq-triage/scripts/asq_manager.py --region CAN --json > data/can-asqs.json

# Save report to file
uv run skills/asq-triage/scripts/asq_manager.py --region CAN --hygiene --charter-eval --output reports/can-triage-$(date +%Y%m%d).md
```

## CAN Region Findings (2026-03-22)

| Metric | Value |
|--------|-------|
| Total Open ASQs | 100 |
| RULE5_EXCESSIVE (>90 days) | 100 (100%) |
| CRITICAL urgency | 100 |
| Unassigned | 16 |
| Missing Notes | 96 |
| Charter Aligned | 84% |
| Competitive Opportunities | 68 |
| Near Win (U4/U5) | 57 |
| Misaligned | 11 |

**Top SSAs by ASQ Count:**
- Qi Su: 21 ASQs
- Unassigned: 16 ASQs
- Liliana Tang: 13 ASQs
- Michael Davison: 7 ASQs

**Key Competitive Displacements:** Snowflake, Google BigQuery, Azure Synapse, AWS Redshift

## Available Flags

| Flag | Description |
|------|-------------|
| `--region CAN` | Filter by region (CAN, RCT, FINS, MFG, etc.) |
| `--manager-id <id>` | Filter by manager hierarchy (SFDC User ID) |
| `--hygiene` | Run 5-rule hygiene check |
| `--charter-eval` | Evaluate against SSA charter priorities |
| `--assign` | Suggest assignments (TODO: not implemented) |
| `--slack` | Send Slack DMs (TODO: not implemented) |
| `--report-only` | Generate report without actions |
| `--dry-run` | Preview actions without executing |
| `--output <path>` | Save report to file |
| `--json` | Output raw JSON data |
| `--limit N` | Max ASQs to process (default: 100) |

## Hygiene Rules

| Rule | Trigger | Severity |
|------|---------|----------|
| RULE1 | Assigned >7 days, no notes | HIGH |
| RULE3 | Open 30-90 days, past due | HIGH |
| RULE4 | Due date expired >7 days | CRITICAL |
| RULE5 | Open >90 days | CRITICAL |
| COMPLIANT | No violations | OK |

## Charter Scoring

| Signal | Points |
|--------|--------|
| UCO at U6 (Production) | +15 |
| UCO at U4/U5 (Near Win) | +10 |
| Competitive displacement | +10 |
| Win keywords in notes | +5 |
| >90 days without production | -10 |
| >30 days without UCO linkage | -5 |
| Risk/churn keywords | -5 |

**Alignment Categories:**
- ≥20: HIGHLY_ALIGNED
- 10-19: ALIGNED
- 0-9: NEUTRAL
- <0: MISALIGNED

## SSA Profiles (Built 2026-03-22)

Profiles built from 864 ASQs over 2 years.

**Files:**
- Profiles JSON: `data/ssa_profiles.json`
- Team Summary: `data/team_profiles_summary.md`

| SSA | ASQs | In Progress | Avg Days | Top Expertise | Specialization |
|-----|------|-------------|----------|---------------|----------------|
| Mathieu Pelletier | 212 | 6 | 16.1 | ML/AI, SQL, Platform | Geospatial, Data Eng, Governance |
| Harsha Pasala | 180 | 5 | 20.5 | Data Engineering (79%), ML/AI | Data Engineering |
| Scott McKean | 176 | 14 | 31.5 | ML/AI, GenAI (63%) | Data Science/ML |
| Allan Cao | 115 | 8 | 36.4 | ML/AI, Platform (62%) | Data Governance (79%) |
| Volodymyr Vragov | 106 | 10 | 19.1 | ML/AI, GenAI (47%) | Data Science/ML |
| Réda Khouani | 75 | 7 | 33.3 | ML/AI, SQL, Migration (49%) | Data Engineering, Analytics |

**Team Differentiation:**
- **GenAI/LLM**: Scott McKean (63%), Volodymyr (47%)
- **Data Engineering**: Harsha Pasala (79%)
- **Geospatial**: Mathieu Pelletier (28%)
- **Data Governance/UC**: Allan Cao (79% specialization)
- **Migration**: Réda Khouani (49%), Allan (44%)
- **Fastest turnaround**: Mathieu (16 days), Volodymyr (19 days)

## Profile-Based Assignment

Use `scripts/assign_with_profiles.py` for data-driven recommendations:

```bash
cd ~/cowork/dev/ssa-ops && uv run skills/asq-triage/scripts/assign_with_profiles.py
```

**Scoring Weights:**
- +30-45 pts: Account continuity (prior ASQs with same account)
- +0-20 pts: Technology match (weighted by historical frequency)
- +0-15 pts: Specialization match
- +0-10 pts: Support type match
- -15 to +5 pts: Current workload adjustment

## Next Steps (from automation-plan.md)

1. **Phase 1 (Ready):** Weekly hygiene report automation via `/macos-scheduler`
2. **Phase 2:** Implement Slack notifications (`scripts/slack_notifier.py`)
3. **Phase 3 (DONE):** Assignment suggestions based on specialization and workload
4. **Phase 4:** Google Doc report publishing
5. **Phase 5:** Manager dashboard with trend tracking
6. **Phase 6:** Create `ssa_profile` view in Databricks (SQL at `sql/ssa_profiles_view.sql`)

## Data Sources

- **ASQs:** `main.gtm_silver.approval_request_detail`
- **UCOs:** `main.gtm_silver.use_case_detail`
- **Team:** `main.gtm_silver.individual_hierarchy_field`
- **Warehouse:** `927ac096f9833442` (Shared SQL Endpoint - Stable)
- **Profile:** `logfood` (in ~/.databrickscfg)

## Known Limitations

1. `account_segmentation` table is EMPTY — cannot calculate L400+/BU+1 focus metric
2. Slack integration not yet implemented (TODO)
3. ✅ Assignment suggestions implemented using SSA profiles (see `assign_with_profiles.py`)
4. `individual_hierarchy_field` table incomplete — use direct owner_user_name queries instead
5. Aggregate capacity queries may timeout — use simplified analysis as fallback

---

*Created: 2026-03-22*
*Skill Version: 1.0.0*
