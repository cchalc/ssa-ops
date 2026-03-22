# ASQ Triage Automation Plan

## Overview

This document outlines the plan to build manager workflow automations using the ASQ Triage skill.

## Current State (Validated 2026-03-22)

**CAN Region ASQ Analysis:**
- **100 open ASQs** (all RULE5_EXCESSIVE - >90 days old)
- **16 unassigned** ASQs
- **96 missing notes** (NO_NOTES status)
- **84% charter aligned** (82 HIGHLY_ALIGNED + 2 ALIGNED)
- **68 competitive opportunities** (Snowflake, BigQuery, Synapse displacements)
- **57 near-win** (U4/U5 stage UCOs)
- **11 misaligned** (STALE_NO_PRODUCTION flag)

**Top SSAs by ASQ Count:**
| SSA | Count | Notes |
|-----|-------|-------|
| Qi Su | 21 | Heavy SAP/Energy load |
| Unassigned | 16 | Need immediate assignment |
| Liliana Tang | 13 | Energy/Retail focused |
| Michael Davison | 7 | - |
| Fernando Vásquez | 4 | - |

## Automation Phases

### Phase 1: Weekly Hygiene Report (Ready Now)

**Trigger:** Every Monday 8:00 AM local
**Command:**
```bash
cd ~/cowork/dev/ssa-ops
uv run skills/asq-triage/scripts/asq_manager.py --region CAN --hygiene --charter-eval --output reports/asq-hygiene-$(date +%Y%m%d).md
```

**Output:** Markdown report saved to `reports/` directory

**launchd Schedule:**
```xml
<key>StartCalendarInterval</key>
<dict>
    <key>Weekday</key><integer>1</integer>
    <key>Hour</key><integer>8</integer>
    <key>Minute</key><integer>0</integer>
</dict>
```

### Phase 2: Slack Notifications

**Dependencies:**
1. Slack OAuth token with chat:write scope
2. SSA → Slack User ID mapping table

**Implementation Steps:**
1. Create `scripts/slack_notifier.py` with:
   - Fetch user mapping from logfood or profile
   - Build DM message per SSA with their violations
   - Send via Slack Web API
2. Add `--slack` flag handler in `asq_manager.py`
3. Test with `--dry-run --slack` first

**Message Template:**
```
🔔 ASQ Hygiene Alert

You have {count} ASQs requiring attention:

{for each asq}
• {AR-NUMBER} - {Account} - {Status}
  Due: {date} | Days Overdue: {days}
  Action: {recommended_action}
{end}

Please update by EOD Friday.
```

### Phase 3: Assignment Suggestions

**Logic:**
1. Match unassigned ASQs by `technical_specialization`
2. Consider current workload (count of open ASQs)
3. Prefer SSAs with <10 active ASQs
4. Weight by SSA expertise areas

**Data Sources:**
- `main.gtm_silver.individual_hierarchy_field` - Team roster
- `main.field_ssa_lakehouse.ssa_expertise` - Specializations (if exists)

**Implementation:**
```python
def suggest_assignment(asq: dict, team: list[dict]) -> str:
    spec = asq.get("technical_specialization", "General")
    candidates = [
        ssa for ssa in team
        if spec in ssa.get("expertise", [])
        and ssa.get("open_asq_count", 0) < 10
    ]
    return min(candidates, key=lambda x: x["open_asq_count"])
```

### Phase 4: Google Doc Reports

**Dependencies:**
1. Google OAuth credentials (via fe-google-tools plugin)
2. Google Docs API access

**Implementation:**
1. Use existing `markdown_to_gdocs.py` converter
2. Create weekly report in shared team folder
3. Link to individual ASQs via SF URLs

### Phase 5: Manager Dashboard

**Components:**
1. **Daily Pulse** - Quick summary of critical items
2. **Weekly Report** - Full hygiene + charter analysis
3. **Trend Tracking** - Compare week-over-week

**Visualization:**
- Use `visual-explainer` skill for HTML charts
- Or Databricks AI/BI dashboard

## Files to Create

| File | Purpose |
|------|---------|
| `scripts/slack_notifier.py` | Send Slack DMs to SSAs |
| `scripts/assignment_suggester.py` | Recommend assignments |
| `scripts/gdoc_publisher.py` | Publish to Google Docs |
| `automations/weekly-hygiene.md` | launchd automation doc |
| `templates/slack_message.md` | Slack message template |
| `templates/weekly_report.md` | Report structure |

## Quick Start Commands

```bash
# Current hygiene report (CAN region)
cd ~/cowork/dev/ssa-ops
uv run skills/asq-triage/scripts/asq_manager.py --region CAN --hygiene

# Full charter evaluation
uv run skills/asq-triage/scripts/asq_manager.py --region CAN --hygiene --charter-eval

# Preview Slack notifications
uv run skills/asq-triage/scripts/asq_manager.py --region CAN --hygiene --slack --dry-run

# Filter by manager hierarchy
uv run skills/asq-triage/scripts/asq_manager.py --manager-id 0053f000000pKoTAAU --hygiene

# Export JSON for further analysis
uv run skills/asq-triage/scripts/asq_manager.py --region CAN --json > data/can-asqs.json
```

## Next Steps

1. [ ] Test Phase 1 automation (weekly report generation)
2. [ ] Implement Slack notification script
3. [ ] Create SSA → Slack mapping table
4. [ ] Set up launchd schedule via `/macos-scheduler`
5. [ ] Build assignment suggestion logic
6. [ ] Integrate with Google Docs

---

*Created: 2026-03-22*
*Status: Phase 1 Ready, Phases 2-5 Planned*
