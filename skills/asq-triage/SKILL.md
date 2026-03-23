---
name: asq-triage
description: SSA Manager tool for triaging and evaluating ASQs from Databricks logfood. Supports hygiene checks, charter alignment evaluation, assignment suggestions, and Slack notifications. Use when asked "asq triage", "triage asqs", "asq hygiene", "charter evaluation", "assign asqs", or "asq report for [region]".
argument-hint: "[--region CAN] [--hygiene] [--charter-eval] [--assign] [--slack] [--dry-run]"
user-invocable: true
---

# ASQ Triage — Logfood-Powered SSA Management

Evaluate, manage, and automate ASQ workflows using Databricks logfood as the data source.

## Flags

| Flag | Description |
|------|-------------|
| `--region CAN` | Filter by region (CAN, RCT, FINS, MFG, etc.) |
| `--manager-id <id>` | Filter by manager hierarchy (SFDC User ID) |
| `--hygiene` | Run 5-rule hygiene check and flag violations |
| `--charter-eval` | Evaluate ASQs against SSA charter priorities |
| `--assign` | Suggest or make SSA assignments for unassigned ASQs |
| `--slack` | Send Slack DMs to affected SSAs |
| `--report-only` | Generate report without taking actions |
| `--dry-run` | Preview actions without executing |

## Usage Examples

```bash
# Full hygiene check for CAN region
/asq-triage --region CAN --hygiene

# Charter evaluation with Slack notifications
/asq-triage --region CAN --charter-eval --slack

# Dry run to preview assignment suggestions
/asq-triage --region CAN --assign --dry-run

# Full report for manager's team
/asq-triage --manager-id 0053f000000pKoTAAU --report-only
```

---

## Prerequisites

### Databricks Authentication
Ensure `logfood` profile is configured in `~/.databrickscfg`:
```ini
[logfood]
host = https://adb-2548836972759138.18.azuredatabricks.net
auth_type = databricks-cli
```

### Environment Setup
```bash
cd /Users/christopher.chalcraft/cowork/dev/ssa-ops
uv sync  # Install dependencies
```

---

## Step 1 — Parse Arguments

Extract flags from the command:
- `region` — filter by region_level_1
- `manager_id` — filter by manager hierarchy
- `hygiene` — run hygiene checks
- `charter_eval` — evaluate against charter
- `assign` — suggest assignments
- `slack` — send notifications
- `report_only` — no actions
- `dry_run` — preview mode

Default: `--region CAN --hygiene --report-only`

---

## Step 2 — Query ASQs from Logfood

Use the Databricks SDK to query `main.gtm_silver.approval_request_detail`:

```python
# scripts/query_asqs.py
from databricks.sdk import WorkspaceClient

w = WorkspaceClient(profile="logfood")
result = w.statement_execution.execute_statement(
    warehouse_id="927ac096f9833442",
    statement=open("sql/logfood/cjc-asq-evaluation.sql").read().replace("{{ region }}", region),
    wait_timeout="30s"
)
```

**Key columns returned:**
- `asq_number`, `asq_title`, `status`
- `account_name`, `assigned_to`
- `days_open`, `days_overdue`
- `hygiene_status` (RULE1-RULE5 or COMPLIANT)
- `urgency` (CRITICAL, HIGH, MEDIUM, NORMAL)
- `notes_status` (HAS_NOTES, NO_NOTES)
- `sf_link` (clickable Salesforce URL)

---

## Step 3 — Hygiene Evaluation (--hygiene)

Apply the 5-rule framework to each ASQ:

| Rule | Trigger | Severity | Action |
|------|---------|----------|--------|
| **RULE1** | Assigned >7 days, no notes | HIGH | Add status update |
| **RULE3** | Open 30-90 days, past/no due date | HIGH | Close or extend |
| **RULE4** | Due date expired >7 days | CRITICAL | Update or close |
| **RULE5** | Open >90 days | CRITICAL | Escalate to manager |
| **COMPLIANT** | No violations | OK | No action |

Group ASQs by hygiene status and assigned SSA.

---

## Step 4 — Charter Evaluation (--charter-eval)

Evaluate ASQs against SSA charter priorities:

### 4.1 Production Outcomes
- Are UCOs linked to the ASQ?
- What stage are the UCOs? (U3-Scoping, U4-Confirming, U5-Onboarding, U6-Live)
- Flag: `NEAR_WIN` if U4/U5, `PRODUCTION` if U6

### 4.2 Competitive Wins
- Does description mention competitors? (Fabric, Synapse, Snowflake, etc.)
- Flag: `COMPETITIVE` if competitive displacement opportunity

### 4.3 Focus & Discipline (80% L400+ Goal)
- **NOTE:** `account_segmentation` table is currently EMPTY
- Cannot calculate L400+/BU+1 percentage at this time
- Flag this as a data gap

### 4.4 Alignment Score
Calculate charter alignment:
- +10 if has NEAR_WIN UCO
- +10 if COMPETITIVE displacement
- +5 if estimated DBU > $10K/mo
- -5 if >90 days without production outcome
- -10 if RULE5_EXCESSIVE

---

## Step 5 — Assignment Suggestions (--assign)

**This step integrates with the `/build-ssa-profile` skill for specialization matching.**

For unassigned ASQs (no `assigned_to`):

### 5.1 Build SSA Profiles

First, load SSA profiles using `sql/logfood/cjc-ssa-profiles.sql`:

```python
# Get 2-year work history and specializations for direct reports
from databricks.sdk import WorkspaceClient
w = WorkspaceClient(profile="logfood")

direct_report_ids = "'005Vp000002lC2zIAE', '0058Y00000CPeiKQAT', '0058Y00000CP6yKQAT', '0053f000000Wi00AAC', '005Vp0000016p45IAA', '0058Y00000CPn0bQAD'"

sql = open("sql/logfood/cjc-ssa-profiles.sql").read()
sql = sql.replace("{{ user_ids }}", direct_report_ids)

profiles = w.statement_execution.execute_statement(
    warehouse_id="927ac096f9833442",
    statement=sql,
    wait_timeout="50s"
)
```

### 5.2 Get Unassigned ASQs

Load unassigned ASQs using `sql/logfood/cjc-unassigned-asqs.sql`:

```python
sql = open("sql/logfood/cjc-unassigned-asqs.sql").read()
sql = sql.replace("'{{ region }}'", f"'{region}'")
sql = sql.replace("{{ days }}", "30")

unassigned = w.statement_execution.execute_statement(
    warehouse_id="927ac096f9833442",
    statement=sql,
    wait_timeout="50s"
)
```

### 5.3 Run Assignment Matching

Use the combined query `sql/logfood/cjc-asq-assignment-match.sql` for automatic matching:

```python
sql = open("sql/logfood/cjc-asq-assignment-match.sql").read()
sql = sql.replace("'{{ region }}'", f"'{region}'")

matches = w.statement_execution.execute_statement(
    warehouse_id="927ac096f9833442",
    statement=sql,
    wait_timeout="50s"
)
```

### 5.4 Matching Algorithm

The assignment match query scores SSAs based on:

| Factor | Score |
|--------|-------|
| Exact specialization match | +20 |
| Category match (e.g., both "Engineering") | +15 |
| LIGHT workload | +15 |
| MODERATE workload | +5 |
| HEAVY workload | -5 |
| OVERLOADED | Excluded from candidates |

### 5.5 Output Assignment Table

```markdown
| ASQ | Account | Spec | Recommended SSA | Status | Match Score |
|-----|---------|------|-----------------|--------|-------------|
| AR-000114455 | Hydro Quebec | Geospatial | Mathieu Pelletier | MODERATE | 35 |
| AR-000114395 | CN Rail | Data Engineering | Harsha Pasala | LIGHT | 35 |
```

If `--dry-run`, show suggestions. Otherwise, use Salesforce CLI to update assignments.

---

## Step 6 — Generate Report

Create a markdown report with:

```markdown
# ASQ Triage Report — {region} — {date}

## Executive Summary
| Metric | Value |
|--------|-------|
| Total Open ASQs | N |
| CRITICAL (>14 days overdue) | X |
| RULE5_EXCESSIVE (>90 days) | Y |
| Unassigned | Z |
| Charter Aligned | A% |

## Hygiene Violations by SSA

### {SSA Name} — {N} violations
- [AR-XXXXXX](sf_link) — RULE5_EXCESSIVE — Account — {days} days
...

## Charter Alignment Issues

### Not Linked to UCO
- [AR-XXXXXX](sf_link) — {description}

### Long-running without Production
- [AR-XXXXXX](sf_link) — {days} days — no U5/U6 UCO

## Recommended Actions

### Immediate (Today)
1. Close [AR-XXXXXX](sf_link) — open {days} days, no recent activity
...

### This Week
1. Assign [AR-XXXXXX](sf_link) to {suggested_ssa}
...
```

---

## Step 7 — Slack Notifications (--slack)

If `--slack` flag is set:

### 7.1 DM to Each Affected SSA
```
🔔 ASQ Hygiene Alert

You have {N} ASQs requiring attention:

CRITICAL:
• AR-XXXXXX (Account) — {days} days overdue
  Action: Update end date or close

HIGH:
• AR-XXXXXXX (Account) — Missing status notes
  Action: Add status update by Thursday

Please address by end of day Thursday.
```

### 7.2 Summary to Manager
```
📊 ASQ Triage Summary — {region}

Total: {N} ASQs analyzed
Violations: {X} hygiene issues across {Y} SSAs

Top Issues:
• {SSA1}: {N} CRITICAL items
• {SSA2}: {N} unassigned waiting

Report: {google_doc_link}
```

---

## Step 8 — Output Options

Use `AskUserQuestion`:
- **"View in terminal"** (Recommended)
- **"Create Google Doc"**
- **"Save to file"**
- **"All of the above"**

### Google Doc Output
```bash
CONVERTER=$(ls ~/.claude/plugins/cache/fe-vibe/fe-google-tools/*/skills/google-docs/resources/markdown_to_gdocs.py 2>/dev/null | sort -V | tail -1)
python3 "$CONVERTER" --input "/tmp/asq_manager_report.md" --title "ASQ Triage — {region} — {date}"
```

---

## Scripts

| Script | Purpose |
|--------|---------|
| `scripts/query_asqs.py` | Query ASQs from logfood |
| `scripts/evaluate_charter.py` | Evaluate charter alignment |
| `scripts/suggest_assignments.py` | Match unassigned to SSAs |
| `scripts/send_slack.py` | Send Slack notifications |

---

## References

- `references/hygiene-rules.md` — Detailed 5-rule framework
- `references/charter-criteria.md` — SSA charter evaluation criteria
- `references/slack-templates.md` — Notification templates

---

## Related Skills

| Skill | Purpose |
|-------|---------|
| `/build-ssa-profile` | Build SSA work profiles from 2-year history for assignment matching |
| `/ssagent-profile` | Configure SSAgent preferences |
| `/ssagent-weekly` | Weekly cadence workflow for all open ASQs |

---

## SQL Queries (on logfood workspace)

Saved queries in `sql/logfood/` and deployed to logfood workspace:

| Query | Purpose |
|-------|---------|
| `cjc-asq-assignment-match.sql` | Match unassigned ASQs to SSAs by specialization |
| `cjc-ssa-profiles.sql` | SSA 2-year work history and specializations |
| `cjc-unassigned-asqs.sql` | Unassigned ASQs with UCO linkage |
| `cjc-direct-reports-config.sql` | CJC's direct reports reference |

Workspace location: `/Users/christopher.chalcraft@databricks.com/ssa-ops/sql/`

---

## Known Limitations

1. **account_segmentation table is EMPTY** — Cannot calculate L400+/BU+1 focus metric
2. **Calendar data requires MCP** — Use `mcp__google__calendar_event_list` for meeting analysis
3. **GTM hierarchy doesn't track SSA managers** — Direct reports configured locally in `management/README.md`
4. **SSA profiles are static** — No automatic refresh; re-run `/build-ssa-profile` for current data
