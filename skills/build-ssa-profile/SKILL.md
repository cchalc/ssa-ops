---
name: build-ssa-profile
description: Build SSA work profiles from 2-year ASQ history. Analyzes technical specializations, account types, effort patterns, and calendar availability to match SSAs to new tickets. Use when assigning ASQs or evaluating team specializations.
argument-hint: "[user_id] [--calendar] [--output json|table]"
user-invocable: true
---

# Build SSA Profile — 2-Year Work History Analysis

Generates a comprehensive work profile for an SSA based on their ASQ history, technical specializations, and calendar availability.

## Usage

```bash
# Build profile for a specific SSA
/build-ssa-profile 0058Y00000CPeiKQAT

# Build profile with calendar integration
/build-ssa-profile 0058Y00000CPeiKQAT --calendar

# Build profiles for all CAN direct reports
/build-ssa-profile --team CAN
```

---

## Prerequisites

### Databricks Authentication
Ensure `logfood` profile is configured:
```ini
[logfood]
host = https://adb-2548836972759138.18.azuredatabricks.net
auth_type = databricks-cli
```

### Google Calendar (Optional)
For `--calendar` flag, requires Google MCP server configured.

---

## Step 1 — Parse Arguments

Extract from command:
- `user_id` — Salesforce User ID (18-char)
- `--calendar` — Include calendar meeting analysis
- `--team CAN` — Build profiles for all team members
- `--output json|table` — Output format (default: table)

---

## Step 2 — Query 2-Year Work History

Query ASQ data from the last 2 years for the specified SSA:

```python
from databricks.sdk import WorkspaceClient
from datetime import datetime, timedelta

w = WorkspaceClient(profile="logfood")

# Calculate 2-year lookback
two_years_ago = (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')

query = f"""
WITH ssa_history AS (
    SELECT
        owner_user_id,
        owner_user_name,
        approval_request_id,
        approval_request_name,
        account_name,
        account_id,
        status,
        support_type,
        technical_specialization,
        COALESCE(actual_effort_in_days, estimated_effort_in_days, 5) as effort_days,
        created_date,
        actual_completion_date,
        business_unit,
        region_level_1,
        -- Calculate duration
        DATEDIFF(
            COALESCE(actual_completion_date, CURRENT_DATE),
            created_date
        ) as duration_days
    FROM main.gtm_silver.approval_request_detail
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
      AND owner_user_id = '{{{{ user_id }}}}'
      AND created_date >= '{two_years_ago}'
)
SELECT * FROM ssa_history
ORDER BY created_date DESC
"""

result = w.statement_execution.execute_statement(
    warehouse_id="927ac096f9833442",
    statement=query.replace('{{ user_id }}', user_id),
    wait_timeout="60s"
)
```

---

## Step 3 — Analyze Specializations

Extract technical expertise patterns from work history:

### 3.1 Technical Specialization Distribution

```sql
SELECT
    technical_specialization,
    COUNT(*) as ticket_count,
    SUM(effort_days) as total_effort,
    ROUND(AVG(duration_days), 1) as avg_duration,
    COUNT(CASE WHEN status = 'Complete' THEN 1 END) as completed
FROM ssa_history
GROUP BY technical_specialization
ORDER BY total_effort DESC
```

### 3.2 Support Type Distribution

```sql
SELECT
    support_type,
    COUNT(*) as ticket_count,
    SUM(effort_days) as total_effort
FROM ssa_history
GROUP BY support_type
ORDER BY ticket_count DESC
```

---

## Step 4 — Account Analysis

Identify account types and industries worked with:

### 4.1 Account Frequency

```sql
SELECT
    account_name,
    COUNT(*) as engagement_count,
    SUM(effort_days) as total_effort,
    MIN(created_date) as first_engagement,
    MAX(created_date) as latest_engagement
FROM ssa_history
GROUP BY account_name
ORDER BY engagement_count DESC
LIMIT 20
```

### 4.2 Join with Account Metadata

```sql
SELECT
    a.vertical_segment,
    COUNT(*) as ticket_count,
    SUM(h.effort_days) as total_effort
FROM ssa_history h
LEFT JOIN main.gtm_gold.account_obt a ON h.account_id = a.account_id
WHERE a.fiscal_year_quarter = (SELECT MAX(fiscal_year_quarter) FROM main.gtm_gold.account_obt)
GROUP BY a.vertical_segment
ORDER BY total_effort DESC
```

---

## Step 5 — Success Metrics

Calculate completion rates and efficiency:

```sql
SELECT
    COUNT(*) as total_asqs,
    COUNT(CASE WHEN status = 'Complete' THEN 1 END) as completed,
    COUNT(CASE WHEN status IN ('Approved', 'In Progress', 'New', 'Assigned') THEN 1 END) as open,
    ROUND(100.0 * COUNT(CASE WHEN status = 'Complete' THEN 1 END) / COUNT(*), 1) as completion_rate,
    SUM(effort_days) as total_effort_days,
    ROUND(AVG(CASE WHEN status = 'Complete' THEN duration_days END), 1) as avg_completion_days
FROM ssa_history
```

---

## Step 6 — Current Capacity

Calculate current workload:

```sql
WITH current_load AS (
    SELECT
        owner_user_id,
        owner_user_name,
        COUNT(*) as open_asqs,
        SUM(COALESCE(estimated_effort_in_days, 5)) as planned_effort,
        COUNT(CASE WHEN DATEDIFF(CURRENT_DATE, target_end_date) > 0 THEN 1 END) as overdue_count
    FROM main.gtm_silver.approval_request_detail
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
      AND owner_user_id = '{{ user_id }}'
      AND status IN ('Approved', 'In Progress', 'New', 'Assigned')
    GROUP BY owner_user_id, owner_user_name
)
SELECT
    *,
    CASE
        WHEN planned_effort > 50 THEN 'OVERLOADED'
        WHEN planned_effort > 30 THEN 'HEAVY'
        WHEN planned_effort > 15 THEN 'MODERATE'
        ELSE 'LIGHT'
    END as workload_status
FROM current_load
```

---

## Step 7 — Calendar Integration (--calendar)

If `--calendar` flag is set, analyze recent meetings:

### 7.1 Fetch Calendar Events

Use Google Calendar MCP:

```python
# Get events from last 30 days
events = mcp__google__calendar_event_list(
    calendar_id="primary",
    time_min="2026-02-20T00:00:00Z",
    time_max="2026-03-22T23:59:59Z",
    max_results=100
)
```

### 7.2 Categorize Meetings

Parse meeting titles and attendees to identify:
- **Customer meetings** — External attendees from customer domains
- **Internal syncs** — Team standups, 1:1s, planning
- **Technical reviews** — Architecture reviews, POC sessions
- **Training** — Enablement, certifications

### 7.3 Meeting Time Calculation

```python
total_meeting_hours = sum(event.duration for event in events)
customer_meeting_hours = sum(e.duration for e in events if is_customer_meeting(e))
available_focus_time = (working_hours * days) - total_meeting_hours
```

---

## Step 8 — Generate Profile

Build the SSA profile structure:

```python
profile = {
    "ssa_id": user_id,
    "ssa_name": ssa_name,

    # Work History (2 years)
    "history": {
        "total_asqs": N,
        "completed_asqs": M,
        "completion_rate": X%,
        "total_effort_days": Y,
        "avg_completion_days": Z
    },

    # Technical Expertise
    "specializations": [
        {"name": "Data Engineering", "ticket_count": N, "effort_days": X},
        {"name": "Data Science", "ticket_count": M, "effort_days": Y},
        ...
    ],

    # Account Experience
    "accounts": {
        "total_unique": N,
        "top_accounts": ["Account1", "Account2", ...],
        "industries": ["Financial Services", "Manufacturing", ...]
    },

    # Current Capacity
    "capacity": {
        "open_asqs": N,
        "planned_effort_days": X,
        "overdue_count": Y,
        "workload_status": "MODERATE"
    },

    # Calendar (if --calendar)
    "calendar": {
        "meeting_hours_30d": X,
        "customer_meetings_30d": Y,
        "available_focus_hours": Z
    },

    # Assignment Recommendations
    "best_fit_for": [
        "Data Engineering migrations",
        "Large enterprise accounts",
        "Financial Services vertical"
    ]
}
```

---

## Step 9 — Output

### Table Format (default)

```
╔══════════════════════════════════════════════════════════════════╗
║ SSA Profile: Allan Cao                                          ║
╠══════════════════════════════════════════════════════════════════╣
║ 2-Year Summary                                                  ║
║   Total ASQs: 156 │ Completed: 134 (86%) │ Avg Duration: 18 days ║
║   Total Effort: 892 days                                        ║
╠══════════════════════════════════════════════════════════════════╣
║ Top Specializations                                             ║
║   1. Data Governance    │ 45 tickets │ 312 effort days          ║
║   2. Platform Admin     │ 38 tickets │ 198 effort days          ║
║   3. Data Engineering   │ 28 tickets │ 156 effort days          ║
╠══════════════════════════════════════════════════════════════════╣
║ Current Capacity: LIGHT (13 effort days)                        ║
║   Open ASQs: 8 │ Overdue: 0                                     ║
╠══════════════════════════════════════════════════════════════════╣
║ Best Fit For:                                                   ║
║   • Data Governance requests                                    ║
║   • Unity Catalog implementations                               ║
║   • Financial Services accounts                                 ║
╚══════════════════════════════════════════════════════════════════╝
```

### JSON Format (--output json)

Returns the profile structure as JSON for programmatic use.

---

## Step 10 — Assignment Matching

When used for assignment, compare new ASQ attributes against SSA profiles:

```python
def score_ssa_for_asq(ssa_profile, asq):
    score = 0

    # Specialization match
    if asq.technical_specialization in ssa_profile.specializations:
        score += 20

    # Account experience
    if asq.account_name in ssa_profile.accounts:
        score += 15

    # Industry experience
    if asq.account_industry in ssa_profile.industries:
        score += 10

    # Capacity bonus (lighter load = higher score)
    if ssa_profile.workload_status == 'LIGHT':
        score += 15
    elif ssa_profile.workload_status == 'MODERATE':
        score += 5
    elif ssa_profile.workload_status == 'HEAVY':
        score -= 5
    elif ssa_profile.workload_status == 'OVERLOADED':
        score -= 20

    # Calendar availability
    if ssa_profile.calendar and ssa_profile.calendar.available_focus_hours > 20:
        score += 10

    return score
```

---

## Direct Reports Configuration

Current CAN team direct reports:

| Name | Salesforce User ID |
|------|-------------------|
| Volodymyr Vragov | `005Vp000002lC2zIAE` |
| Allan Cao | `0058Y00000CPeiKQAT` |
| Harsha Pasala | `0058Y00000CP6yKQAT` |
| Réda Khouani | `0053f000000Wi00AAC` |
| Scott McKean | `005Vp0000016p45IAA` |
| Mathieu Pelletier | `0058Y00000CPn0bQAD` |

---

## Related Skills

- `/asq-triage` — Hygiene checks and charter evaluation
- `/ssagent-profile` — Configure SSAgent preferences

---

## Known Limitations

1. **No industry data in ASQ** — Must join with account_obt for vertical info
2. **Calendar requires MCP** — Google Calendar MCP server must be configured
3. **Historical ASQ data** — Uses current snapshot, not historical snapshots
