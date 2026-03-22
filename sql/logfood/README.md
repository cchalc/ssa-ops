# ASQ Logfood Queries

Parameterized SQL queries for ASQ evaluation, hygiene tracking, and team capacity analysis.

## Workspace

These queries are designed to run on the **logfood** workspace:
- **Profile**: `logfood`
- **Warehouse**: `927ac096f9833442`
- **Catalog**: `main`

## Query Reference

| Query | Parameters | Purpose |
|-------|------------|---------|
| `cjc-asq-region-summary.sql` | None | Overview of ASQs by region |
| `cjc-asq-evaluation.sql` | `{{ region }}` | Full ASQ evaluation with hygiene/urgency |
| `cjc-asq-by-manager.sql` | `{{ manager_id }}` | ASQs for a manager's direct reports |
| `cjc-asq-hygiene-summary.sql` | `{{ region }}` | Hygiene violations by SSA |
| `cjc-asq-team-capacity.sql` | `{{ manager_id }}` | Team workload distribution |
| `cjc-asq-with-ucos.sql` | `{{ region }}` | ASQs linked to UCOs |
| `cjc-uco-competitive.sql` | `{{ region }}` | Competitive analysis for UCOs |
| `cjc-ssa-profiles.sql` | `{{ user_ids }}` | SSA 2-year work history and specializations |
| `cjc-unassigned-asqs.sql` | `{{ region }}`, `{{ days }}` | Unassigned ASQs with UCO linkage |
| `cjc-direct-reports-config.sql` | None | CJC's direct reports reference |
| `cjc-asq-assignment-match.sql` | `{{ region }}` | Match unassigned ASQs to SSAs by specialization |

## Parameters

### `{{ region }}`
Region code from `region_level_1` field. Common values:
- AMER: `CAN`, `RCT`, `EE & Startup`, `DNB`, `CMEG`, `LATAM`
- AMER Industries: `MFG`, `FINS`, `HLS`, `PS`
- EMEA: `SEMEA`, `UKI`, `Central`, `BeNo`, `Emerging`
- APJ: `ANZ`, `India`, `Asean + GCR`, `Korea`, `Japan`

### `{{ manager_id }}`
Salesforce User ID (18-character) of the manager. Example:
- `0053f000000pKoTAAU` (CJC)

### `{{ user_ids }}`
Comma-separated quoted Salesforce User IDs for SSA profiles. Example:
- `'0058Y00000CPeiKQAT', '0058Y00000CP6yKQAT'`

### `{{ days }}`
Lookback period in days. Example:
- `30` for last 30 days

## Running Queries

### Using Python SDK

```python
from databricks.sdk import WorkspaceClient
from pathlib import Path

w = WorkspaceClient(profile='logfood')

# Read and parameterize query
sql = Path('sql/logfood/cjc-asq-evaluation.sql').read_text()
sql = sql.replace("'{{ region }}'", "'RCT'")

# Execute
result = w.statement_execution.execute_statement(
    warehouse_id='927ac096f9833442',
    statement=sql,
    wait_timeout='30s'
)

for row in result.result.data_array[:10]:
    print(row)
```

### Using Databricks SQL UI

1. Open [logfood workspace SQL editor](https://central-logfood-prodtools-azure-westus.cloud.databricks.com/sql/editor)
2. Copy query content
3. Replace `{{ parameter }}` with actual value
4. Run query

## 5-Rule Hygiene Framework

| Rule | Trigger | Severity |
|------|---------|----------|
| `RULE1_MISSING_NOTES` | Assigned >7 days, no status notes | High |
| `RULE3_STALE` | Open 30-90 days + past/no due date | High |
| `RULE4_EXPIRED` | Due date expired >7 days ago | Critical |
| `RULE5_EXCESSIVE` | Open >90 days | Critical |
| `COMPLIANT` | No violations | Good |

## Urgency Classification

| Level | Condition |
|-------|-----------|
| `CRITICAL` | >14 days overdue |
| `HIGH` | 7-14 days overdue |
| `MEDIUM` | 1-7 days overdue OR stale notes |
| `NORMAL` | On track |

## Source Tables

| Table | Description |
|-------|-------------|
| `main.gtm_silver.approval_request_detail` | ASQ records (snapshot-based) |
| `main.gtm_silver.use_case_detail` | UCO records (snapshot-based) |
| `main.gtm_silver.individual_hierarchy_salesforce` | Manager hierarchy |

## Key Field Names

These queries use corrected GTM field names:

| Field | Source Table | Notes |
|-------|--------------|-------|
| `line_manager_id` | individual_hierarchy_salesforce | NOT `manager_level_1_id` |
| `stage` | use_case_detail | Values: U1-U6, Lost, Disqualified |
| `competitors` | use_case_detail | NOT `competitor_status` (empty) |
| `created_date` | approval_request_detail | NOT `assignment_date` |
| `Title` | approval_request_detail | NOT `request_name` |

## Validation

Run tests with:
```bash
# From ssa-ops directory
uv run python3 -c "
from databricks.sdk import WorkspaceClient
from pathlib import Path

w = WorkspaceClient(profile='logfood')
sql = Path('sql/tests/validate_asq_queries.sql').read_text()
# Execute each SELECT statement...
"
```

See `sql/tests/validate_asq_queries.sql` for validation test suite.
