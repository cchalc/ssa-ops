# Lessons Learned

Patterns and rules to prevent repeated mistakes.

---

## Shell

- **Always use fish shell syntax** - no bash/zsh constructs
- Use `set VAR (command)` not `VAR=$(command)`
- Use `set -x VAR value` not `export VAR=value`
- **Never use Homebrew** - no `brew` commands on this machine
- Bash tool still executes commands, but use fish-compatible syntax

## Git/Jujutsu

- Use SSH remote (`git@github.com:`) not HTTPS for push access
- `jj split <file>` to separate changes into distinct commits
- `jj bookmark set main -r @-` then `jj git push` to push commits

## Databricks

- Profile managed via `DATABRICKS_CONFIG_PROFILE` env var
- direnv loads profile automatically in project directory
- **logfood workspace has IP ACL** - GitHub Actions IPs are blocked
- Deploy locally: `databricks bundle deploy -t dev`
- Secrets go in `.env` (gitignored), not `.envrc` (committed)

## Metric Views

- **NO HARDCODED FILTERS** - All filtering done at query time via WHERE clauses
- Use `FILTER (WHERE ...)` syntax for conditional aggregation in metric views
- Join GTM Silver for snapshots, GTM Gold for curated/aggregated data
- Always use `snapshot_date = (SELECT MAX...)` pattern for current data

## GTM Data Model (Critical Field Mappings)

### Account Segmentation
- **Source table**: `main.gtm_gold.rpt_account_dim` (NOT `account_segmentation`)
- **Account tiers**: `A+`, `A`, `B`, `C`, `Focus Account`, `Focus Account - HLS`, `Focus Account - Retail`
- **NOT L100-L500** - Those tier names don't exist in actual data
- **Strategic account**: `is_strategic_account_ind` flag
- **Focus account**: `is_focus_account_ind` flag
- **BU top accounts**: `bu_top_accounts` field

### UCO (Use Case) Fields
- **Stage field**: `stage` with values `U1`, `U2`, `U3`, `U4`, `U5`, `U6`, `Lost`, `Disqualified`
- **NOT** `use_case_stage` with values like `1-Identified`, `2-Qualifying`
- **Days in stage**: `days_in_stage` (NOT `current_stage_days_count`)
- **Days in pipeline**: `days_in_pipeline`
- **Stuck flag**: `stuck_in_stage` boolean
- **Competitors**: `competitors` field (NOT `competitor_status`)
  - Values: `No Competitor`, `Microsoft Fabric`, `Snowflake`, `Azure Synapse`, `AWS Redshift`, etc.
  - May contain semicolon-separated multiple values
- **Win = stage U6 (Live)**, Loss = stage Lost (NOT Closed Won/Closed Lost status)
- **UCO ID field**: `usecase_id` (NOT `use_case_id`)

### Individual Hierarchy
- **Source table**: `main.gtm_silver.individual_hierarchy_salesforce` (NOT `individual_hierarchy_field`)
- **Manager L1**: `line_manager_name`, `line_manager_id`
- **Manager L2**: `2nd_line_manager_name`, `2nd_line_manager_id`
- **NOT** `manager_level_1_name`, `manager_level_2_name`

### Account OBT Fields
- **Segment**: `account_segment` (NOT `segment`)
- **Vertical**: `vertical_segment` (NOT `vertical`)
- **Spend tier**: `spend_tier` with values `Scaling`, `Ramping`, `Greenfield Prospect`, `Greenfield PAYG`

## Data Model Design

- **Priority Account** = A+/A tier, Focus Account, OR `is_strategic_account_ind = TRUE`
- **Effort default** = 5 days when actual/estimated missing
- **UCO Milestones**: U3→U4 (Tech Win), U4→U5 (Production), U5→U6 (Go Live)
- Fiscal year ends January 31 (Databricks FY convention)
- Status values vary widely - map to categories (Open/Closed/Approved/On Hold)
- **Always verify field names** by querying `information_schema.columns` before implementation

## Testing

- Validation tests should check join coverage (aim for >80%)
- Include data quality checks: duplicates, NULL rates, date sanity
- Test both positive cases and expected rates (e.g., win rate 30-90%)

## ASQ Queries (cjc- prefix)

### Query Size Considerations
- LEFT JOINs with UCOs can return huge result sets (exceeds inline byte limit)
- Use INNER JOIN when only showing matched records
- Add LIMIT for exploratory queries

### Field Names to Remember
- `competitor_status` is EMPTY - use `competitors` field instead
- UCO stages are short form: U1, U2, U3, U4, U5, U6, Lost, Disqualified
- NOT long form like 'U1 - Identified', 'U2 - Qualifying'

### Query Parameters
- `{{ region }}` - must be quoted: `'{{ region }}'`
- `{{ manager_id }}` - Salesforce User ID (18 chars), also quoted

### UCO DBU Field
- **Monthly DBU**: `monthly_total_dollar_dbus` (NOT `total_monthly_dbu_dollars`)
- May contain empty strings - use `TRY_CAST(field AS DOUBLE)` for numeric operations

---

## Individual Hierarchy (Additional Notes)

### GTM Hierarchy is AE-focused
- `individual_hierarchy_salesforce` only contains AE/sales roles
- **SSA managers are NOT in this table**
- Cannot find SSA direct reports via `line_manager_id` queries
- SSA team structure must be configured locally (see `management/README.md`)

### Hierarchy Table Column Names
- `user_id` (NOT `individual_id`)
- `user_name` (NOT `individual_name`)
- `Title` (NOT `individual_role`)

---

## Direct Reports Configuration

CJC's SSA direct reports are configured in `management/README.md`:

| Name | Salesforce User ID |
|------|-------------------|
| Volodymyr Vragov | `005Vp000002lC2zIAE` |
| Allan Cao | `0058Y00000CPeiKQAT` |
| Harsha Pasala | `0058Y00000CP6yKQAT` |
| Réda Khouani | `0053f000000Wi00AAC` |
| Scott McKean | `005Vp0000016p45IAA` |
| Mathieu Pelletier | `0058Y00000CPn0bQAD` |

---

## Databricks SDK

### Statement Execution
- `wait_timeout` must be between 5-50 seconds (or 0 to disable)
- Use `wait_timeout="50s"` not `wait_timeout="60s"`

---

_Add new lessons as corrections occur._
