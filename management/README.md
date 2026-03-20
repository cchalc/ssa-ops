# SSA Management Notebooks

Management tools and automation for SSA team operations.

## Notebooks

### `asq_overdue_chatter.py`

Automatically finds overdue ASQs for your direct reports and posts Chatter comments prompting for updates.

**What it does:**
1. Connects to Salesforce using OAuth token from Databricks secrets
2. Queries for overdue SSA ASQs owned by configured direct reports
3. Displays them in a table with days overdue, owner, account, and SFDC links
4. Posts Chatter comments on each overdue ASQ

**Configuration:**
- `DIRECT_REPORT_IDS` - List of Salesforce User IDs for your direct reports
- `OVERDUE_COMMENT` - The message to post on overdue ASQs
- `POST_COMMENTS` - Set to `False` for dry-run mode

**Workspace:** [Field Eng Azure](https://adb-984752964297111.11.azuredatabricks.net)

## Authentication Setup

The notebook uses OAuth tokens stored in Databricks secrets (no password required).

### Initial Setup

```bash
# Store your current token
sf-refresh-token
```

### Token Refresh

Tokens expire after a few hours of inactivity. Refresh with:

```bash
sf-refresh-token
```

This fish function:
1. Checks if you're logged in to Salesforce
2. Opens browser for login if needed
3. Extracts the access token
4. Updates the Databricks secret

### Secrets Used

| Scope | Key | Description |
|-------|-----|-------------|
| `salesforce` | `access_token` | OAuth access token |
| `salesforce` | `instance_url` | Salesforce instance URL |

## Deploying Notebooks

To sync notebooks to Databricks:

```bash
# Deploy single notebook
databricks workspace import /Users/christopher.chalcraft@databricks.com/management/asq_overdue_chatter \
  --file management/asq_overdue_chatter.py \
  --language PYTHON \
  --overwrite \
  --profile DEFAULT

# Or add to databricks.yml for DAB deployment
```

## Direct Reports Configuration

Current direct reports (update in notebook if team changes):

| Name | Salesforce User ID |
|------|-------------------|
| Volodymyr Vragov | `005Vp000002lC2zIAE` |
| Allan Cao | `0058Y00000CPeiKQAT` |
| Harsha Pasala | `0058Y00000CP6yKQAT` |
| Réda Khouani | `0053f000000Wi00AAC` |
| Scott McKean | `005Vp0000016p45IAA` |
| Mathieu Pelletier | `0058Y00000CPn0bQAD` |
