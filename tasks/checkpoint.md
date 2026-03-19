# Session Checkpoint - 2026-03-19

## Context
SSA Activity Dashboard with three-tier data sync architecture:
- **Logfood** (Azure) → SQL Views → **fevm-cjc** (AWS) Delta Tables → **Lakebase** PostgreSQL → **ssa-ops app**

## Completed This Session

### 1. PR #1 Merged
- Data sync infrastructure
- Architecture docs with Mermaid diagrams
- Validation test suite
- Commit: `fa7d79f4`

### 2. Fixed Sync Job Config
- `spark_python_task` (was `python_task`)
- Added `SingleNode` custom_tags for cluster
- Commit: `8ca7dc76`

### 3. Created Justfile for Local Deploys
Both workspaces have IP ACL restrictions blocking GitHub Actions.

**Available recipes:**
```
just deploy-dev         # Deploy to fevm-cjc (dev)
just deploy-staging     # Deploy to fevm-cjc (staging)
just deploy-prod        # Deploy to fevm-cjc (prod)
just deploy-views       # Instructions for logfood SQL views
just sync-to-delta      # Trigger export job
just sync-to-lakebase   # Trigger sync job
just sync-all           # Full pipeline
just validate           # Validate bundle
just validate-data      # Run all validation tiers
just jobs               # List deployed jobs
just lakebase-token     # Generate OAuth token
just lakebase-connect   # Connect via psql
just dev                # Start local dev server
```

### 4. GitHub Actions
- `deploy-pipeline.yml` - Validation only (active)
- `deploy-automated.yml.disabled` - Full CI/CD (ready to activate for other workspaces)

### 5. Environment Setup
Updated `.env` with both workspace tokens:
- `DATABRICKS_HOST` / `DATABRICKS_TOKEN` → fevm-cjc
- `LOGFOOD_HOST` / `LOGFOOD_TOKEN` → logfood

## Deployed Resources (fevm-cjc)

| Job | URL |
|-----|-----|
| Export to Delta | [jobs/295281267186432](https://fevm-cjc-aws-workspace.cloud.databricks.com/jobs/295281267186432) |
| Sync to Lakebase | [jobs/813793625205273](https://fevm-cjc-aws-workspace.cloud.databricks.com/jobs/813793625205273) |
| Validate Data | [jobs/162713252250130](https://fevm-cjc-aws-workspace.cloud.databricks.com/jobs/162713252250130) |
| Deploy Views | [jobs/1117467850827411](https://fevm-cjc-aws-workspace.cloud.databricks.com/jobs/1117467850827411) |

## Next Steps

1. **Run initial data sync**
   ```fish
   just sync-all
   ```

2. **Deploy SQL views to logfood** (manual)
   ```fish
   just deploy-views  # Shows instructions
   ```

3. **Unpause scheduled jobs** in Databricks (currently PAUSED)

4. **Build the app UI** - Dashboard routes for metrics

## File Structure

```
ssa-ops/
├── justfile                              # Local deployment orchestration
├── .github/workflows/
│   ├── deploy-pipeline.yml               # Validation only (active)
│   └── deploy-automated.yml.disabled     # Full CI/CD (ready to activate)
├── infra/resources/
│   └── sync_jobs.yml                     # Job definitions
├── scripts/
│   └── run-validation.sh                 # Validation runner
├── sql/
│   ├── sync/                             # Sync SQL
│   └── tests/                            # Validation SQL
└── src/jobs/
    └── sync_to_lakebase.py               # Python sync job
```

## Git Log (Recent)

```
28e14fc5 Fix validation scripts for CLI compatibility
0a3a8819 Add disabled automated deployment workflow
356b0413 Add justfile for local deployment orchestration
8ca7dc76 Fix sync job config and consolidate CI/CD workflow
e69b45c7 Merge pull request #1
fa7d79f4 Add SSA Dashboard data sync infrastructure and test suite
```
