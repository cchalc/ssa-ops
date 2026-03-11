# Project Conventions

## Infrastructure & Deployment

**Databricks Asset Bundles (DABs)** with **Lakebase Autoscaling** for database-per-branch development.

### Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Databricks    │────▶│    Lakebase     │────▶│   Electric SQL  │────▶│  TanStack DB    │
│   SQL Warehouse │     │  (Autoscaling)  │     │   (sync layer)  │     │  (local state)  │
└─────────────────┘     └─────────────────┘     └─────────────────┘     └─────────────────┘
```

### Lakebase Branching Workflow

```
main  ──push──▶  deploy bundle  ──▶  configure project  ──▶  run migrations
                      │
feature/*  ──create──▶  fork Lakebase branch from production (7-day TTL)
           ──delete──▶  delete Lakebase branch (auto-cleanup)
```

**Key Features:**
- **Copy-on-write storage**: Branch creation is instant regardless of database size
- **Environment isolation**: Each feature branch gets its own database fork
- **Auto-cleanup**: 7-day TTL on feature branches, automatic deletion on Git branch delete
- **Scale-to-zero**: Feature branch compute suspends when idle

### Workspaces

| Profile | Workspace | Purpose |
|---------|-----------|---------|
| `fevm-cjc` | fevm-cjc-aws-workspace.cloud.databricks.com | Infrastructure (Lakebase, Apps) |
| `logfood` | adb-2548836972759138 | Data queries (SQL Warehouse) |

### Deploy Commands

```fish
# Validate bundle configuration
databricks bundle validate -t dev

# Deploy Lakebase project + read-replica
databricks bundle deploy -t dev

# Run post-deploy configuration (permissions, migrations)
./scripts/post_deploy.sh
```

## Lakebase Autoscaling

### Project Structure

| Environment | Project Name | Database | Compute |
|-------------|--------------|----------|---------|
| dev | `ssa-ops-dev` | `ssa_ops_dev` | 0.5-2 CU |
| staging | `ssa-ops-staging` | `ssa_ops_staging` | 0.5-4 CU |
| prod | `ssa-ops` | `ssa_ops` | 0.5-8 CU |

### Connect via CLI

```fish
# Interactive session (production branch)
databricks psql ssa-ops-dev --profile fevm-cjc -- -d ssa_ops_dev

# Connect to feature branch
databricks psql ssa-ops-dev --branch feature-xyz --profile fevm-cjc -- -d ssa_ops_dev

# Run single query
databricks psql ssa-ops-dev --profile fevm-cjc -- -d ssa_ops_dev -c "SELECT * FROM app.test_items;"
```

### Manual Branch Management

```fish
# Create a branch (normally done by CI)
databricks postgres create-branch "projects/ssa-ops-dev" "my-branch" \
  --parent-branch-id production \
  --ttl-duration "604800s"

# Delete a branch
databricks postgres delete-branch "projects/ssa-ops-dev/branches/my-branch"

# List branches
databricks postgres list-branches "projects/ssa-ops-dev"
```

### Tables

| Schema | Table | Description |
|--------|-------|-------------|
| `app` | `test_items` | Sample items for local-first sync demo |

### Database Structure

```
ssa_ops_dev/
├── databricks_postgres    # Default database
└── ssa_ops_dev/          # Application database
    └── app/              # Application schema
        └── test_items    # Sample table
```

## GitHub Actions CI/CD

### Workflows

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| `deploy-lakebase.yml` | Push to main | Deploy bundle + run migrations |
| `create-lakebase-branch.yml` | Branch created | Fork Lakebase from production |
| `delete-lakebase-branch.yml` | Branch deleted | Cleanup Lakebase branch |
| `deploy.yml` | Push/PR to main | Build, lint, test app |

### Required Secrets/Variables

**Secrets:**
- `DATABRICKS_HOST` - Workspace URL
- `DATABRICKS_TOKEN` - PAT or OAuth token

**Variables:**
- `LAKEBASE_PROJECT_NAME` - Project name (e.g., `ssa-ops-dev`)

### Setup

1. Create workspace group `ssa-ops-developers`
2. Add team members to the group
3. Configure GitHub secrets/variables
4. Push to main to trigger initial deployment

## Electric SQL (Local-First Sync)

Electric SQL syncs Lakebase tables to the browser for local-first data.

**Setup:** See `docs/electric-setup.md` for full instructions.

**Quick Start:**
```fish
# In electric repo (sibling directory)
cd ~/cowork/dev/electric
devenv shell
cd packages/sync-service
mix run --no-halt

# In ssa-ops
pnpm dev
```

Navigate to http://localhost:5173/data to see synced items.

**Architecture:**
```
Lakebase (Postgres) → Electric SQL (Elixir) → Browser (TanStack DB)
                      localhost:3000
```

**Note:** Electric SQL requires REPLICATION privilege for logical replication. This may require Databricks support to enable on Lakebase.

**Files:**
- `docs/electric-setup.md` - Setup instructions
- `docs/adr/001-electric-sql-from-source.md` - Decision record
- `docker-compose.yml` - Docker fallback option
- `src/db/collections/testItems.ts` - Electric collection definition
- `src/routes/data.tsx` - Data explorer page

## Project Structure

```
ssa-ops/
├── .github/workflows/
│   ├── deploy-lakebase.yml        # Lakebase deployment + config
│   ├── create-lakebase-branch.yml # Auto-create dev branches
│   ├── delete-lakebase-branch.yml # Auto-cleanup dev branches
│   └── deploy.yml                 # App build/test
├── infra/resources/
│   ├── lakebase.yml               # Lakebase Autoscaling project
│   ├── jobs.yml                   # Data setup jobs
│   └── app.yml                    # App deployment (enable after build)
├── scripts/
│   ├── post_deploy.sh             # Post-deploy configuration
│   └── migrations.sql             # Idempotent schema migrations
├── src/
│   ├── db/collections/            # TanStack DB collections
│   ├── routes/                    # TanStack Router pages
│   └── components/                # React components
├── databricks.yml                 # Main bundle config
└── docs/                          # Documentation
```

## Python Environment

**uv** for Python dependency management:

```fish
uv sync                      # Install dependencies
uv run pytest               # Run tests
uv add <package>            # Add dependency
```

Virtual env location: `~/.virtualenvs/ssa-ops` (via UV_PROJECT_ENVIRONMENT)

## Version Control

**Jujutsu only** - Do not use git commands directly.

```fish
# Common commands
jj status                    # Check working copy
jj log                       # View history
jj describe -m "message"     # Add commit message
jj new                       # Create new working copy
jj bookmark set main -r @-   # Move main to previous commit
jj git push                  # Push to GitHub

# Split changes into separate commits
jj split <file>              # Extract file into its own commit

# Rebase/move commits
jj rebase -d <destination>
```

## Shell

**Fish shell only** - See CLAUDE.md for syntax rules.

## Environment

**direnv** manages environment variables:
- `.envrc` - Loads profile and `.env` file
- `.env` - Local secrets (gitignored)
- `.env.example` - Template for required vars

Databricks profiles: `fevm-cjc` (deploy), `logfood` (data)

## Package Manager

**pnpm** - Do not use npm or yarn.

```fish
pnpm install          # Install deps
pnpm add <pkg>        # Add dependency
pnpm dev              # Start dev server
pnpm build            # Build for production
```

## Code Style

**Biome** for formatting and linting:

```fish
pnpm format           # Format code
pnpm lint             # Lint code
pnpm check            # Full check
```

## Documentation

| File | Purpose |
|------|---------|
| `PLAN.md` | Project plan, architecture, progress |
| `PROJECT.md` | Dev conventions (this file) |
| `CLAUDE.md` | AI assistant instructions |
| `tasks/todo.md` | Task checklist |
| `tasks/lessons.md` | Lessons learned |
| `docs/electric-setup.md` | Electric SQL setup |
| `docs/adr/*.md` | Architecture Decision Records |

## Databricks Skills & Documentation

**Local skills** at `~/.claude/skills/`:
- `databricks-asset-bundles` - DAB configuration
- `databricks-lakebase-autoscale` - Lakebase Autoscaling setup
- `databricks-app-python` - Python apps (Dash, Streamlit)
- `databricks-unity-catalog` - System tables, volumes

Invoke with `/skill-name` or reference in prompts.

## Commit Workflow

1. Make changes
2. `jj status` to review
3. `jj describe -m "message"` to describe
4. `jj new` to create fresh working copy
5. `jj bookmark set main -r @-` to update main
6. `jj git push` to push

## Lakebase Autoscaling vs Provisioned

| Feature | Autoscaling | Provisioned |
|---------|-------------|-------------|
| Database branching | ✅ Copy-on-write | ❌ Manual |
| Read replicas | ✅ Instant | ✅ Instant |
| Scale-to-zero | ✅ Yes | ❌ No |
| Point-in-time recovery | ✅ 0-30 days | ✅ PITR |
| Compute scaling | ✅ Auto | ❌ Manual |
| Environment isolation | ✅ Branch per env | ❌ Instance per env |
