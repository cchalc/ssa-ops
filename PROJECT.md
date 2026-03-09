# Project Conventions

## Infrastructure & Deployment

**Databricks Asset Bundles (DABs)** with **Go SDK** for all infrastructure.

Reference example: [bundle-examples/knowledge_base/app_with_database](https://github.com/databricks/bundle-examples/tree/main/knowledge_base/app_with_database)

```yaml
# databricks.yml structure
bundle:
  name: ssa-ops

resources:
  apps:
    ssa_ops_app:
      # App definition
  databases:
    ssa_ops_db:
      # Lakebase instance
  catalogs:
    ssa_ops_catalog:
      # Unity Catalog registration
```

**Deploy commands:**
```fish
cd infra
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run ssa_ops_app -t dev
```

**Infrastructure structure:**
```
infra/
├── databricks.yml           # Main bundle config
└── resources/
    ├── database.yml         # Lakebase instance + catalog
    └── app.yml              # App deployment
```

**Requirements:**
- Databricks CLI v0.267.0+
- Go SDK for custom extensions

**CI/CD:**
- GitHub Actions workflow at `.github/workflows/deploy.yml`
- Auto-deploys on push to main
- Manual dispatch for prod deployments

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

Databricks profile: `logfood`

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

## Databricks Skills & Documentation

**Context7 MCP** - Add `use context7` to prompts for live documentation.

**Local skills** at `~/.claude/skills/`:
- `databricks-asset-bundles` - DAB configuration
- `databricks-lakebase-provisioned` - Lakebase setup
- `databricks-app-python` - Python apps (Dash, Streamlit)
- `databricks-unity-catalog` - System tables, volumes
- `databricks-jobs` - Workflows
- `databricks-dbsql` - SQL warehouse

Invoke with `/skill-name` or reference in prompts.

## Commit Workflow

1. Make changes
2. `jj status` to review
3. `jj describe -m "message"` to describe
4. `jj new` to create fresh working copy
5. `jj bookmark set main -r @-` to update main
6. `jj git push` to push
