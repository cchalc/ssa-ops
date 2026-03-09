# Project Conventions

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

## Commit Workflow

1. Make changes
2. `jj status` to review
3. `jj describe -m "message"` to describe
4. `jj new` to create fresh working copy
5. `jj bookmark set main -r @-` to update main
6. `jj git push` to push
