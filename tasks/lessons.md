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

---

_Add new lessons as corrections occur._
