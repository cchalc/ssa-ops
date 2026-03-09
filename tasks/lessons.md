# Lessons Learned

Patterns and rules to prevent repeated mistakes.

---

## Shell

- **Always use fish shell syntax** - no bash/zsh constructs
- Use `set VAR (command)` not `VAR=$(command)`
- Use `set -x VAR value` not `export VAR=value`

## Git/Jujutsu

- Use SSH remote (`git@github.com:`) not HTTPS for push access
- `jj split <file>` to separate changes into distinct commits
- `jj bookmark set main -r @-` then `jj git push` to push commits

## Databricks

- Profile managed via `DATABRICKS_CONFIG_PROFILE` env var
- direnv loads profile automatically in project directory

---

_Add new lessons as corrections occur._
