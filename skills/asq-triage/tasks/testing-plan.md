# ASQ Triage Skill — Testing Plan

## Overview

This document provides a testing plan for the ASQ Triage skill and instructions for reloading context between sessions.

## Pre-Test Checklist

### 1. Databricks Authentication
```bash
# Check if logfood profile exists
grep -A3 "\[logfood\]" ~/.databrickscfg

# Refresh OAuth token (run this if you get "Unable to load OAuth Config")
databricks auth login --profile logfood --host https://adb-2548836972759138.18.azuredatabricks.net
```

### 2. Verify Skill Installation
```bash
# User-scope skill should exist
ls ~/.claude/skills/asq-triage/SKILL.md

# SSA-ops scripts should exist
ls ~/cowork/dev/ssa-ops/skills/asq-triage/scripts/asq_manager.py
```

### 3. Verify Dependencies
```bash
cd ~/cowork/dev/ssa-ops && uv sync
```

---

## Test Cases

### Test 1: Basic Hygiene Check (CAN Region)

**Command:**
```bash
cd ~/cowork/dev/ssa-ops && uv run skills/asq-triage/scripts/asq_manager.py --region CAN --hygiene --limit 10
```

**Expected Output:**
- Banner: `ASQ Triage — CAN`
- Hygiene Summary with counts
- Markdown report with Executive Summary table
- List of ASQs with SF links

**Pass Criteria:**
- [ ] No OAuth errors
- [ ] Returns ASQ data (not empty)
- [ ] Hygiene status populated (RULE1-5 or COMPLIANT)
- [ ] SF links are valid URLs

---

### Test 2: Charter Evaluation

**Command:**
```bash
cd ~/cowork/dev/ssa-ops && uv run skills/asq-triage/scripts/asq_manager.py --region CAN --hygiene --charter-eval --limit 20
```

**Expected Output:**
- Charter Summary with alignment counts
- Competitive and Near Win counts
- Charter Alignment Issues section in report

**Pass Criteria:**
- [ ] charter_alignment field populated (HIGHLY_ALIGNED, ALIGNED, NEUTRAL, MISALIGNED)
- [ ] competitive flag set for accounts with competitor UCOs
- [ ] near_win flag set for U4/U5 stage UCOs

---

### Test 3: Manager Hierarchy Filter

**Command:**
```bash
cd ~/cowork/dev/ssa-ops && uv run skills/asq-triage/scripts/asq_manager.py --manager-id 0053f000000pKoTAAU --hygiene --limit 10
```

**Expected Output:**
- Only ASQs from direct reports of specified manager
- Banner shows "Manager 0053f000..."

**Pass Criteria:**
- [ ] Returns subset of ASQs (not all CAN)
- [ ] All ASQs have owner in manager's hierarchy

---

### Test 4: JSON Export

**Command:**
```bash
cd ~/cowork/dev/ssa-ops && uv run skills/asq-triage/scripts/asq_manager.py --region CAN --json --limit 5 > /tmp/test-asqs.json && cat /tmp/test-asqs.json | jq '.[0] | keys'
```

**Expected Output:**
- Valid JSON array
- Each ASQ has expected fields

**Pass Criteria:**
- [ ] Valid JSON (jq parses without error)
- [ ] Contains: asq_number, account_name, hygiene_status, days_open, sf_link

---

### Test 5: Save Report to File

**Command:**
```bash
cd ~/cowork/dev/ssa-ops && uv run skills/asq-triage/scripts/asq_manager.py --region CAN --hygiene --output /tmp/test-report.md --limit 10 && head -30 /tmp/test-report.md
```

**Expected Output:**
- File created at /tmp/test-report.md
- Contains markdown headers and tables

**Pass Criteria:**
- [ ] File exists
- [ ] Contains "# ASQ Triage Report"
- [ ] Contains "## Executive Summary"

---

### Test 6: Skill Invocation via Claude

**In a new Claude session, say:**
> "run asq triage for CAN region with limit 5"

**Expected:**
- Claude reads the skill from `~/.claude/skills/asq-triage/SKILL.md`
- Executes the command
- Returns formatted results

**Pass Criteria:**
- [ ] Skill is discovered and invoked
- [ ] Command executes successfully
- [ ] Results displayed in session

---

## Reload Context Between Sessions

### Quick Reload Command

When starting a new Claude session, say:

> "Read the ASQ Triage context file at `~/cowork/dev/ssa-ops/skills/asq-triage/tasks/ssa-ops-context.md`"

Or:

> "I'm continuing work on the ASQ Triage skill. Read `skills/asq-triage/tasks/ssa-ops-context.md` for context."

### Context Files

| File | Purpose |
|------|---------|
| `skills/asq-triage/tasks/ssa-ops-context.md` | Full context summary |
| `skills/asq-triage/tasks/automation-plan.md` | Automation phases |
| `skills/asq-triage/tasks/testing-plan.md` | This file |
| `skills/asq-triage/SKILL.md` | Skill definition |

### Key Information to Provide

When resuming work, include:
1. **Working directory:** `~/cowork/dev/ssa-ops`
2. **Skill location:** `skills/asq-triage/`
3. **User-scope skill:** `~/.claude/skills/asq-triage/`
4. **Last known state:** (e.g., "Phase 1 complete, working on Slack integration")

---

## Troubleshooting

### OAuth Error
```
< 400 Bad Request
< Unable to load OAuth Config
```

**Fix:**
```bash
databricks auth login --profile logfood --host https://adb-2548836972759138.18.azuredatabricks.net
```

### No ASQs Returned

**Check:**
1. Region spelling (use CAN, not Canada)
2. Snapshot date is current
3. Status filter includes your ASQ statuses

**Debug:**
```bash
cd ~/cowork/dev/ssa-ops && uv run skills/asq-triage/scripts/query_asqs.py --region CAN --output json --limit 5
```

### Skill Not Found

**Check:**
```bash
ls ~/.claude/skills/asq-triage/SKILL.md
```

**Reinstall if needed:**
```bash
mkdir -p ~/.claude/skills/asq-triage
cp ~/cowork/dev/ssa-ops/skills/asq-triage/SKILL.md ~/.claude/skills/asq-triage/SKILL.md
```

---

## Test Log

| Date | Test | Result | Notes |
|------|------|--------|-------|
| 2026-03-22 | Test 1 | PASS | 100 CAN ASQs returned, all RULE5_EXCESSIVE |
| 2026-03-22 | Test 2 | PASS | 82 HIGHLY_ALIGNED, 68 competitive, 57 near-win |
| | | | |

---

*Created: 2026-03-22*
