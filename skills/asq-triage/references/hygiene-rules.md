# ASQ Hygiene Rules — 5-Rule Framework

## Overview

The hygiene framework identifies ASQs requiring manager attention. Rules are evaluated in order of severity.

## Rule Definitions

### RULE1: Missing Status Notes
**Trigger:** ASQ assigned >7 days with NULL or empty `request_status_notes`

**Severity:** HIGH

**Action Required:** Add an initial status update

**SQL Logic:**
```sql
WHEN DATEDIFF(CURRENT_DATE, created_date) > 7
  AND (request_status_notes IS NULL OR LENGTH(request_status_notes) < 10)
THEN 'RULE1_MISSING_NOTES'
```

---

### RULE3: Stale ASQ (30-90 days)
**Trigger:** ASQ open 30-90 days AND (due date is NULL OR due date has passed)

**Severity:** HIGH

**Action Required:** Close the ASQ or extend end date with justification

**SQL Logic:**
```sql
WHEN DATEDIFF(CURRENT_DATE, created_date) BETWEEN 30 AND 90
  AND (target_end_date IS NULL OR target_end_date < CURRENT_DATE)
THEN 'RULE3_STALE'
```

---

### RULE4: Expired End Date
**Trigger:** Due date expired >7 days ago (and Rules 3/5 not triggered)

**Severity:** CRITICAL

**Action Required:** Update end date or close the ASQ

**SQL Logic:**
```sql
WHEN target_end_date < CURRENT_DATE - INTERVAL 7 DAYS
THEN 'RULE4_EXPIRED'
```

---

### RULE5: Excessively Stale (>90 days)
**Trigger:** ASQ open >90 days — replaces Rule 3

**Severity:** CRITICAL

**Action Required:** Escalate to manager. Options:
1. Close with summary
2. Extend with strong business justification
3. Discuss disposition in 1:1

**SQL Logic:**
```sql
WHEN DATEDIFF(CURRENT_DATE, created_date) > 90
THEN 'RULE5_EXCESSIVE'
```

---

### COMPLIANT
**Trigger:** No violations detected

**Status:** OK — no action needed

---

## Priority Order

Rules are evaluated in this order (first match wins):
1. RULE5_EXCESSIVE (>90 days)
2. RULE3_STALE (30-90 days + past due)
3. RULE4_EXPIRED (due >7 days ago)
4. RULE1_MISSING_NOTES (>7 days + no notes)
5. COMPLIANT

---

## Urgency Classification

Based on days overdue:

| Urgency | Condition |
|---------|-----------|
| CRITICAL | >14 days overdue |
| HIGH | 7-14 days overdue |
| MEDIUM | 1-7 days overdue OR stale notes |
| NORMAL | On track |

---

## Expected SLAs

| Milestone | Target |
|-----------|--------|
| New → Under Review | < 2 business days |
| Under Review → In Progress | < 3 business days |
| In Progress → First Note | < 5 business days |
| Complete by Due Date | 100% |

---

## Weekly Hygiene Cadence

1. **Monday:** Manager runs `/asq-manager --hygiene --slack`
2. **Tuesday-Thursday:** SSAs address flagged items
3. **Friday:** Manager reviews compliance rate
