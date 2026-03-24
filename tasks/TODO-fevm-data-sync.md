# TODO: fevm-cjc Data Sync Plan

**Created:** 2025-03-23
**Status:** Blocked (fevm-cjc warehouse permission issue)

## Context

Charter metric views (4-8) are deployed on logfood and working. User wants to replicate data to fevm-cjc for Lakebase integration.

## Blocker

fevm-cjc serverless warehouse fails to start:
```
PERMISSION_DENIED: Principal 4208779108861901 is not part of org: 7474645166465249
```

**Action needed:** Contact fevm-cjc workspace admin to fix serverless permissions.

## Proposed Plan (once fevm-cjc is fixed)

### Option A: fevm-cjc reads from logfood (preferred)
1. Verify fevm-cjc can read `main.gtm_silver.*` and `main.gtm_gold.*` from logfood
2. Create scheduled job on fevm-cjc (twice daily)
3. Query GTM tables with 1-year filter
4. Write to managed tables: `cjc_aws_workspace_catalog.ssa_ops_dev.*`
5. Create metric views on fevm-cjc referencing local tables

### Option B: logfood-only (fallback)
1. Create snapshot tables in `home_christopher_chalcraft.ssa_ops`
2. Scheduled job on logfood copies GTM data to your tables
3. Update metric views to reference snapshot tables
4. Build dashboard on logfood

### Tables to Sync (1 year retention)
- `approval_request_detail` (~112K rows/snapshot)
- `use_case_detail` (~231K rows/snapshot)
- `individual_hierarchy_salesforce` (small)
- `account_obt` (filtered to current FY quarter)
- `core_usecase_curated` (for ARR data)

### Schedule
- Twice daily (e.g., 6am and 6pm PST)
- MERGE or full refresh depending on table size

## When to Revisit
- After fevm-cjc permission issue is resolved
- Or if logfood-only approach is sufficient
