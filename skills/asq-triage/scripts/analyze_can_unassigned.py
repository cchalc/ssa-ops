#!/usr/bin/env python3
"""
Analyze unassigned CAN ASQs with focus on Chris Chalcraft's known team.

Known direct reports (from asq-pulse skill):
- Réda Khouani
- Volodymyr Vragov
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from query_asqs import query_asqs, WAREHOUSE_ID
from evaluate_charter import evaluate_charter
from databricks.sdk import WorkspaceClient

# Known direct reports
DIRECT_REPORTS = [
    "Réda Khouani",
    "Volodymyr Vragov",
]

w = WorkspaceClient(profile="logfood")

print("=" * 80)
print("CAN UNASSIGNED ASQ ANALYSIS — Chris Chalcraft's Team")
print("=" * 80)

# Get all CAN ASQs (last 30 days)
print("\nQuerying CAN ASQs (last 30 days)...", file=sys.stderr)
asqs = query_asqs(region="CAN", limit=200)
print(f"Found {len(asqs)} total ASQs", file=sys.stderr)

# Evaluate charter
asqs = evaluate_charter(asqs)

# Filter unassigned
unassigned = [a for a in asqs if not a.get("assigned_to")]
print(f"Found {len(unassigned)} unassigned ASQs", file=sys.stderr)

# Get capacity for direct reports
print("\n## Direct Reports Capacity\n")

query = """
SELECT
    owner_user_name AS ssa_name,
    COUNT(*) AS total_asqs,
    SUM(CASE WHEN created_date >= CURRENT_DATE - INTERVAL 30 DAYS THEN 1 ELSE 0 END) AS new_asqs,
    SUM(CASE WHEN target_end_date < CURRENT_DATE THEN 1 ELSE 0 END) AS overdue
FROM main.gtm_silver.approval_request_detail
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
    AND status IN ('New', 'Submitted', 'Under Review', 'In Progress')
    AND region_level_1 = 'CAN'
    AND owner_user_name IS NOT NULL
GROUP BY owner_user_name
ORDER BY total_asqs DESC
"""

result = w.statement_execution.execute_statement(
    warehouse_id=WAREHOUSE_ID,
    statement=query,
    wait_timeout="50s",
)

capacity = {}
if result.status.state.value == "SUCCEEDED" and result.result:
    columns = [col.name for col in result.manifest.schema.columns]
    for row in result.result.data_array:
        rec = dict(zip(columns, row))
        capacity[rec["ssa_name"]] = {
            "total": int(rec["total_asqs"] or 0),
            "new": int(rec["new_asqs"] or 0),
            "overdue": int(rec["overdue"] or 0),
        }

# Show direct reports capacity
for dr in DIRECT_REPORTS:
    if dr in capacity:
        c = capacity[dr]
        status = "🟢" if c["total"] < 3 else "🟡" if c["total"] < 6 else "🔴"
        print(f"  {status} {dr}: {c['total']} total ASQs ({c['new']} new, {c['overdue']} overdue)")
    else:
        print(f"  ⚪ {dr}: No ASQs assigned")

# Analyze unassigned ASQs
print(f"\n## Unassigned ASQs ({len(unassigned)})\n")

if not unassigned:
    print("No unassigned ASQs found.")
    sys.exit(0)

# Categorize
assign_ready = []
needs_clarification = []
return_to_ae = []

for asq in unassigned:
    issues = []
    description = (asq.get("description") or "").lower()

    # Check description quality
    if not description or len(description) < 50:
        issues.append("MISSING_CONTEXT")

    if "context:" in description and "please add details" in description:
        issues.append("TEMPLATE_NOT_FILLED")

    # Check for clear technical ask
    tech_keywords = ["migration", "architecture", "performance", "integration", "poc", "pilot",
                     "deployment", "optimization", "troubleshoot", "design", "review", "workshop"]
    has_tech_ask = any(kw in description for kw in tech_keywords)
    if not has_tech_ask:
        issues.append("UNCLEAR_ASK")

    # Categorize
    if not issues:
        assign_ready.append(asq)
    elif "TEMPLATE_NOT_FILLED" in issues:
        return_to_ae.append((asq, issues))
    else:
        needs_clarification.append((asq, issues))

# Print ASSIGN READY
if assign_ready:
    print(f"### ✅ READY TO ASSIGN ({len(assign_ready)})\n")
    for asq in assign_ready:
        score = int(asq.get("charter_score") or 0)
        flags = []
        if asq.get("competitive"):
            flags.append("🏆 Competitive")
        if asq.get("near_win"):
            flags.append("🎯 Near Win")

        print(f"**[{asq['asq_number']}]({asq['sf_link']})** — {asq['account_name']}")
        print(f"  Type: {asq.get('support_type')} | Spec: {asq.get('technical_specialization') or 'General'}")
        print(f"  Charter Score: {score} ({asq.get('charter_alignment')}) {' '.join(flags)}")
        print(f"  Days Open: {asq.get('days_open')}")
        desc = (asq.get("description") or "")[:200]
        print(f"  Description: {desc}...")
        print()

# Print RETURN TO AE
if return_to_ae:
    print(f"\n### ⛔ RETURN TO AE ({len(return_to_ae)})\n")
    for asq, issues in return_to_ae:
        print(f"**[{asq['asq_number']}]({asq['sf_link']})** — {asq['account_name']}")
        print(f"  Issues: {', '.join(issues)}")
        print(f"  Action: Template not properly filled out, return to AE for completion")
        print()

# Print NEEDS CLARIFICATION
if needs_clarification:
    print(f"\n### ⚠️ NEEDS CLARIFICATION ({len(needs_clarification)})\n")
    for asq, issues in needs_clarification:
        score = int(asq.get("charter_score") or 0)
        print(f"**[{asq['asq_number']}]({asq['sf_link']})** — {asq['account_name']}")
        print(f"  Charter Score: {score} | Type: {asq.get('support_type')}")
        print(f"  Issues: {', '.join(issues)}")
        desc = (asq.get("description") or "")[:150]
        print(f"  Description: {desc}...")
        print()

# Summary
print("\n## Summary\n")
print(f"| Category | Count |")
print(f"|----------|-------|")
print(f"| Ready to Assign | {len(assign_ready)} |")
print(f"| Needs Clarification | {len(needs_clarification)} |")
print(f"| Return to AE | {len(return_to_ae)} |")
print(f"| **Total Unassigned** | **{len(unassigned)}** |")

print("\n## Recommendation\n")
if capacity.get("Réda Khouani", {}).get("total", 0) < capacity.get("Volodymyr Vragov", {}).get("total", 0):
    print(f"Réda Khouani has lower workload - consider assigning new ASQs to Réda first.")
else:
    print(f"Volodymyr Vragov has lower workload - consider assigning new ASQs to Volodymyr first.")
