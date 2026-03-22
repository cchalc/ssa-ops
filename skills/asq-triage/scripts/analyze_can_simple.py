#!/usr/bin/env python3
"""
Simple analysis of unassigned CAN ASQs.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from query_asqs import query_asqs
from evaluate_charter import evaluate_charter

# Known direct reports
DIRECT_REPORTS = ["Réda Khouani", "Volodymyr Vragov"]

print("=" * 80)
print("CAN UNASSIGNED ASQ ANALYSIS")
print("=" * 80)

# Get all CAN ASQs (last 30 days)
print("\nQuerying CAN ASQs...", file=sys.stderr)
asqs = query_asqs(region="CAN", limit=200)
print(f"Found {len(asqs)} total ASQs", file=sys.stderr)

# Evaluate charter
print("Evaluating charter alignment...", file=sys.stderr)
asqs = evaluate_charter(asqs)

# Calculate capacity from the data we have
print("\n## Team Capacity (from current ASQ assignments)\n")
capacity = {}
for asq in asqs:
    owner = asq.get("assigned_to")
    if owner:
        if owner not in capacity:
            capacity[owner] = {"total": 0, "overdue": 0}
        capacity[owner]["total"] += 1
        if int(asq.get("days_overdue") or 0) > 0:
            capacity[owner]["overdue"] += 1

# Show direct reports
for dr in DIRECT_REPORTS:
    if dr in capacity:
        c = capacity[dr]
        status = "🟢" if c["total"] < 3 else "🟡" if c["total"] < 6 else "🔴"
        print(f"  {status} {dr}: {c['total']} ASQs ({c['overdue']} overdue)")
    else:
        print(f"  ⚪ {dr}: 0 ASQs in last 30 days")

# Filter unassigned
unassigned = [a for a in asqs if not a.get("assigned_to")]
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
                     "deployment", "optimization", "troubleshoot", "design", "review", "workshop",
                     "ci/cd", "cicd", "ingestion", "pipeline", "model", "ml", "ai", "genai"]
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
        desc = (asq.get("description") or "")[:250]
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
        flags = []
        if asq.get("competitive"):
            flags.append("🏆")
        if asq.get("near_win"):
            flags.append("🎯")
        print(f"**[{asq['asq_number']}]({asq['sf_link']})** — {asq['account_name']} {' '.join(flags)}")
        print(f"  Charter Score: {score} | Type: {asq.get('support_type')}")
        print(f"  Issues: {', '.join(issues)}")
        desc = (asq.get("description") or "")[:200]
        print(f"  Description: {desc}...")
        print()

# Summary
print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"\n| Category | Count |")
print(f"|----------|-------|")
print(f"| Ready to Assign | {len(assign_ready)} |")
print(f"| Needs Clarification | {len(needs_clarification)} |")
print(f"| Return to AE | {len(return_to_ae)} |")
print(f"| **Total Unassigned** | **{len(unassigned)}** |")

# Recommendation
print("\n## Assignment Recommendation\n")
reda_load = capacity.get("Réda Khouani", {}).get("total", 0)
vlad_load = capacity.get("Volodymyr Vragov", {}).get("total", 0)

if reda_load <= vlad_load:
    print(f"**Réda Khouani** has lower workload ({reda_load} ASQs) — assign to Réda first")
else:
    print(f"**Volodymyr Vragov** has lower workload ({vlad_load} ASQs) — assign to Volodymyr first")
