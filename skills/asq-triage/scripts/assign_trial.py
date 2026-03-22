#!/usr/bin/env python3
"""
Trial run: Assign unassigned ASQs to direct reports.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from query_asqs import query_asqs
from evaluate_charter import evaluate_charter

# Direct reports with current load
TEAM = {
    "Réda Khouani": {"current_load": 3, "specializations": ["ML", "Data Science", "GenAI", "Feature Store"]},
    "Volodymyr Vragov": {"current_load": 4, "specializations": ["Data Engineering", "Platform", "CI/CD", "Pipelines"]},
}

print("=" * 80)
print("TRIAL RUN: ASQ Assignment Recommendations")
print("=" * 80)

# Get ASQs
print("\nQuerying CAN ASQs...", file=sys.stderr)
asqs = query_asqs(region="CAN", limit=200)
asqs = evaluate_charter(asqs)

# Filter unassigned and ready to assign
unassigned = [a for a in asqs if not a.get("assigned_to")]

ready = []
for asq in unassigned:
    description = (asq.get("description") or "").lower()
    issues = []

    if not description or len(description) < 50:
        issues.append("MISSING_CONTEXT")
    if "context:" in description and "please add details" in description:
        issues.append("TEMPLATE_NOT_FILLED")

    tech_keywords = ["migration", "architecture", "performance", "integration", "poc", "pilot",
                     "deployment", "optimization", "troubleshoot", "design", "review", "workshop",
                     "ci/cd", "cicd", "ingestion", "pipeline", "model", "ml", "ai", "genai"]
    has_tech_ask = any(kw in description for kw in tech_keywords)
    if not has_tech_ask:
        issues.append("UNCLEAR_ASK")

    if not issues:
        ready.append(asq)

# Sort by charter score descending
ready.sort(key=lambda x: int(x.get("charter_score") or 0), reverse=True)

print(f"\n## Current Team Capacity\n")
for name, data in TEAM.items():
    print(f"  {name}: {data['current_load']} ASQs")
    print(f"    Specializations: {', '.join(data['specializations'])}")

print(f"\n## Assignment Plan ({len(ready)} ASQs)\n")

# Track assignments
assignments = {name: [] for name in TEAM}
projected_load = {name: data["current_load"] for name, data in TEAM.items()}

def match_specialization(asq, ssa_specs):
    """Check if ASQ matches SSA specializations."""
    desc = (asq.get("description") or "").lower()
    support_type = (asq.get("support_type") or "").lower()
    spec = (asq.get("technical_specialization") or "").lower()

    text = desc + " " + support_type + " " + spec

    for s in ssa_specs:
        if s.lower() in text:
            return True
    return False

for asq in ready:
    score = int(asq.get("charter_score") or 0)
    support_type = asq.get("support_type") or "General"
    spec = asq.get("technical_specialization") or "General"
    desc = (asq.get("description") or "").lower()

    # Determine best fit based on specialization
    reda_match = match_specialization(asq, TEAM["Réda Khouani"]["specializations"])
    vlad_match = match_specialization(asq, TEAM["Volodymyr Vragov"]["specializations"])

    # Assign based on:
    # 1. Specialization match
    # 2. Load balancing (prefer lower load)

    if reda_match and not vlad_match:
        assignee = "Réda Khouani"
        reason = "ML/Data Science specialization match"
    elif vlad_match and not reda_match:
        assignee = "Volodymyr Vragov"
        reason = "Data Engineering/Platform specialization match"
    elif projected_load["Réda Khouani"] <= projected_load["Volodymyr Vragov"]:
        assignee = "Réda Khouani"
        reason = "Load balancing (lower current load)"
    else:
        assignee = "Volodymyr Vragov"
        reason = "Load balancing"

    assignments[assignee].append(asq)
    projected_load[assignee] += 1

    # Determine key characteristics
    flags = []
    if asq.get("competitive"):
        flags.append("🏆 Competitive")
    if asq.get("near_win"):
        flags.append("🎯 Near Win")

    print(f"### [{asq['asq_number']}]({asq['sf_link']}) → **{assignee}**")
    print(f"**Account:** {asq['account_name']}")
    print(f"**Charter Score:** {score} | **Type:** {support_type}")
    if flags:
        print(f"**Flags:** {' '.join(flags)}")
    print(f"**Reason:** {reason}")

    # Brief description
    desc_short = (asq.get("description") or "")[:150]
    print(f"**Ask:** {desc_short}...")
    print()

# Summary
print("=" * 80)
print("SUMMARY")
print("=" * 80)

print(f"\n## Projected Workload After Assignment\n")
print(f"| SSA | Current | New | Projected |")
print(f"|-----|---------|-----|-----------|")
for name, data in TEAM.items():
    current = data["current_load"]
    new = len(assignments[name])
    projected = projected_load[name]
    print(f"| {name} | {current} | +{new} | **{projected}** |")

print(f"\n## Assignments by SSA\n")
for name, asqs in assignments.items():
    print(f"### {name} (+{len(asqs)} ASQs)\n")
    for asq in asqs:
        print(f"- [{asq['asq_number']}]({asq['sf_link']}) — {asq['account_name']} (Score: {asq.get('charter_score')})")
    print()
