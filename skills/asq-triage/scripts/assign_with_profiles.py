#!/usr/bin/env python3
"""
Profile-based ASQ assignment using historical data.
Uses SSA profiles built from 2 years of completed ASQs.
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from query_asqs import query_asqs
from evaluate_charter import evaluate_charter

# Load SSA profiles
PROFILES_PATH = Path(__file__).parent.parent / "data" / "ssa_profiles.json"
with open(PROFILES_PATH) as f:
    SSA_PROFILES = json.load(f)

print("=" * 80)
print("PROFILE-BASED ASQ ASSIGNMENT")
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

print(f"\n## SSA Profiles Summary\n")
for name, profile in SSA_PROFILES.items():
    print(f"### {name}")
    print(f"  - Historical ASQs: {profile['total_asqs']}")
    print(f"  - Current In-Progress: {profile['in_progress']}")
    print(f"  - Avg Duration: {profile['avg_duration_days']} days")
    top_tech = sorted(profile["technologies"].items(), key=lambda x: -x[1])[:3]
    tech_str = ", ".join(f"{t[0]} ({t[1]})" for t in top_tech)
    print(f"  - Top Technologies: {tech_str}")
    print()


def calculate_match_score(asq: dict, profile: dict, current_load: int) -> tuple[float, list[str]]:
    """Calculate how well an ASQ matches an SSA's profile."""
    score = 0.0
    reasons = []

    account = asq.get("account_name") or ""
    description = (asq.get("description") or "").lower()
    title = (asq.get("asq_title") or "").lower()
    support_type = asq.get("support_type") or ""
    spec = asq.get("technical_specialization") or ""
    text = f"{description} {title}"

    # 1. Account relationship continuity (reduced weight - workload balance is priority)
    if account in profile["accounts"]:
        prior_asqs = profile["accounts"][account]
        score += 10 + min(prior_asqs * 2, 10)  # Base 10 + 2 per prior ASQ, max +20
        reasons.append(f"ACCOUNT_CONTINUITY: {prior_asqs} prior ASQs with {account}")
    elif account in [a[0] for a in profile["top_accounts"]]:
        score += 8
        reasons.append(f"TOP_ACCOUNT: {account}")

    # 2. Technology match (weighted by historical frequency)
    tech_keywords = {
        "ML/AI": ["machine learning", "ml ", " ai ", "model", "mlflow", "feature store", "training", "inference"],
        "GenAI": ["genai", "llm", "gpt", "langchain", "rag", "vector", "embedding", "agent", "chatbot"],
        "Data Engineering": ["pipeline", "etl", "ingestion", "delta", "spark", "streaming", "cdc", "lakeflow"],
        "SQL/Analytics": ["sql", "warehouse", "bi", "dashboard", "analytics", "reporting", "dbsql"],
        "Platform/Admin": ["workspace", "cluster", "unity catalog", "governance", "security", "admin", "cicd", "ci/cd"],
        "Migration": ["migration", "migrate", "synapse", "snowflake", "redshift", "fabric"],
        "SAP": ["sap", "s4hana", "hana"],
        "Geospatial": ["geospatial", "geo", "lidar", "gis", "spatial"],
    }

    detected_tech = []
    for tech, keywords in tech_keywords.items():
        if any(kw in text for kw in keywords):
            detected_tech.append(tech)
            if tech in profile["technologies"]:
                # Weight by historical frequency (percentage of ASQs)
                freq = profile["technologies"][tech] / profile["total_asqs"]
                tech_score = 20 * freq  # Up to 20 points for most frequent tech
                score += tech_score
                reasons.append(f"TECH_MATCH: {tech} ({int(freq*100)}% of history)")

    # 3. Support type match
    if support_type in profile["support_types"]:
        type_freq = profile["support_types"][support_type] / profile["total_asqs"]
        score += 10 * type_freq
        reasons.append(f"SUPPORT_TYPE_MATCH: {support_type}")

    # 4. Specialization match
    if spec in profile["specializations"]:
        spec_freq = profile["specializations"][spec] / profile["total_asqs"]
        score += 15 * spec_freq
        reasons.append(f"SPECIALIZATION_MATCH: {spec}")

    # 5. Current workload - PRIORITY: balance across team (target ~12, max 20)
    # current_load is passed in (includes assignments made this run)
    TARGET_LOAD = 12
    MAX_LOAD = 20

    if current_load >= MAX_LOAD:
        score -= 100  # Effectively block assignment
        reasons.append(f"AT_CAPACITY: {current_load} ASQs (max {MAX_LOAD})")
    elif current_load >= 16:
        score -= 40
        reasons.append(f"NEAR_CAPACITY: {current_load} ASQs")
    elif current_load >= TARGET_LOAD:
        score -= 25
        reasons.append(f"ABOVE_TARGET: {current_load} ASQs (target {TARGET_LOAD})")
    elif current_load >= 8:
        score -= 5
        reasons.append(f"MODERATE_LOAD: {current_load} ASQs")
    elif current_load >= 5:
        score += 10
        reasons.append(f"AVAILABLE: {current_load} ASQs")
    else:
        score += 20
        reasons.append(f"LOW_LOAD: {current_load} ASQs - prioritize")

    # 6. Duration consideration (faster SSA for simpler tasks)
    charter_score = int(asq.get("charter_score") or 0)
    avg_duration = profile["avg_duration_days"]
    if charter_score < 10 and avg_duration < 25:
        score += 5
        reasons.append("FAST_TURNAROUND")

    return score, reasons


print(f"\n## Assignment Recommendations ({len(ready)} ASQs)\n")

# Track assignments
assignments = {name: [] for name in SSA_PROFILES}
projected_load = {name: p["in_progress"] for name, p in SSA_PROFILES.items()}

for asq in ready:
    # Calculate match scores for each SSA (using projected load for balance)
    scores = {}
    all_reasons = {}
    for name, profile in SSA_PROFILES.items():
        score, reasons = calculate_match_score(asq, profile, projected_load[name])
        scores[name] = score
        all_reasons[name] = reasons

    # Select best match and second best
    sorted_scores = sorted(scores.items(), key=lambda x: -x[1])
    best_ssa = sorted_scores[0][0]
    best_score = sorted_scores[0][1]
    second_ssa = sorted_scores[1][0] if len(sorted_scores) > 1 else None
    second_score = sorted_scores[1][1] if len(sorted_scores) > 1 else 0
    margin = best_score - second_score

    # Confidence level
    if margin > 20:
        confidence = "HIGH"
    elif margin > 10:
        confidence = "MEDIUM"
    else:
        confidence = f"LOW (also consider {second_ssa})"

    assignments[best_ssa].append(asq)
    projected_load[best_ssa] += 1

    # Print recommendation
    print(f"### [{asq['asq_number']}]({asq['sf_link']}) → **{best_ssa}**")
    print(f"**Account:** {asq['account_name']}")
    print(f"**Charter Score:** {asq.get('charter_score')} | **Type:** {asq.get('support_type')}")
    print(f"**Match Score:** {best_score:.1f} (2nd: {second_ssa} {second_score:.1f}) — **{confidence}**")

    # Show match reasons
    print(f"**Why {best_ssa}:**")
    for reason in all_reasons[best_ssa][:4]:  # Top 4 reasons
        print(f"  - {reason}")

    # Show flags
    flags = []
    if asq.get("competitive"):
        flags.append("🏆 Competitive")
    if asq.get("near_win"):
        flags.append("🎯 Near Win")
    if flags:
        print(f"**Flags:** {' '.join(flags)}")

    # Brief description
    desc_short = (asq.get("description") or "")[:120]
    print(f"**Ask:** {desc_short}...")
    print()

# Summary
print("=" * 80)
print("SUMMARY")
print("=" * 80)

print(f"\n## Projected Workload After Assignment\n")
print(f"| SSA | Current | New | Projected |")
print(f"|-----|---------|-----|-----------|")
for name, profile in SSA_PROFILES.items():
    current = profile["in_progress"]
    new = len(assignments[name])
    projected = projected_load[name]
    balance = "✅" if abs(projected - list(projected_load.values())[0]) < 3 else "⚠️"
    print(f"| {name} | {current} | +{new} | **{projected}** {balance} |")

print(f"\n## Assignments by SSA\n")
for name, asqs in assignments.items():
    print(f"### {name} (+{len(asqs)} ASQs)\n")
    for asq in asqs:
        print(f"- [{asq['asq_number']}]({asq['sf_link']}) — {asq['account_name']} (Score: {asq.get('charter_score')})")
    print()

# Quality check
total_new = sum(len(a) for a in assignments.values())
print(f"\n## Assignment Quality Check\n")
print(f"- Total ASQs to assign: {total_new}")

# Show distribution across team
print(f"\n| SSA | Assigned | % of Total |")
print(f"|-----|----------|------------|")
for name in sorted(assignments.keys(), key=lambda n: -len(assignments[n])):
    count = len(assignments[name])
    pct = (count / total_new * 100) if total_new > 0 else 0
    print(f"| {name} | {count} | {pct:.0f}% |")

# Check balance
counts = [len(a) for a in assignments.values() if len(a) > 0]
if counts:
    balance_ratio = min(counts) / max(counts)
    avg_load = total_new / len(SSA_PROFILES)
    if balance_ratio > 0.5:
        print(f"\n- Balance: ✅ Good (ratio {balance_ratio:.0%}, avg {avg_load:.1f} per SSA)")
    else:
        print(f"\n- Balance: ⚠️ Uneven (ratio {balance_ratio:.0%}) — review assignments")
else:
    print(f"\n- No assignments to distribute")
