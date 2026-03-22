#!/usr/bin/env python3
"""
Analyze unassigned ASQs against SSA charter criteria.

Usage:
    uv run skills/asq-triage/scripts/analyze_unassigned.py --region CAN
    uv run skills/asq-triage/scripts/analyze_unassigned.py --manager-id 0053f000000pKoTAAU
"""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from query_asqs import query_asqs, WAREHOUSE_ID
from evaluate_charter import evaluate_charter
from databricks.sdk import WorkspaceClient


def get_team_capacity(region: str = "", manager_id: str = "") -> dict:
    """Get SSA team capacity (open ASQ counts) for the region or manager hierarchy."""
    w = WorkspaceClient(profile="logfood")

    # Build filter clause
    if manager_id:
        filter_clause = f"""
        AND owner_user_id IN (
            SELECT user_id FROM main.gtm_silver.individual_hierarchy_field
            WHERE manager_level_1_id = '{manager_id}'
               OR user_id = '{manager_id}'
        )"""
    elif region:
        filter_clause = f"AND region_level_1 = '{region}'"
    else:
        filter_clause = ""

    query = f"""
    SELECT
        owner_user_name AS ssa_name,
        owner_user_id AS ssa_id,
        COUNT(*) AS open_asq_count,
        SUM(CASE WHEN target_end_date < CURRENT_DATE THEN 1 ELSE 0 END) AS overdue_count
    FROM main.gtm_silver.approval_request_detail
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
        AND status IN ('New', 'Submitted', 'Under Review', 'In Progress')
        AND owner_user_name IS NOT NULL
        {filter_clause}
    GROUP BY owner_user_name, owner_user_id
    ORDER BY open_asq_count ASC
    """

    result = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=query,
        wait_timeout="50s",
    )

    if result.status.state.value != "SUCCEEDED" or not result.result:
        return {}

    columns = [col.name for col in result.manifest.schema.columns]
    capacity = {}
    for row in result.result.data_array:
        rec = dict(zip(columns, row))
        capacity[rec["ssa_name"]] = {
            "ssa_id": rec["ssa_id"],
            "open_asq_count": int(rec["open_asq_count"] or 0),
            "overdue_count": int(rec["overdue_count"] or 0),
        }
    return capacity


def analyze_asq(asq: dict, capacity: dict) -> dict:
    """Analyze a single ASQ and provide recommendation."""

    analysis = {
        "asq_number": asq.get("asq_number"),
        "account": asq.get("account_name"),
        "days_open": asq.get("days_open"),
        "support_type": asq.get("support_type"),
        "specialization": asq.get("technical_specialization"),
        "charter_alignment": asq.get("charter_alignment"),
        "charter_score": asq.get("charter_score"),
        "competitive": asq.get("competitive"),
        "near_win": asq.get("near_win"),
        "sf_link": asq.get("sf_link"),
        "description": asq.get("description"),
        "issues": [],
        "recommendation": None,
        "suggested_assignee": None,
    }

    description = (asq.get("description") or "").lower()

    # Check description quality
    if not description or len(description) < 50:
        analysis["issues"].append("MISSING_CONTEXT: Description too short, needs more detail")

    if "context:" in description and "please add details" in description:
        analysis["issues"].append("TEMPLATE_NOT_FILLED: Description contains unfilled template text")

    # Check for clear technical ask
    tech_keywords = ["migration", "architecture", "performance", "integration", "poc", "pilot",
                     "deployment", "optimization", "troubleshoot", "design", "review"]
    has_tech_ask = any(kw in description for kw in tech_keywords)
    if not has_tech_ask:
        analysis["issues"].append("UNCLEAR_ASK: No clear technical objective identified")

    # Check charter alignment
    score = int(asq.get("charter_score") or 0)
    if score < 0:
        analysis["issues"].append("MISALIGNED: Negative charter score, review for closure")
    elif score < 10:
        analysis["issues"].append("LOW_VALUE: Low charter score, confirm business justification")

    # Check for UCO linkage
    flags = asq.get("charter_flags") or []
    has_uco = any("U4" in f or "U5" in f or "U6" in f for f in flags)
    if not has_uco and int(asq.get("days_open") or 0) > 7:
        analysis["issues"].append("NO_UCO: No linked UCO found, verify pipeline impact")

    # Determine recommendation
    if len(analysis["issues"]) == 0:
        analysis["recommendation"] = "ASSIGN"
    elif any("MISALIGNED" in i or "TEMPLATE_NOT_FILLED" in i for i in analysis["issues"]):
        analysis["recommendation"] = "RETURN_TO_AE"
    elif any("MISSING_CONTEXT" in i or "UNCLEAR_ASK" in i for i in analysis["issues"]):
        analysis["recommendation"] = "NEEDS_CLARIFICATION"
    else:
        analysis["recommendation"] = "REVIEW"

    # Suggest assignee based on specialization and capacity
    spec = asq.get("technical_specialization") or "General"

    # Find SSAs with lowest workload
    if capacity:
        sorted_ssas = sorted(capacity.items(), key=lambda x: x[1]["open_asq_count"])
        # Prefer SSAs with < 5 ASQs
        low_load = [s for s in sorted_ssas if s[1]["open_asq_count"] < 5]
        if low_load:
            analysis["suggested_assignee"] = low_load[0][0]
            analysis["assignee_load"] = low_load[0][1]["open_asq_count"]
        elif sorted_ssas:
            analysis["suggested_assignee"] = sorted_ssas[0][0]
            analysis["assignee_load"] = sorted_ssas[0][1]["open_asq_count"]

    return analysis


def main():
    parser = argparse.ArgumentParser(description="Analyze unassigned ASQs")
    parser.add_argument("--region", help="Region filter (e.g., CAN)")
    parser.add_argument("--manager-id", help="Manager SFDC User ID to filter by hierarchy")
    parser.add_argument("--json", action="store_true", help="Output raw JSON")
    args = parser.parse_args()

    if not args.region and not args.manager_id:
        print("Error: Must provide --region or --manager-id", file=sys.stderr)
        sys.exit(1)

    filter_desc = args.region or f"Manager {args.manager_id[:8]}..."
    print(f"Analyzing unassigned ASQs for {filter_desc}...", file=sys.stderr)

    # Query ASQs
    asqs = query_asqs(region=args.region or "", manager_id=args.manager_id or "", limit=200)
    print(f"Found {len(asqs)} total ASQs", file=sys.stderr)

    # Evaluate charter
    asqs = evaluate_charter(asqs)

    # Filter unassigned
    unassigned = [a for a in asqs if not a.get("assigned_to")]
    print(f"Found {len(unassigned)} unassigned ASQs", file=sys.stderr)

    if not unassigned:
        print("No unassigned ASQs found.")
        return

    # Get team capacity
    print("Getting team capacity...", file=sys.stderr)
    capacity = get_team_capacity(region=args.region or "", manager_id=args.manager_id or "")
    print(f"Found {len(capacity)} SSAs with capacity data", file=sys.stderr)

    # Analyze each unassigned ASQ
    analyses = []
    for asq in unassigned:
        analysis = analyze_asq(asq, capacity)
        analyses.append(analysis)

    if args.json:
        print(json.dumps(analyses, indent=2, default=str))
        return

    # Print report
    print("\n" + "=" * 80)
    print(f"UNASSIGNED ASQ ANALYSIS — {filter_desc} — {len(unassigned)} ASQs")
    print("=" * 80)

    # Group by recommendation
    by_rec = {}
    for a in analyses:
        rec = a["recommendation"]
        if rec not in by_rec:
            by_rec[rec] = []
        by_rec[rec].append(a)

    # Print capacity summary
    print("\n## Team Capacity (Lowest Workload First)\n")
    if capacity:
        sorted_cap = sorted(capacity.items(), key=lambda x: x[1]["open_asq_count"])
        for ssa, data in sorted_cap[:10]:
            load = "🟢" if data["open_asq_count"] < 3 else "🟡" if data["open_asq_count"] < 6 else "🔴"
            print(f"  {load} {ssa}: {data['open_asq_count']} ASQs ({data['overdue_count']} overdue)")

    # Print by recommendation category
    for rec in ["ASSIGN", "NEEDS_CLARIFICATION", "RETURN_TO_AE", "REVIEW"]:
        if rec not in by_rec:
            continue

        print(f"\n## {rec} ({len(by_rec[rec])})\n")

        for a in by_rec[rec]:
            print(f"### {a['asq_number']} — {a['account']}")
            print(f"Days Open: {a['days_open']} | Charter: {a['charter_alignment']} (score: {a['charter_score']})")
            print(f"Type: {a['support_type']} | Spec: {a['specialization']}")
            if a['competitive']:
                print("🏆 COMPETITIVE OPPORTUNITY")
            if a['near_win']:
                print("🎯 NEAR WIN (U4/U5)")
            print(f"Link: {a['sf_link']}")

            if a['issues']:
                print("\n**Issues:**")
                for issue in a['issues']:
                    print(f"  ⚠️  {issue}")

            if a['suggested_assignee']:
                print(f"\n**Suggested Assignee:** {a['suggested_assignee']} ({a.get('assignee_load', '?')} current ASQs)")

            print(f"\n**Description:**")
            desc = (a['description'] or "")[:400]
            print(f"  {desc}...")
            print()

    print("=" * 80)


if __name__ == "__main__":
    main()
