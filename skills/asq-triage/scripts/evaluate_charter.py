#!/usr/bin/env python3
"""
Evaluate ASQs against SSA charter priorities.

Charter priorities:
1. Production Outcomes - UCOs reaching U5/U6
2. Competitive Wins - Displacement of competitors
3. Focus & Discipline - 80% on L400+ accounts (currently blocked - no data)
"""
import argparse
import json
import re
import sys
from databricks.sdk import WorkspaceClient

WAREHOUSE_ID = "927ac096f9833442"

# Competitive keywords
COMPETITIVE_KEYWORDS = [
    "fabric", "synapse", "snowflake", "redshift", "bigquery",
    "power bi", "tableau", "looker", "compete", "displacement",
    "migrate from", "replacing", "vs databricks", "alternative"
]

# Production/win keywords
WIN_KEYWORDS = [
    "production", "go live", "live", "deployed", "launched",
    "signed", "closed won", "contract", "expansion"
]

# Risk keywords
RISK_KEYWORDS = [
    "churn", "cancel", "risk", "unhappy", "escalation",
    "competitor", "leaving", "switching", "lost"
]


def get_uco_data(account_ids: list[str]) -> dict:
    """Get UCO data for accounts from logfood."""
    if not account_ids:
        return {}

    w = WorkspaceClient(profile="logfood")

    ids_str = ", ".join(f"'{aid}'" for aid in account_ids if aid)
    query = f"""
    SELECT
      account_id,
      usecase_id,
      usecase_name,
      stage,
      estimated_monthly_dollar_dbus,
      competitor_status,
      primary_competitor
    FROM main.gtm_silver.use_case_detail
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.use_case_detail)
      AND account_id IN ({ids_str})
      AND is_active_ind = true
    """

    result = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=query,
        wait_timeout="50s",
    )

    if result.status.state.value != "SUCCEEDED" or not result.result:
        return {}

    # Group by account_id
    ucos_by_account = {}
    columns = [col.name for col in result.manifest.schema.columns]

    for row in result.result.data_array:
        uco = dict(zip(columns, row))
        account_id = uco["account_id"]
        if account_id not in ucos_by_account:
            ucos_by_account[account_id] = []
        ucos_by_account[account_id].append(uco)

    return ucos_by_account


def evaluate_charter(asqs: list[dict]) -> list[dict]:
    """Evaluate ASQs against charter criteria and return enriched ASQs."""

    # Get UCO data for all accounts
    account_ids = list(set(asq.get("account_id") for asq in asqs if asq.get("account_id")))
    ucos_by_account = get_uco_data(account_ids)

    results = []

    for asq in asqs:
        evaluation = {
            **asq,
            "charter_score": 0,
            "charter_flags": [],
            "linked_ucos": [],
            "competitive": False,
            "near_win": False,
            "production": False,
            "risk": False,
        }

        account_id = asq.get("account_id")
        description = (asq.get("description") or "").lower()
        notes = (asq.get("notes") or "").lower()
        text = description + " " + notes

        # Check for competitive keywords
        for keyword in COMPETITIVE_KEYWORDS:
            if keyword in text:
                evaluation["competitive"] = True
                evaluation["charter_flags"].append(f"COMPETITIVE:{keyword}")
                evaluation["charter_score"] += 10
                break

        # Check for win keywords
        for keyword in WIN_KEYWORDS:
            if keyword in text:
                evaluation["charter_flags"].append(f"WIN_SIGNAL:{keyword}")
                evaluation["charter_score"] += 5
                break

        # Check for risk keywords
        for keyword in RISK_KEYWORDS:
            if keyword in text:
                evaluation["risk"] = True
                evaluation["charter_flags"].append(f"RISK:{keyword}")
                evaluation["charter_score"] -= 5
                break

        # Check UCO linkage
        account_ucos = ucos_by_account.get(account_id, [])
        if account_ucos:
            evaluation["linked_ucos"] = account_ucos

            for uco in account_ucos:
                stage = uco.get("stage") or ""

                # Near win (U4/U5)
                if "U4" in stage or "U5" in stage:
                    evaluation["near_win"] = True
                    evaluation["charter_flags"].append(f"NEAR_WIN:{stage}")
                    evaluation["charter_score"] += 10

                # Production (U6)
                if "U6" in stage:
                    evaluation["production"] = True
                    evaluation["charter_flags"].append(f"PRODUCTION:{stage}")
                    evaluation["charter_score"] += 15

                # Competitive UCO
                if uco.get("competitor_status") or uco.get("primary_competitor"):
                    evaluation["competitive"] = True
                    comp = uco.get("primary_competitor") or uco.get("competitor_status")
                    evaluation["charter_flags"].append(f"UCO_COMPETITIVE:{comp}")
                    evaluation["charter_score"] += 5
        else:
            # No UCO linkage - potential issue for long-running ASQs
            days_open_check = int(asq.get("days_open") or 0)
            if days_open_check > 30:
                evaluation["charter_flags"].append("NO_UCO_LINKAGE")
                evaluation["charter_score"] -= 5

        # Penalize excessively old ASQs without production
        days_open = int(asq.get("days_open") or 0)
        if days_open > 90 and not evaluation["production"]:
            evaluation["charter_flags"].append("STALE_NO_PRODUCTION")
            evaluation["charter_score"] -= 10

        # Classify charter alignment
        score = evaluation["charter_score"]
        if score >= 20:
            evaluation["charter_alignment"] = "HIGHLY_ALIGNED"
        elif score >= 10:
            evaluation["charter_alignment"] = "ALIGNED"
        elif score >= 0:
            evaluation["charter_alignment"] = "NEUTRAL"
        else:
            evaluation["charter_alignment"] = "MISALIGNED"

        results.append(evaluation)

    return results


def main():
    parser = argparse.ArgumentParser(description="Evaluate ASQs against SSA charter")
    parser.add_argument("--input", help="JSON file with ASQ data (from query_asqs.py)")
    parser.add_argument("--region", help="Query ASQs for this region")
    parser.add_argument("--output", choices=["json", "summary"], default="summary")
    args = parser.parse_args()

    if args.input:
        with open(args.input) as f:
            asqs = json.load(f)
    elif args.region:
        from query_asqs import query_asqs
        asqs = query_asqs(region=args.region, limit=100)
    else:
        print("Error: Must provide --input or --region", file=sys.stderr)
        sys.exit(1)

    results = evaluate_charter(asqs)

    if args.output == "json":
        print(json.dumps(results, indent=2, default=str))
    else:
        # Summary output
        alignment_counts = {}
        competitive_count = 0
        near_win_count = 0
        production_count = 0
        risk_count = 0
        no_uco_count = 0

        for r in results:
            alignment = r.get("charter_alignment", "UNKNOWN")
            alignment_counts[alignment] = alignment_counts.get(alignment, 0) + 1

            if r.get("competitive"):
                competitive_count += 1
            if r.get("near_win"):
                near_win_count += 1
            if r.get("production"):
                production_count += 1
            if r.get("risk"):
                risk_count += 1
            if "NO_UCO_LINKAGE" in r.get("charter_flags", []):
                no_uco_count += 1

        print(f"Charter Evaluation Summary ({len(results)} ASQs)")
        print()
        print("Alignment:")
        for alignment, count in sorted(alignment_counts.items()):
            print(f"  {alignment}: {count}")
        print()
        print("Flags:")
        print(f"  Competitive: {competitive_count}")
        print(f"  Near Win (U4/U5): {near_win_count}")
        print(f"  Production (U6): {production_count}")
        print(f"  Risk: {risk_count}")
        print(f"  No UCO Linkage: {no_uco_count}")


if __name__ == "__main__":
    main()
