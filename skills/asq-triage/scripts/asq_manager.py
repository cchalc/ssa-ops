#!/usr/bin/env python3
"""
ASQ Triage — Main entry point for SSA ASQ management.

Usage:
    uv run skills/asq-triage/scripts/asq_manager.py --region CAN --hygiene
    uv run skills/asq-triage/scripts/asq_manager.py --region CAN --charter-eval --report-only
    uv run skills/asq-triage/scripts/asq_manager.py --manager-id 0053f000000pKoTAAU --dry-run
"""
import argparse
import json
import sys
from datetime import date
from pathlib import Path

# Add scripts directory to path
sys.path.insert(0, str(Path(__file__).parent))

from query_asqs import query_asqs
from evaluate_charter import evaluate_charter
from generate_report import generate_report


def main():
    parser = argparse.ArgumentParser(
        description="ASQ Triage — Evaluate and manage ASQs from Databricks logfood",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Hygiene check for CAN region
    %(prog)s --region CAN --hygiene

    # Charter evaluation with report
    %(prog)s --region CAN --charter-eval --report-only

    # Dry run to preview actions
    %(prog)s --region CAN --hygiene --dry-run

    # Full report for manager's team
    %(prog)s --manager-id 0053f000000pKoTAAU --report-only
        """
    )

    # Filter options
    parser.add_argument("--region", help="Filter by region (CAN, RCT, FINS, etc.)")
    parser.add_argument("--manager-id", help="Filter by manager hierarchy (SFDC User ID)")

    # Action flags
    parser.add_argument("--hygiene", action="store_true", help="Run 5-rule hygiene check")
    parser.add_argument("--charter-eval", action="store_true", help="Evaluate against SSA charter")
    parser.add_argument("--assign", action="store_true", help="Suggest assignments for unassigned ASQs")
    parser.add_argument("--slack", action="store_true", help="Send Slack notifications")

    # Mode flags
    parser.add_argument("--report-only", action="store_true", help="Generate report without actions")
    parser.add_argument("--dry-run", action="store_true", help="Preview actions without executing")

    # Output options
    parser.add_argument("--output", help="Output file path for report")
    parser.add_argument("--json", action="store_true", help="Output raw JSON data")
    parser.add_argument("--limit", type=int, default=100, help="Max ASQs to process")

    args = parser.parse_args()

    # Validate inputs
    if not args.region and not args.manager_id:
        print("Error: Must provide --region or --manager-id", file=sys.stderr)
        sys.exit(1)

    # Default to hygiene + report-only if no action specified
    if not any([args.hygiene, args.charter_eval, args.assign, args.slack]):
        args.hygiene = True
        args.report_only = True

    # Banner
    filter_desc = args.region or f"Manager {args.manager_id[:8]}..."
    print(f"ASQ Triage — {filter_desc}", file=sys.stderr)
    print(f"Date: {date.today().isoformat()}", file=sys.stderr)
    print(f"Mode: {'DRY RUN' if args.dry_run else 'REPORT ONLY' if args.report_only else 'LIVE'}", file=sys.stderr)
    print(file=sys.stderr)

    # Step 1: Query ASQs
    print("Querying ASQs from logfood...", file=sys.stderr)
    asqs = query_asqs(
        region=args.region or "",
        manager_id=args.manager_id or "",
        limit=args.limit
    )
    print(f"Found {len(asqs)} ASQs", file=sys.stderr)

    if not asqs:
        print("No ASQs found matching criteria", file=sys.stderr)
        sys.exit(0)

    # Step 2: Charter evaluation (if requested)
    if args.charter_eval:
        print("Evaluating against SSA charter...", file=sys.stderr)
        asqs = evaluate_charter(asqs)

    # Step 3: Hygiene summary
    if args.hygiene:
        print("\n--- Hygiene Summary ---", file=sys.stderr)
        hygiene_counts = {}
        for asq in asqs:
            hs = asq.get("hygiene_status", "UNKNOWN")
            hygiene_counts[hs] = hygiene_counts.get(hs, 0) + 1

        for status in ["RULE5_EXCESSIVE", "RULE4_EXPIRED", "RULE3_STALE", "RULE1_MISSING_NOTES", "COMPLIANT"]:
            if status in hygiene_counts:
                print(f"  {status}: {hygiene_counts[status]}", file=sys.stderr)

    # Step 4: Charter summary
    if args.charter_eval:
        print("\n--- Charter Summary ---", file=sys.stderr)
        alignment_counts = {}
        for asq in asqs:
            al = asq.get("charter_alignment", "UNKNOWN")
            alignment_counts[al] = alignment_counts.get(al, 0) + 1

        for alignment in ["HIGHLY_ALIGNED", "ALIGNED", "NEUTRAL", "MISALIGNED"]:
            if alignment in alignment_counts:
                print(f"  {alignment}: {alignment_counts[alignment]}", file=sys.stderr)

        competitive = sum(1 for a in asqs if a.get("competitive"))
        near_win = sum(1 for a in asqs if a.get("near_win"))
        print(f"  Competitive: {competitive}", file=sys.stderr)
        print(f"  Near Win: {near_win}", file=sys.stderr)

    # Step 5: Assignment suggestions
    if args.assign:
        unassigned = [a for a in asqs if not a.get("assigned_to")]
        if unassigned:
            print(f"\n--- Unassigned ASQs ({len(unassigned)}) ---", file=sys.stderr)
            for asq in unassigned[:5]:
                print(f"  {asq['asq_number']} — {asq.get('account_name')} — {asq.get('technical_specialization', 'General')}", file=sys.stderr)

            if args.dry_run:
                print("\n[DRY RUN] Would suggest assignments based on specialization and workload", file=sys.stderr)
            elif not args.report_only:
                print("\n[TODO] Assignment logic not yet implemented", file=sys.stderr)

    # Step 6: Generate report
    print("\n", file=sys.stderr)

    if args.json:
        print(json.dumps(asqs, indent=2, default=str))
    else:
        report = generate_report(asqs, args.region or "", args.manager_id or "")

        if args.output:
            Path(args.output).write_text(report)
            print(f"Report saved to: {args.output}", file=sys.stderr)
        else:
            print(report)

    # Step 7: Slack notifications
    if args.slack:
        if args.dry_run:
            print("\n[DRY RUN] Would send Slack notifications to:", file=sys.stderr)
            ssas = set(a.get("assigned_to") for a in asqs if a.get("assigned_to"))
            for ssa in sorted(ssas):
                violations = sum(1 for a in asqs if a.get("assigned_to") == ssa and a.get("hygiene_status") != "COMPLIANT")
                if violations > 0:
                    print(f"  - {ssa}: {violations} violations", file=sys.stderr)
        elif not args.report_only:
            print("\n[TODO] Slack integration not yet implemented", file=sys.stderr)

    print("\nDone.", file=sys.stderr)


if __name__ == "__main__":
    main()
