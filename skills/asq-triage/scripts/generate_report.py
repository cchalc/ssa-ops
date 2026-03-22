#!/usr/bin/env python3
"""
Generate ASQ Triage report in Markdown format.

Combines hygiene analysis and charter evaluation into a comprehensive report.
"""
import argparse
import json
import sys
from datetime import date
from pathlib import Path


def safe_int(value, default: int = 0) -> int:
    """Convert value to int, handling strings and None."""
    if value is None:
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def generate_report(asqs: list[dict], region: str = "", manager_id: str = "") -> str:
    """Generate markdown report from evaluated ASQs."""

    today = date.today().isoformat()
    filter_desc = region or f"Manager {manager_id[:8]}..." if manager_id else "All"

    # Calculate summary stats
    total = len(asqs)
    critical = sum(1 for a in asqs if a.get("urgency") == "CRITICAL")
    rule5 = sum(1 for a in asqs if a.get("hygiene_status") == "RULE5_EXCESSIVE")
    unassigned = sum(1 for a in asqs if not a.get("assigned_to"))
    no_notes = sum(1 for a in asqs if a.get("notes_status") == "NO_NOTES")

    # Charter stats
    aligned = sum(1 for a in asqs if a.get("charter_alignment") in ("HIGHLY_ALIGNED", "ALIGNED"))
    competitive = sum(1 for a in asqs if a.get("competitive"))
    near_win = sum(1 for a in asqs if a.get("near_win"))

    alignment_pct = round(100 * aligned / total, 1) if total > 0 else 0

    report = f"""# ASQ Triage Report — {filter_desc} — {today}

## Executive Summary

| Metric | Value |
|--------|-------|
| Total Open ASQs | {total} |
| CRITICAL (>14 days overdue) | {critical} |
| RULE5_EXCESSIVE (>90 days) | {rule5} |
| Unassigned | {unassigned} |
| Missing Notes | {no_notes} |
| Charter Aligned | {alignment_pct}% |
| Competitive Opportunities | {competitive} |
| Near Win (U4/U5) | {near_win} |

---

## Hygiene Violations by Severity

"""

    # Group by hygiene status
    by_hygiene = {}
    for asq in asqs:
        hs = asq.get("hygiene_status", "UNKNOWN")
        if hs not in by_hygiene:
            by_hygiene[hs] = []
        by_hygiene[hs].append(asq)

    # Order by severity
    severity_order = ["RULE5_EXCESSIVE", "RULE4_EXPIRED", "RULE3_STALE", "RULE1_MISSING_NOTES", "COMPLIANT"]

    for hs in severity_order:
        if hs not in by_hygiene:
            continue
        asqs_in_status = by_hygiene[hs]
        if not asqs_in_status:
            continue

        report += f"### {hs} ({len(asqs_in_status)})\n\n"

        for asq in asqs_in_status[:10]:  # Limit to 10 per status
            ar = asq.get("asq_number", "AR-???")
            account = asq.get("account_name", "Unknown")
            assigned = asq.get("assigned_to") or "**UNASSIGNED**"
            days = asq.get("days_open", 0)
            sf_link = asq.get("sf_link", "#")

            report += f"- [{ar}]({sf_link}) — {account} — {assigned} — {days} days\n"

        if len(asqs_in_status) > 10:
            report += f"- ... and {len(asqs_in_status) - 10} more\n"

        report += "\n"

    # Charter evaluation section
    report += """---

## Charter Alignment Issues

"""

    # No UCO linkage (long-running)
    no_uco = [a for a in asqs if "NO_UCO_LINKAGE" in a.get("charter_flags", [])]
    if no_uco:
        report += f"### Long-Running Without UCO ({len(no_uco)})\n\n"
        for asq in no_uco[:5]:
            ar = asq.get("asq_number", "AR-???")
            account = asq.get("account_name", "Unknown")
            days = asq.get("days_open", 0)
            sf_link = asq.get("sf_link", "#")
            report += f"- [{ar}]({sf_link}) — {account} — {days} days — no linked UCO\n"
        report += "\n"

    # Misaligned ASQs
    misaligned = [a for a in asqs if a.get("charter_alignment") == "MISALIGNED"]
    if misaligned:
        report += f"### Misaligned with Charter ({len(misaligned)})\n\n"
        for asq in misaligned[:5]:
            ar = asq.get("asq_number", "AR-???")
            account = asq.get("account_name", "Unknown")
            flags = ", ".join(asq.get("charter_flags", []))
            sf_link = asq.get("sf_link", "#")
            report += f"- [{ar}]({sf_link}) — {account} — Flags: {flags}\n"
        report += "\n"

    # Competitive opportunities
    competitive_asqs = [a for a in asqs if a.get("competitive")]
    if competitive_asqs:
        report += f"### Competitive Opportunities ({len(competitive_asqs)})\n\n"
        for asq in competitive_asqs[:5]:
            ar = asq.get("asq_number", "AR-???")
            account = asq.get("account_name", "Unknown")
            flags = [f for f in asq.get("charter_flags", []) if "COMPETITIVE" in f]
            sf_link = asq.get("sf_link", "#")
            report += f"- [{ar}]({sf_link}) — {account} — {', '.join(flags)}\n"
        report += "\n"

    # Near wins
    near_win_asqs = [a for a in asqs if a.get("near_win")]
    if near_win_asqs:
        report += f"### Near Win Opportunities ({len(near_win_asqs)})\n\n"
        for asq in near_win_asqs[:5]:
            ar = asq.get("asq_number", "AR-???")
            account = asq.get("account_name", "Unknown")
            flags = [f for f in asq.get("charter_flags", []) if "NEAR_WIN" in f or "U4" in f or "U5" in f]
            sf_link = asq.get("sf_link", "#")
            report += f"- [{ar}]({sf_link}) — {account} — {', '.join(flags)}\n"
        report += "\n"

    # Recommended actions
    report += """---

## Recommended Actions

### Immediate (Today)

"""

    # Top 5 most critical
    critical_asqs = sorted(
        [a for a in asqs if a.get("urgency") == "CRITICAL"],
        key=lambda x: safe_int(x.get("days_overdue")),
        reverse=True
    )[:5]

    for i, asq in enumerate(critical_asqs, 1):
        ar = asq.get("asq_number", "AR-???")
        account = asq.get("account_name", "Unknown")
        days = asq.get("days_overdue", 0)
        sf_link = asq.get("sf_link", "#")
        report += f"{i}. Close or update [{ar}]({sf_link}) ({account}) — {days} days overdue\n"

    report += "\n### This Week\n\n"

    # Unassigned ASQs
    unassigned_asqs = [a for a in asqs if not a.get("assigned_to")][:3]
    for i, asq in enumerate(unassigned_asqs, 1):
        ar = asq.get("asq_number", "AR-???")
        account = asq.get("account_name", "Unknown")
        sf_link = asq.get("sf_link", "#")
        spec = asq.get("technical_specialization") or "General"
        report += f"{i}. Assign [{ar}]({sf_link}) ({account}) — needs {spec}\n"

    report += f"""

---

## ASQ Breakdown by SSA

"""

    # Group by assigned_to
    by_ssa = {}
    for asq in asqs:
        ssa = asq.get("assigned_to") or "Unassigned"
        if ssa not in by_ssa:
            by_ssa[ssa] = []
        by_ssa[ssa].append(asq)

    for ssa, ssa_asqs in sorted(by_ssa.items(), key=lambda x: -len(x[1])):
        overdue = sum(1 for a in ssa_asqs if safe_int(a.get("days_overdue")) > 0)
        report += f"### {ssa} — {len(ssa_asqs)} ASQs ({overdue} overdue)\n\n"

        for asq in ssa_asqs[:3]:
            ar = asq.get("asq_number", "AR-???")
            account = asq.get("account_name", "Unknown")
            hs = asq.get("hygiene_status", "")
            days = asq.get("days_open", 0)
            sf_link = asq.get("sf_link", "#")
            report += f"- [{ar}]({sf_link}) — {account} — {hs} — {days}d\n"

        if len(ssa_asqs) > 3:
            report += f"- ... and {len(ssa_asqs) - 3} more\n"
        report += "\n"

    report += f"""---

*Report generated: {today}*
*Source: Databricks logfood (main.gtm_silver.approval_request_detail)*
"""

    return report


def main():
    parser = argparse.ArgumentParser(description="Generate ASQ Triage report")
    parser.add_argument("--input", required=True, help="JSON file with evaluated ASQ data")
    parser.add_argument("--region", default="", help="Region for report title")
    parser.add_argument("--manager-id", default="", help="Manager ID for report title")
    parser.add_argument("--output", help="Output file path (default: stdout)")
    args = parser.parse_args()

    with open(args.input) as f:
        asqs = json.load(f)

    report = generate_report(asqs, args.region, args.manager_id)

    if args.output:
        Path(args.output).write_text(report)
        print(f"Report saved to: {args.output}", file=sys.stderr)
    else:
        print(report)


if __name__ == "__main__":
    main()
