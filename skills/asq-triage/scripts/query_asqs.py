#!/usr/bin/env python3
"""
Query ASQs from Databricks logfood with hygiene and urgency evaluation.

Usage:
    uv run scripts/query_asqs.py --region CAN
    uv run scripts/query_asqs.py --manager-id 0053f000000pKoTAAU
"""
import argparse
import json
import sys
from pathlib import Path
from databricks.sdk import WorkspaceClient

WAREHOUSE_ID = "927ac096f9833442"  # Shared SQL Endpoint - Stable (logfood)

QUERY = """
SELECT
  asq.approval_request_id AS asq_id,
  asq.approval_request_name AS asq_number,
  asq.Title AS asq_title,
  asq.status,
  asq.account_id,
  asq.account_name,
  asq.owner_user_id,
  asq.owner_user_name AS assigned_to,
  asq.technical_specialization,
  asq.support_type,
  asq.region_level_1 AS region,
  asq.business_unit,

  DATE(asq.created_date) AS request_date,
  DATE(asq.target_end_date) AS due_date,
  DATEDIFF(CURRENT_DATE, asq.created_date) AS days_open,
  CASE
    WHEN asq.target_end_date < CURRENT_DATE
    THEN DATEDIFF(CURRENT_DATE, asq.target_end_date)
    ELSE 0
  END AS days_overdue,

  CASE
    WHEN asq.request_status_notes IS NULL THEN 'NO_NOTES'
    WHEN LENGTH(asq.request_status_notes) < 10 THEN 'NO_NOTES'
    ELSE 'HAS_NOTES'
  END AS notes_status,

  CASE
    WHEN DATEDIFF(CURRENT_DATE, asq.created_date) > 90 THEN 'RULE5_EXCESSIVE'
    WHEN DATEDIFF(CURRENT_DATE, asq.created_date) BETWEEN 30 AND 90
      AND (asq.target_end_date IS NULL OR asq.target_end_date < CURRENT_DATE) THEN 'RULE3_STALE'
    WHEN asq.target_end_date < CURRENT_DATE - INTERVAL 7 DAYS THEN 'RULE4_EXPIRED'
    WHEN DATEDIFF(CURRENT_DATE, asq.created_date) > 7
      AND (asq.request_status_notes IS NULL OR LENGTH(asq.request_status_notes) < 10) THEN 'RULE1_MISSING_NOTES'
    ELSE 'COMPLIANT'
  END AS hygiene_status,

  CASE
    WHEN asq.target_end_date < CURRENT_DATE - INTERVAL 14 DAYS THEN 'CRITICAL'
    WHEN asq.target_end_date < CURRENT_DATE - INTERVAL 7 DAYS THEN 'HIGH'
    WHEN asq.target_end_date < CURRENT_DATE THEN 'MEDIUM'
    WHEN DATEDIFF(CURRENT_DATE, asq.created_date) > 14
      AND (asq.request_status_notes IS NULL OR LENGTH(asq.request_status_notes) < 10) THEN 'MEDIUM'
    ELSE 'NORMAL'
  END AS urgency,

  LEFT(asq.request_description, 500) AS description,
  LEFT(asq.request_status_notes, 500) AS notes,
  COALESCE(asq.estimated_effort_in_days, 5) AS estimated_days,
  asq.expected_dbu_dollar_impact,

  CONCAT('https://databricks.lightning.force.com/', asq.approval_request_id) AS sf_link

FROM main.gtm_silver.approval_request_detail asq
WHERE asq.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
  AND asq.status IN ('New', 'Submitted', 'Under Review', 'In Progress')
  AND asq.created_date >= CURRENT_DATE - INTERVAL 30 DAYS
  {region_filter}
  {manager_filter}
ORDER BY
  CASE
    WHEN asq.target_end_date < CURRENT_DATE - INTERVAL 14 DAYS THEN 1
    WHEN asq.target_end_date < CURRENT_DATE - INTERVAL 7 DAYS THEN 2
    WHEN asq.target_end_date < CURRENT_DATE THEN 3
    ELSE 4
  END,
  asq.created_date
"""


def query_asqs(region: str = "", manager_id: str = "", limit: int = 100) -> list[dict]:
    """Query ASQs from logfood and return as list of dicts."""
    w = WorkspaceClient(profile="logfood")

    region_filter = f"AND asq.region_level_1 = '{region}'" if region else ""
    manager_filter = ""
    if manager_id:
        manager_filter = f"""
  AND asq.owner_user_id IN (
    SELECT user_id FROM main.gtm_silver.individual_hierarchy_field
    WHERE manager_level_1_id = '{manager_id}'
  )"""

    query = QUERY.format(region_filter=region_filter, manager_filter=manager_filter)
    query += f"\nLIMIT {limit}"

    result = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=query,
        wait_timeout="50s",
    )

    if result.status.state.value != "SUCCEEDED":
        raise Exception(f"Query failed: {result.status}")

    if not result.result or not result.result.data_array:
        return []

    columns = [col.name for col in result.manifest.schema.columns]
    return [dict(zip(columns, row)) for row in result.result.data_array]


def main():
    parser = argparse.ArgumentParser(description="Query ASQs from logfood")
    parser.add_argument("--region", default="", help="Region filter (e.g., CAN)")
    parser.add_argument("--manager-id", default="", help="Manager SFDC User ID")
    parser.add_argument("--limit", type=int, default=100, help="Max rows")
    parser.add_argument("--output", choices=["json", "summary"], default="summary")
    args = parser.parse_args()

    if not args.region and not args.manager_id:
        print("Error: Must provide --region or --manager-id", file=sys.stderr)
        sys.exit(1)

    asqs = query_asqs(args.region, args.manager_id, args.limit)

    if args.output == "json":
        print(json.dumps(asqs, indent=2, default=str))
    else:
        # Summary output
        print(f"Total ASQs: {len(asqs)}")
        print()

        # Count by hygiene status
        hygiene_counts = {}
        urgency_counts = {}
        unassigned = 0

        for asq in asqs:
            hs = asq.get("hygiene_status", "UNKNOWN")
            hygiene_counts[hs] = hygiene_counts.get(hs, 0) + 1

            urg = asq.get("urgency", "UNKNOWN")
            urgency_counts[urg] = urgency_counts.get(urg, 0) + 1

            if not asq.get("assigned_to"):
                unassigned += 1

        print("Hygiene Status:")
        for status, count in sorted(hygiene_counts.items()):
            print(f"  {status}: {count}")

        print()
        print("Urgency:")
        for status, count in sorted(urgency_counts.items()):
            print(f"  {status}: {count}")

        print()
        print(f"Unassigned: {unassigned}")


if __name__ == "__main__":
    main()
