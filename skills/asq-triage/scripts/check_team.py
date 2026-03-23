#!/usr/bin/env python3
"""Check team members and their ASQs."""
import sys
from databricks.sdk import WorkspaceClient

WAREHOUSE_ID = "927ac096f9833442"
MANAGER_ID = "0053f000000pKoTAAU"  # Chris Chalcraft

w = WorkspaceClient(profile="logfood")

# First, get direct reports
print("Checking direct reports for manager:", MANAGER_ID)
query = f"""
SELECT user_id, user_name, manager_level_1_name
FROM main.gtm_silver.individual_hierarchy_field
WHERE manager_level_1_id = '{MANAGER_ID}'
"""

result = w.statement_execution.execute_statement(
    warehouse_id=WAREHOUSE_ID,
    statement=query,
    wait_timeout="50s",
)

if result.status.state.value != "SUCCEEDED":
    print(f"Query failed: {result.status}")
    sys.exit(1)

direct_reports = []
if result.result and result.result.data_array:
    columns = [col.name for col in result.manifest.schema.columns]
    print(f"\nDirect Reports ({len(result.result.data_array)}):")
    for row in result.result.data_array:
        rec = dict(zip(columns, row))
        print(f"  - {rec['user_name']} ({rec['user_id']})")
        direct_reports.append(rec['user_id'])
else:
    print("No direct reports found!")
    sys.exit(0)

# Now check ASQs for these users
if direct_reports:
    ids_str = ", ".join(f"'{uid}'" for uid in direct_reports)
    query2 = f"""
    SELECT
        owner_user_name,
        COUNT(*) as total_asqs,
        SUM(CASE WHEN created_date >= CURRENT_DATE - INTERVAL 30 DAYS THEN 1 ELSE 0 END) as new_asqs,
        SUM(CASE WHEN owner_user_id IS NULL OR owner_user_name IS NULL THEN 1 ELSE 0 END) as unassigned
    FROM main.gtm_silver.approval_request_detail
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
        AND status IN ('New', 'Submitted', 'Under Review', 'In Progress')
        AND owner_user_id IN ({ids_str})
    GROUP BY owner_user_name
    ORDER BY total_asqs DESC
    """

    result2 = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=query2,
        wait_timeout="50s",
    )

    if result2.status.state.value == "SUCCEEDED" and result2.result and result2.result.data_array:
        columns2 = [col.name for col in result2.manifest.schema.columns]
        print(f"\nASQ Counts by Team Member:")
        for row in result2.result.data_array:
            rec = dict(zip(columns2, row))
            print(f"  - {rec['owner_user_name']}: {rec['total_asqs']} total, {rec['new_asqs']} new (last 30d)")
    else:
        print("\nNo ASQs found for direct reports")

# Check unassigned ASQs in CAN that could go to the team
query3 = """
SELECT
    approval_request_name,
    account_name,
    DATEDIFF(CURRENT_DATE, created_date) as days_open,
    technical_specialization,
    support_type
FROM main.gtm_silver.approval_request_detail
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
    AND status IN ('New', 'Submitted', 'Under Review', 'In Progress')
    AND created_date >= CURRENT_DATE - INTERVAL 30 DAYS
    AND region_level_1 = 'CAN'
    AND (owner_user_id IS NULL OR owner_user_name IS NULL OR owner_user_name = '')
ORDER BY created_date DESC
LIMIT 20
"""

result3 = w.statement_execution.execute_statement(
    warehouse_id=WAREHOUSE_ID,
    statement=query3,
    wait_timeout="50s",
)

if result3.status.state.value == "SUCCEEDED" and result3.result and result3.result.data_array:
    columns3 = [col.name for col in result3.manifest.schema.columns]
    print(f"\nUnassigned ASQs in CAN (last 30 days): {len(result3.result.data_array)}")
    for row in result3.result.data_array[:10]:
        rec = dict(zip(columns3, row))
        print(f"  - {rec['approval_request_name']}: {rec['account_name']} ({rec['days_open']}d) - {rec['support_type']}")
else:
    print("\nNo unassigned ASQs in CAN")
