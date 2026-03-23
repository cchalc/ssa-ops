#!/usr/bin/env python3
"""Find direct reports from ASQ data."""
import sys
from databricks.sdk import WorkspaceClient

WAREHOUSE_ID = "927ac096f9833442"

w = WorkspaceClient(profile="logfood")

# Search for specific people from earlier output (Réda Khouani, Volodymyr Vragov)
print("Searching for Réda Khouani and Volodymyr Vragov in ASQ data:")
query = """
SELECT DISTINCT
    owner_user_name,
    owner_user_id
FROM main.gtm_silver.approval_request_detail
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
    AND (
        LOWER(owner_user_name) LIKE '%khouani%'
        OR LOWER(owner_user_name) LIKE '%vragov%'
        OR LOWER(owner_user_name) LIKE '%réda%'
        OR LOWER(owner_user_name) LIKE '%volodymyr%'
    )
"""

result = w.statement_execution.execute_statement(
    warehouse_id=WAREHOUSE_ID,
    statement=query,
    wait_timeout="50s",
)

direct_report_ids = []
if result.status.state.value == "SUCCEEDED" and result.result and result.result.data_array:
    columns = [col.name for col in result.manifest.schema.columns]
    for row in result.result.data_array:
        rec = dict(zip(columns, row))
        print(f"  - {rec['owner_user_name']} (ID: {rec['owner_user_id']})")
        direct_report_ids.append(rec['owner_user_id'])
else:
    print("  Not found")

# Now look up their hierarchy info
if direct_report_ids:
    ids_str = ", ".join(f"'{uid}'" for uid in direct_report_ids)
    print("\nHierarchy info for found SSAs:")
    query2 = f"""
    SELECT
        user_name,
        user_id,
        manager_level_1_name,
        manager_level_1_id
    FROM main.gtm_silver.individual_hierarchy_field
    WHERE user_id IN ({ids_str})
    """

    result2 = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=query2,
        wait_timeout="50s",
    )

    if result2.status.state.value == "SUCCEEDED" and result2.result and result2.result.data_array:
        columns2 = [col.name for col in result2.manifest.schema.columns]
        manager_ids = set()
        for row in result2.result.data_array:
            rec = dict(zip(columns2, row))
            print(f"  - {rec['user_name']} reports to {rec['manager_level_1_name']} (ID: {rec['manager_level_1_id']})")
            manager_ids.add(rec['manager_level_1_id'])

        # Now find all direct reports for those managers
        for mgr_id in manager_ids:
            print(f"\n\nAll direct reports for manager ID {mgr_id}:")
            query3 = f"""
            SELECT user_name, user_id
            FROM main.gtm_silver.individual_hierarchy_field
            WHERE manager_level_1_id = '{mgr_id}'
            ORDER BY user_name
            """

            result3 = w.statement_execution.execute_statement(
                warehouse_id=WAREHOUSE_ID,
                statement=query3,
                wait_timeout="50s",
            )

            if result3.status.state.value == "SUCCEEDED" and result3.result and result3.result.data_array:
                columns3 = [col.name for col in result3.manifest.schema.columns]
                for row in result3.result.data_array:
                    rec = dict(zip(columns3, row))
                    print(f"  - {rec['user_name']} ({rec['user_id']})")
    else:
        print("  Not in hierarchy table")
