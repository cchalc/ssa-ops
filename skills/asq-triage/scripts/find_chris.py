#!/usr/bin/env python3
"""Find Chris Chalcraft in various tables."""
import sys
from databricks.sdk import WorkspaceClient

WAREHOUSE_ID = "927ac096f9833442"

w = WorkspaceClient(profile="logfood")

# Search in ASQ data for Chris
print("Searching ASQ data for 'Chris' owners in CAN:")
query = """
SELECT DISTINCT
    owner_user_name,
    owner_user_id
FROM main.gtm_silver.approval_request_detail
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
    AND region_level_1 = 'CAN'
    AND LOWER(owner_user_name) LIKE '%chris%'
ORDER BY owner_user_name
"""

result = w.statement_execution.execute_statement(
    warehouse_id=WAREHOUSE_ID,
    statement=query,
    wait_timeout="50s",
)

if result.status.state.value == "SUCCEEDED" and result.result and result.result.data_array:
    columns = [col.name for col in result.manifest.schema.columns]
    for row in result.result.data_array:
        rec = dict(zip(columns, row))
        print(f"  - {rec['owner_user_name']} (ID: {rec['owner_user_id']})")
else:
    print("  No Chris found in ASQ owners")

# Search for Réda and Volodymyr (from earlier - likely direct reports)
print("\nSearching for known CAN SSAs (Réda, Volodymyr) to find their manager:")
query2 = """
SELECT
    user_name,
    user_id,
    manager_level_1_name,
    manager_level_1_id,
    manager_level_2_name,
    manager_level_2_id
FROM main.gtm_silver.individual_hierarchy_field
WHERE LOWER(user_name) LIKE '%réda%'
   OR LOWER(user_name) LIKE '%reda%'
   OR LOWER(user_name) LIKE '%volodymyr%'
   OR LOWER(user_name) LIKE '%khouani%'
   OR LOWER(user_name) LIKE '%vragov%'
"""

result2 = w.statement_execution.execute_statement(
    warehouse_id=WAREHOUSE_ID,
    statement=query2,
    wait_timeout="50s",
)

if result2.status.state.value == "SUCCEEDED" and result2.result and result2.result.data_array:
    columns2 = [col.name for col in result2.manifest.schema.columns]
    for row in result2.result.data_array:
        rec = dict(zip(columns2, row))
        print(f"\n  User: {rec['user_name']} (ID: {rec['user_id']})")
        print(f"  Manager L1: {rec['manager_level_1_name']} (ID: {rec['manager_level_1_id']})")
        print(f"  Manager L2: {rec['manager_level_2_name']} (ID: {rec['manager_level_2_id']})")
else:
    print("  SSAs not found")

# Also get all CAN region managers
print("\nCAN Region Managers (from ASQ owners' hierarchy):")
query3 = """
SELECT DISTINCT
    manager_level_1_name,
    manager_level_1_id,
    COUNT(*) as team_size
FROM main.gtm_silver.individual_hierarchy_field h
WHERE EXISTS (
    SELECT 1 FROM main.gtm_silver.approval_request_detail a
    WHERE a.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
      AND a.region_level_1 = 'CAN'
      AND a.owner_user_id = h.user_id
)
GROUP BY manager_level_1_name, manager_level_1_id
ORDER BY team_size DESC
LIMIT 10
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
        print(f"  - {rec['manager_level_1_name']} (ID: {rec['manager_level_1_id']}) - {rec['team_size']} reports")
else:
    print("  No managers found")
