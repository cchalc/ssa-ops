#!/usr/bin/env python3
"""Find manager hierarchy info."""
import sys
from databricks.sdk import WorkspaceClient

WAREHOUSE_ID = "927ac096f9833442"

w = WorkspaceClient(profile="logfood")

# Search for Chris Chalcraft
query = """
SELECT
    user_id,
    user_name,
    manager_level_1_id,
    manager_level_1_name,
    manager_level_2_id,
    manager_level_2_name
FROM main.gtm_silver.individual_hierarchy_field
WHERE LOWER(user_name) LIKE '%chalcraft%'
   OR LOWER(user_name) LIKE '%chris c%'
"""

result = w.statement_execution.execute_statement(
    warehouse_id=WAREHOUSE_ID,
    statement=query,
    wait_timeout="50s",
)

if result.status.state.value == "SUCCEEDED" and result.result and result.result.data_array:
    columns = [col.name for col in result.manifest.schema.columns]
    print("Found:")
    for row in result.result.data_array:
        rec = dict(zip(columns, row))
        print(f"  User: {rec['user_name']} (ID: {rec['user_id']})")
        print(f"  Manager L1: {rec['manager_level_1_name']} (ID: {rec['manager_level_1_id']})")
        print(f"  Manager L2: {rec['manager_level_2_name']} (ID: {rec['manager_level_2_id']})")
        print()
else:
    print("Chris Chalcraft not found in hierarchy table")

# Also check who reports to that manager ID
print("\nChecking who reports to 0053f000000pKoTAAU:")
query2 = """
SELECT user_name, user_id
FROM main.gtm_silver.individual_hierarchy_field
WHERE manager_level_1_id = '0053f000000pKoTAAU'
   OR manager_level_2_id = '0053f000000pKoTAAU'
LIMIT 10
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
        print(f"  - {rec['user_name']} ({rec['user_id']})")
else:
    print("  No one found")

# Let's also check who the manager is for that ID
print("\nLooking up manager ID 0053f000000pKoTAAU:")
query3 = """
SELECT user_name, user_id, manager_level_1_name
FROM main.gtm_silver.individual_hierarchy_field
WHERE user_id = '0053f000000pKoTAAU'
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
        print(f"  User: {rec['user_name']} (ID: {rec['user_id']})")
        print(f"  Reports to: {rec['manager_level_1_name']}")
else:
    print("  Manager ID not found in hierarchy table")
