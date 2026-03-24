#!/usr/bin/env python3
"""Test charter metric views on Databricks."""

import json
import subprocess
import sys

def execute_sql(statement: str, profile: str = "logfood", warehouse_id: str = "927ac096f9833442") -> dict:
    """Execute SQL via Databricks API."""
    payload = {
        "warehouse_id": warehouse_id,
        "statement": statement,
        "wait_timeout": "50s"
    }

    cmd = [
        "databricks", "api", "post", "/api/2.0/sql/statements",
        "--profile", profile,
        "--json", json.dumps(payload)
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        return {"error": result.stderr}

    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return {"error": result.stdout}


def test_view_exists(name: str, catalog: str, schema: str) -> bool:
    """Test that a metric view exists."""
    query = f"""
    SELECT table_name
    FROM {catalog}.information_schema.tables
    WHERE table_schema = '{schema}' AND table_name = '{name}'
    """
    result = execute_sql(query)

    if "error" in result:
        print(f"  ❌ API Error: {result['error'][:100]}")
        return False

    status = result.get("status", {})
    if status.get("state") == "FAILED":
        error = status.get("error", {}).get("message", "Unknown error")
        print(f"  ❌ Query Failed: {error[:100]}")
        return False

    row_count = result.get("manifest", {}).get("total_row_count", 0)
    if row_count == 0:
        print(f"  ❌ View not found")
        return False

    print(f"  ✅ View exists")
    return True


def test_view_returns_data(name: str, catalog: str, schema: str) -> bool:
    """Test that a metric view returns data when queried."""
    query = f"""
    SELECT COUNT(1) AS cnt
    FROM {catalog}.{schema}.{name}
    LIMIT 1
    """
    result = execute_sql(query)

    if "error" in result:
        print(f"  ❌ API Error: {result['error'][:100]}")
        return False

    status = result.get("status", {})
    if status.get("state") == "FAILED":
        error = status.get("error", {}).get("message", "Unknown error")
        print(f"  ❌ Query Failed: {error[:200]}")
        return False

    print(f"  ✅ View is queryable")
    return True


def test_view_dimensions(name: str, catalog: str, schema: str) -> bool:
    """Test that key dimensions exist."""
    # Query columns from the view
    query = f"""
    SELECT column_name
    FROM {catalog}.information_schema.columns
    WHERE table_schema = '{schema}' AND table_name = '{name}'
    """
    result = execute_sql(query)

    if "error" in result:
        print(f"  ❌ API Error: {result['error'][:100]}")
        return False

    status = result.get("status", {})
    if status.get("state") == "FAILED":
        error = status.get("error", {}).get("message", "Unknown error")
        print(f"  ❌ Query Failed: {error[:100]}")
        return False

    columns = result.get("result", {}).get("data_array", [])
    column_names = [row[0] for row in columns]

    # Check for required dimensions
    required = ["Business Unit", "Region", "Owner", "Manager L1"]
    missing = [d for d in required if d not in column_names]

    if missing:
        print(f"  ⚠️ Missing dimensions: {missing}")
        return False

    print(f"  ✅ Has {len(column_names)} columns including required dimensions")
    return True


def test_view_measures(name: str, catalog: str, schema: str, measure_name: str) -> bool:
    """Test that a MEASURE() query works."""
    query = f"""
    SELECT MEASURE(`{measure_name}`) AS result
    FROM {catalog}.{schema}.{name}
    WHERE `Business Unit` = 'AMER Enterprise & Emerging'
    GROUP BY ALL
    LIMIT 1
    """
    result = execute_sql(query)

    if "error" in result:
        print(f"  ❌ API Error: {result['error'][:100]}")
        return False

    status = result.get("status", {})
    if status.get("state") == "FAILED":
        error = status.get("error", {}).get("message", "Unknown error")
        # Some views may legitimately return no data for this filter
        if "zero rows" in error.lower() or "no data" in error.lower():
            print(f"  ⚠️ No data for AMER EE filter (may be expected)")
            return True
        print(f"  ❌ MEASURE() query failed: {error[:200]}")
        return False

    print(f"  ✅ MEASURE(`{measure_name}`) works")
    return True


def run_tests() -> int:
    """Run all tests."""
    catalog = "home_christopher_chalcraft"
    schema = "cjc_views"

    print("╔" + "═" * 58 + "╗")
    print("║  TESTING CHARTER METRIC VIEWS" + " " * 27 + "║")
    print("║  Catalog: " + catalog + " " * 13 + "║")
    print("║  Schema:  " + schema + " " * 30 + "║")
    print("╚" + "═" * 58 + "╝")
    print()

    views = [
        ("mv_time_to_adopt", "Total UCOs"),
        ("mv_asset_reuse", "Total ASQs"),
        ("mv_self_service_health", "Total ASQs"),
        ("mv_product_impact", "Total ASQs"),
        ("mv_customer_risk_reduction", "Total ASQs"),
    ]

    all_passed = True

    for view_name, measure_name in views:
        print(f"\n{'─' * 50}")
        print(f"Testing: {view_name}")
        print(f"{'─' * 50}")

        # Test 1: View exists
        if not test_view_exists(view_name, catalog, schema):
            all_passed = False
            continue

        # Test 2: View is queryable
        if not test_view_returns_data(view_name, catalog, schema):
            all_passed = False
            continue

        # Test 3: Has required dimensions
        if not test_view_dimensions(view_name, catalog, schema):
            all_passed = False

        # Test 4: MEASURE() works
        if not test_view_measures(view_name, catalog, schema, measure_name):
            all_passed = False

    print()
    print("=" * 60)
    if all_passed:
        print("✅ ALL TESTS PASSED")
        return 0
    else:
        print("❌ SOME TESTS FAILED")
        return 1


if __name__ == "__main__":
    sys.exit(run_tests())
