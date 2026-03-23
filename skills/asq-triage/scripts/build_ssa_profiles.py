#!/usr/bin/env python3
"""
Build SSA profiles from historical completed ASQs.

Analyzes:
- Technologies worked with
- Support types handled
- Account relationships
- Specialization patterns
"""
import json
import sys
from datetime import datetime
from collections import defaultdict
from pathlib import Path
from databricks.sdk import WorkspaceClient

WAREHOUSE_ID = "927ac096f9833442"

# SSAs to profile (Chris Chalcraft's direct reports)
TARGET_SSAS = [
    "Réda Khouani",
    "Volodymyr Vragov",
    "Scott McKean",
    "Harsha Pasala",
    "Mathieu Pelletier",
    "Allan Cao",
]

w = WorkspaceClient(profile="logfood")

print("=" * 80)
print("SSA PROFILE BUILDER")
print("=" * 80)

# Query completed ASQs for target SSAs (last 2 years)
print("\nQuerying completed ASQs for target SSAs...", file=sys.stderr)

ssa_list = ", ".join(f"'{ssa}'" for ssa in TARGET_SSAS)

query = f"""
SELECT
    owner_user_name AS ssa_name,
    owner_user_id AS ssa_id,
    approval_request_name AS asq_number,
    account_name,
    account_id,
    status,
    support_type,
    technical_specialization,
    Title AS asq_title,
    LEFT(request_description, 1000) AS description,
    LEFT(request_status_notes, 500) AS notes,
    DATE(created_date) AS created_date,
    DATE(target_end_date) AS end_date,
    DATEDIFF(target_end_date, created_date) AS duration_days,
    region_level_1 AS region
FROM main.gtm_silver.approval_request_detail
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
    AND owner_user_name IN ({ssa_list})
    AND created_date >= CURRENT_DATE - INTERVAL 730 DAYS
ORDER BY owner_user_name, created_date DESC
"""

result = w.statement_execution.execute_statement(
    warehouse_id=WAREHOUSE_ID,
    statement=query,
    wait_timeout="50s",
)

if result.status.state.value != "SUCCEEDED":
    print(f"Query failed: {result.status}", file=sys.stderr)
    sys.exit(1)

if not result.result or not result.result.data_array:
    print("No data found", file=sys.stderr)
    sys.exit(1)

columns = [col.name for col in result.manifest.schema.columns]
asqs = [dict(zip(columns, row)) for row in result.result.data_array]

print(f"Found {len(asqs)} ASQs for {len(TARGET_SSAS)} SSAs", file=sys.stderr)

# Build profiles
profiles = {}

for ssa in TARGET_SSAS:
    ssa_asqs = [a for a in asqs if a["ssa_name"] == ssa]

    profile = {
        "ssa_name": ssa,
        "ssa_id": ssa_asqs[0]["ssa_id"] if ssa_asqs else None,
        "total_asqs": len(ssa_asqs),
        "completed": len([a for a in ssa_asqs if a["status"] == "Completed"]),
        "in_progress": len([a for a in ssa_asqs if a["status"] == "In Progress"]),
        "accounts": defaultdict(int),
        "support_types": defaultdict(int),
        "specializations": defaultdict(int),
        "technologies": defaultdict(int),
        "avg_duration_days": 0,
        "recent_accounts": [],
    }

    # Technology keywords to detect
    tech_keywords = {
        "ML/AI": ["machine learning", "ml", "ai", "model", "mlflow", "feature store", "training", "inference"],
        "GenAI": ["genai", "llm", "gpt", "langchain", "rag", "vector", "embedding", "agent", "chatbot"],
        "Data Engineering": ["pipeline", "etl", "ingestion", "delta", "spark", "streaming", "cdc", "lakeflow"],
        "SQL/Analytics": ["sql", "warehouse", "bi", "dashboard", "analytics", "reporting", "dbsql"],
        "Platform/Admin": ["workspace", "cluster", "unity catalog", "governance", "security", "admin", "cicd", "ci/cd"],
        "Migration": ["migration", "migrate", "synapse", "snowflake", "redshift", "fabric"],
        "SAP": ["sap", "s4hana", "hana"],
        "Geospatial": ["geospatial", "geo", "lidar", "gis", "spatial"],
    }

    durations = []

    for asq in ssa_asqs:
        # Count accounts
        if asq["account_name"]:
            profile["accounts"][asq["account_name"]] += 1

        # Count support types
        if asq["support_type"]:
            profile["support_types"][asq["support_type"]] += 1

        # Count specializations
        if asq["technical_specialization"]:
            profile["specializations"][asq["technical_specialization"]] += 1

        # Detect technologies from description
        text = ((asq["description"] or "") + " " + (asq["asq_title"] or "") + " " + (asq["notes"] or "")).lower()
        for tech, keywords in tech_keywords.items():
            if any(kw in text for kw in keywords):
                profile["technologies"][tech] += 1

        # Track duration
        if asq["duration_days"] and int(asq["duration_days"]) > 0:
            durations.append(int(asq["duration_days"]))

    # Calculate averages
    if durations:
        profile["avg_duration_days"] = round(sum(durations) / len(durations), 1)

    # Get top accounts (for continuity)
    sorted_accounts = sorted(profile["accounts"].items(), key=lambda x: -x[1])
    profile["top_accounts"] = sorted_accounts[:10]

    # Get recent accounts (last 90 days worth of ASQs)
    recent = [a["account_name"] for a in ssa_asqs[:15] if a["account_name"]]
    profile["recent_accounts"] = list(dict.fromkeys(recent))[:10]  # Unique, preserve order

    # Convert defaultdicts to regular dicts for JSON
    profile["accounts"] = dict(profile["accounts"])
    profile["support_types"] = dict(profile["support_types"])
    profile["specializations"] = dict(profile["specializations"])
    profile["technologies"] = dict(profile["technologies"])

    profiles[ssa] = profile

# Output profiles
print("\n")
for ssa, profile in profiles.items():
    print("=" * 80)
    print(f"SSA PROFILE: {ssa}")
    print("=" * 80)

    print(f"\n## Summary")
    print(f"- Total ASQs (2 years): {profile['total_asqs']}")
    print(f"- Completed: {profile['completed']}")
    print(f"- In Progress: {profile['in_progress']}")
    print(f"- Avg Duration: {profile['avg_duration_days']} days")

    print(f"\n## Technology Expertise (by ASQ count)")
    sorted_tech = sorted(profile["technologies"].items(), key=lambda x: -x[1])
    for tech, count in sorted_tech:
        pct = round(100 * count / profile["total_asqs"], 1)
        bar = "█" * int(pct / 5)
        print(f"  {tech:20} {count:3} ({pct:5.1f}%) {bar}")

    print(f"\n## Support Types")
    sorted_types = sorted(profile["support_types"].items(), key=lambda x: -x[1])
    for stype, count in sorted_types[:8]:
        print(f"  {stype:40} {count:3}")

    print(f"\n## Specializations")
    sorted_specs = sorted(profile["specializations"].items(), key=lambda x: -x[1])
    for spec, count in sorted_specs:
        print(f"  {spec:40} {count:3}")

    print(f"\n## Top Accounts (Relationship Continuity)")
    for account, count in profile["top_accounts"]:
        print(f"  {account:45} {count:3} ASQs")

    print(f"\n## Recent Accounts (Last ~90 days)")
    for account in profile["recent_accounts"]:
        print(f"  - {account}")

    print()

# Save profiles to JSON for future use
output_path = Path(__file__).parent.parent / "data" / "ssa_profiles.json"
output_path.parent.mkdir(exist_ok=True)

with open(output_path, "w") as f:
    json.dump(profiles, f, indent=2, default=str)

print(f"\nProfiles saved to: {output_path}")

# Also create a SQL view definition
view_sql = """
-- SSA Profile View for ssa-ops
-- Run this in Databricks to create a reusable view

CREATE OR REPLACE VIEW main.field_ssa_lakehouse.ssa_profiles AS
WITH ssa_asqs AS (
    SELECT
        owner_user_name AS ssa_name,
        owner_user_id AS ssa_id,
        account_name,
        account_id,
        support_type,
        technical_specialization,
        status,
        created_date,
        target_end_date,
        request_description,
        Title
    FROM main.gtm_silver.approval_request_detail
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
        AND created_date >= CURRENT_DATE - INTERVAL 730 DAYS
        AND owner_user_name IS NOT NULL
),
tech_detection AS (
    SELECT
        ssa_name,
        ssa_id,
        CASE WHEN LOWER(request_description) LIKE '%machine learning%' OR LOWER(request_description) LIKE '%mlflow%' OR LOWER(request_description) LIKE '%model%' THEN 1 ELSE 0 END AS has_ml,
        CASE WHEN LOWER(request_description) LIKE '%genai%' OR LOWER(request_description) LIKE '%llm%' OR LOWER(request_description) LIKE '%rag%' THEN 1 ELSE 0 END AS has_genai,
        CASE WHEN LOWER(request_description) LIKE '%pipeline%' OR LOWER(request_description) LIKE '%etl%' OR LOWER(request_description) LIKE '%ingestion%' THEN 1 ELSE 0 END AS has_data_eng,
        CASE WHEN LOWER(request_description) LIKE '%migration%' OR LOWER(request_description) LIKE '%synapse%' OR LOWER(request_description) LIKE '%snowflake%' THEN 1 ELSE 0 END AS has_migration,
        CASE WHEN LOWER(request_description) LIKE '%sap%' OR LOWER(request_description) LIKE '%s4hana%' THEN 1 ELSE 0 END AS has_sap
    FROM ssa_asqs
)
SELECT
    a.ssa_name,
    a.ssa_id,
    COUNT(*) AS total_asqs,
    COUNT(CASE WHEN a.status = 'Completed' THEN 1 END) AS completed_asqs,
    COUNT(DISTINCT a.account_id) AS unique_accounts,
    ROUND(AVG(DATEDIFF(a.target_end_date, a.created_date)), 1) AS avg_duration_days,
    SUM(t.has_ml) AS ml_asqs,
    SUM(t.has_genai) AS genai_asqs,
    SUM(t.has_data_eng) AS data_eng_asqs,
    SUM(t.has_migration) AS migration_asqs,
    SUM(t.has_sap) AS sap_asqs,
    COLLECT_SET(a.account_name) AS account_list
FROM ssa_asqs a
JOIN tech_detection t ON a.ssa_name = t.ssa_name AND a.ssa_id = t.ssa_id
GROUP BY a.ssa_name, a.ssa_id
ORDER BY total_asqs DESC;
"""

view_path = Path(__file__).parent.parent / "sql" / "ssa_profiles_view.sql"
view_path.parent.mkdir(exist_ok=True)

with open(view_path, "w") as f:
    f.write(view_sql)

print(f"SQL view definition saved to: {view_path}")
