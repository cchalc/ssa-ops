"""
Sync Delta tables to Lakebase PostgreSQL via JDBC.

This job reads from Delta tables in cjc_aws_workspace_catalog.ssa_ops_dev
and writes to Lakebase PostgreSQL tables in the dashboard schema.

Run via: Databricks Job with Python task

NOTE: This script is designed to run on Databricks Runtime where PySpark
is pre-installed. It will not run locally without installing pyspark.
For local testing, use: uv sync --group databricks
"""

from pyspark.sql import SparkSession
import os

# Configuration
DELTA_CATALOG = "cjc_aws_workspace_catalog"
DELTA_SCHEMA = "ssa_ops_dev"
LAKEBASE_JDBC_URL = os.environ.get(
    "LAKEBASE_JDBC_URL",
    "jdbc:postgresql://ssa-ops-dev.lakebase.databricks.com:5432/ssa_ops_dev"
)
LAKEBASE_SCHEMA = "dashboard"

# Table mappings: (source_table, target_table, mode)
TABLE_MAPPINGS = [
    ("team_summary", "team_summary", "overwrite"),
    ("asq_completed_metrics", "asq_completed_metrics", "overwrite"),
    ("asq_sla_metrics", "asq_sla_metrics", "overwrite"),
    ("asq_effort_accuracy", "asq_effort_accuracy", "overwrite"),
    ("asq_reengagement", "asq_reengagement", "overwrite"),
    ("ssa_performance", "ssa_performance", "overwrite"),
]


def get_spark() -> SparkSession:
    """Get or create Spark session."""
    return SparkSession.builder.getOrCreate()


def sync_table(
    spark: SparkSession,
    source_table: str,
    target_table: str,
    mode: str = "overwrite"
) -> dict:
    """
    Sync a single Delta table to Lakebase.

    Returns dict with sync stats.
    """
    source_path = f"{DELTA_CATALOG}.{DELTA_SCHEMA}.{source_table}"
    target_path = f"{LAKEBASE_SCHEMA}.{target_table}"

    print(f"Syncing {source_path} -> {target_path}")

    # Read from Delta
    df = spark.read.table(source_path)
    row_count = df.count()

    # Write to Lakebase via JDBC
    jdbc_props = {
        "driver": "org.postgresql.Driver",
        "user": os.environ.get("LAKEBASE_USER", ""),
        "password": os.environ.get("LAKEBASE_PASSWORD", ""),
        "ssl": "true",
        "sslmode": "require",
    }

    df.write \
        .format("jdbc") \
        .option("url", LAKEBASE_JDBC_URL) \
        .option("dbtable", target_path) \
        .options(**jdbc_props) \
        .mode(mode) \
        .save()

    print(f"  Synced {row_count} rows")

    return {
        "source": source_path,
        "target": target_path,
        "rows": row_count,
        "mode": mode,
    }


def main():
    """Run full sync pipeline."""
    spark = get_spark()
    results = []

    print("=" * 60)
    print("SSA Dashboard - Delta to Lakebase Sync")
    print("=" * 60)

    for source, target, mode in TABLE_MAPPINGS:
        try:
            result = sync_table(spark, source, target, mode)
            results.append({"status": "success", **result})
        except Exception as e:
            print(f"  ERROR: {e}")
            results.append({
                "status": "error",
                "source": f"{DELTA_CATALOG}.{DELTA_SCHEMA}.{source}",
                "error": str(e),
            })

    print("=" * 60)
    print("Sync Summary:")
    for r in results:
        status = r.get("status", "unknown")
        source = r.get("source", "")
        if status == "success":
            print(f"  ✓ {source}: {r.get('rows', 0)} rows")
        else:
            print(f"  ✗ {source}: {r.get('error', 'Unknown error')}")

    # Fail job if any errors
    errors = [r for r in results if r.get("status") != "success"]
    if errors:
        raise RuntimeError(f"{len(errors)} table(s) failed to sync")


if __name__ == "__main__":
    main()
