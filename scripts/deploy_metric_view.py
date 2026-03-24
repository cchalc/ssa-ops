#!/usr/bin/env python3
"""Deploy metric views to Databricks using the SQL Statements API."""

import json
import subprocess
import sys

def execute_sql(statement: str, profile: str = "logfood", warehouse_id: str = "927ac096f9833442",
                catalog: str = None, schema: str = None) -> dict:
    """Execute SQL via Databricks API."""
    payload = {
        "warehouse_id": warehouse_id,
        "statement": statement,
        "wait_timeout": "50s"
    }
    if catalog:
        payload["catalog"] = catalog
    if schema:
        payload["schema"] = schema

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

def deploy_metric_view(name: str, yaml_content: str, catalog: str, schema: str,
                       profile: str = "logfood", warehouse_id: str = "927ac096f9833442") -> bool:
    """Deploy a single metric view."""
    statement = f"""CREATE OR REPLACE VIEW {catalog}.{schema}.{name}
WITH METRICS
LANGUAGE YAML
AS $$
{yaml_content}
$$"""

    print(f"Deploying {name}...")
    result = execute_sql(statement, profile, warehouse_id)

    if "error" in result:
        print(f"  ❌ Error: {result['error']}")
        return False

    status = result.get("status", {})
    if status.get("state") == "SUCCEEDED":
        print(f"  ✅ Success")
        return True
    elif status.get("state") == "FAILED":
        error = status.get("error", {}).get("message", "Unknown error")
        print(f"  ❌ Failed: {error[:300]}")
        return False
    else:
        print(f"  ⚠️ Status: {status.get('state')}")
        return False

# ============================================================================
# METRIC VIEW DEFINITIONS (YAML format)
# ============================================================================

METRIC_VIEWS = {
    # Charter Metric #4: Time-to-Adopt
    "mv_time_to_adopt": """
version: 1.1
comment: "UCO stage acceleration metrics. Charter Metric #4: Time-to-Adopt. Measures U3 to U4 transition time."
source: main.gtm_silver.approval_request_detail
filter: snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)

joins:
  - name: hier
    source: main.gtm_silver.individual_hierarchy_salesforce
    on: source.owner_user_id = hier.user_id
  - name: uco
    source: main.gtm_silver.use_case_detail
    on: source.account_id = uco.account_id AND uco.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.use_case_detail) AND uco.is_active_ind = true

dimensions:
  - name: Business Unit
    expr: source.business_unit
    comment: "BU: AMER Enterprise and Emerging, AMER Industries, EMEA, APJ"
  - name: Region
    expr: source.region_level_1
    comment: "Region: CAN, RCT, FINS, etc."
  - name: Owner
    expr: source.owner_user_name
    comment: "SSA owner name"
  - name: Manager L1
    expr: hier.line_manager_name
    comment: "Direct manager"
  - name: Account
    expr: source.account_name
    comment: "Customer account name"
  - name: Current Stage
    expr: uco.stage
    comment: "Current UCO stage"
  - name: Product Category
    expr: uco.use_case_product
    comment: "Product category"
  - name: Adoption Speed
    expr: |
      CASE
        WHEN uco.u4_date_sfdc_original IS NULL THEN 'Not Yet Adopted'
        WHEN uco.u3_date_sfdc_original IS NULL THEN 'No U3 Date'
        WHEN DATEDIFF(uco.u4_date_sfdc_original, uco.u3_date_sfdc_original) <= 14 THEN 'Fast (14 days or less)'
        WHEN DATEDIFF(uco.u4_date_sfdc_original, uco.u3_date_sfdc_original) <= 30 THEN 'Normal (15-30 days)'
        WHEN DATEDIFF(uco.u4_date_sfdc_original, uco.u3_date_sfdc_original) <= 60 THEN 'Slow (31-60 days)'
        ELSE 'Very Slow (over 60 days)'
      END
    comment: "Adoption speed tier based on U3 to U4 days"
  - name: Fiscal Year
    expr: CASE WHEN MONTH(source.created_date) = 1 THEN YEAR(source.created_date) ELSE YEAR(source.created_date) + 1 END
    comment: "Fiscal year (ends Jan 31)"

measures:
  - name: Total UCOs
    expr: COUNT(DISTINCT uco.usecase_id)
    comment: "Total UCOs linked to ASQs"
  - name: Adopted UCOs
    expr: COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.u4_date_sfdc_original IS NOT NULL)
    comment: "UCOs that reached U4 (tech win)"
  - name: Production UCOs
    expr: COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.stage IN ('U5', 'U6'))
    comment: "UCOs at production (U5+)"
  - name: Avg Days to Adopt
    expr: AVG(DATEDIFF(uco.u4_date_sfdc_original, uco.u3_date_sfdc_original))
    comment: "Average days from U3 to U4"
  - name: Adoption Rate
    expr: COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.u4_date_sfdc_original IS NOT NULL) * 1.0 / NULLIF(COUNT(DISTINCT uco.usecase_id), 0)
    comment: "Percentage of UCOs that reached tech win (U4)"
""",

    # Charter Metric #5: Asset Reuse Rate
    "mv_asset_reuse": """
version: 1.1
comment: "Asset reuse and pattern application metrics. Charter Metric #5."
source: main.gtm_silver.approval_request_detail
filter: snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail) AND status NOT IN ('Rejected', 'Cancelled')

joins:
  - name: hier
    source: main.gtm_silver.individual_hierarchy_salesforce
    on: source.owner_user_id = hier.user_id

dimensions:
  - name: Business Unit
    expr: source.business_unit
  - name: Region
    expr: source.region_level_1
  - name: Owner
    expr: source.owner_user_name
  - name: Manager L1
    expr: hier.line_manager_name
  - name: Specialization
    expr: source.technical_specialization
  - name: Support Type
    expr: source.support_type
  - name: Account
    expr: source.account_name
  - name: Fiscal Year
    expr: CASE WHEN MONTH(source.created_date) = 1 THEN YEAR(source.created_date) ELSE YEAR(source.created_date) + 1 END

measures:
  - name: Total ASQs
    expr: COUNT(DISTINCT source.approval_request_id)
  - name: Unique Accounts
    expr: COUNT(DISTINCT source.account_id)
  - name: Pattern Count
    expr: COUNT(DISTINCT CONCAT(source.owner_user_id, '|', source.technical_specialization))
    comment: "Unique SSA + specialization patterns"
  - name: Avg ASQs per Pattern
    expr: COUNT(DISTINCT source.approval_request_id) * 1.0 / NULLIF(COUNT(DISTINCT CONCAT(source.owner_user_id, '|', source.technical_specialization)), 0)
    comment: "Average ASQs per pattern (higher = more reuse)"
  - name: Avg Accounts per Pattern
    expr: COUNT(DISTINCT source.account_id) * 1.0 / NULLIF(COUNT(DISTINCT CONCAT(source.owner_user_id, '|', source.technical_specialization)), 0)
    comment: "Average accounts per pattern"
""",

    # Charter Metric #6: Self-Service Health (ASQ Deflection Proxy)
    "mv_self_service_health": """
version: 1.1
comment: "Self-service health proxy for ASQ deflection. Charter Metric #6. Tracks account engagement frequency."
source: main.gtm_silver.approval_request_detail
filter: snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail) AND status NOT IN ('Rejected', 'Cancelled')

joins:
  - name: hier
    source: main.gtm_silver.individual_hierarchy_salesforce
    on: source.owner_user_id = hier.user_id

dimensions:
  - name: Business Unit
    expr: source.business_unit
  - name: Region
    expr: source.region_level_1
  - name: Owner
    expr: source.owner_user_name
  - name: Manager L1
    expr: hier.line_manager_name
  - name: Account
    expr: source.account_name
  - name: Fiscal Year
    expr: CASE WHEN MONTH(source.created_date) = 1 THEN YEAR(source.created_date) ELSE YEAR(source.created_date) + 1 END

measures:
  - name: Total ASQs
    expr: COUNT(DISTINCT source.approval_request_id)
  - name: Total Accounts
    expr: COUNT(DISTINCT source.account_id)
  - name: ASQs per Account
    expr: COUNT(DISTINCT source.approval_request_id) * 1.0 / NULLIF(COUNT(DISTINCT source.account_id), 0)
    comment: "ASQs per account (lower = better enablement)"
  - name: Total Effort Days
    expr: SUM(source.actual_effort_in_days)
""",

    # Charter Metric #7: Product Impact
    "mv_product_impact": """
version: 1.1
comment: "Product adoption impact from SSA engagements. Charter Metric #7: Product Impact."
source: main.gtm_silver.approval_request_detail
filter: snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)

joins:
  - name: hier
    source: main.gtm_silver.individual_hierarchy_salesforce
    on: source.owner_user_id = hier.user_id
  - name: ao
    source: main.gtm_gold.account_obt
    on: source.account_id = ao.account_id AND ao.fiscal_year_quarter = "FY'26 Q4"
  - name: uco
    source: main.gtm_silver.use_case_detail
    on: source.account_id = uco.account_id AND uco.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.use_case_detail) AND uco.is_active_ind = true

dimensions:
  - name: Business Unit
    expr: source.business_unit
  - name: Region
    expr: source.region_level_1
  - name: Owner
    expr: source.owner_user_name
  - name: Manager L1
    expr: hier.line_manager_name
  - name: Account
    expr: source.account_name
  - name: Account Segment
    expr: ao.account_segment
  - name: UCO Product
    expr: uco.use_case_product
  - name: UCO Stage
    expr: uco.stage
  - name: Product Group
    expr: |
      CASE
        WHEN uco.use_case_product LIKE '%Lakeflow%' OR uco.use_case_product LIKE '%DLT%' THEN 'Lakeflow'
        WHEN uco.use_case_product LIKE '%Serverless%' OR uco.use_case_product LIKE '%SQL Warehouse%' THEN 'Serverless SQL'
        WHEN uco.use_case_product LIKE '%Model Serving%' OR uco.use_case_product LIKE '%ML%' THEN 'AI/ML'
        WHEN uco.use_case_product LIKE '%Unity Catalog%' THEN 'Unity Catalog'
        ELSE 'Other'
      END
  - name: Has Lakeflow
    expr: CASE WHEN COALESCE(ao.dlt_dbu_dollars_qtd, 0) > 0 THEN 'Yes' ELSE 'No' END
  - name: Has Serverless SQL
    expr: CASE WHEN COALESCE(ao.dbsql_serverless_dbu_dollars_qtd, 0) > 0 THEN 'Yes' ELSE 'No' END
  - name: Fiscal Year
    expr: CASE WHEN MONTH(source.created_date) = 1 THEN YEAR(source.created_date) ELSE YEAR(source.created_date) + 1 END

measures:
  - name: Total ASQs
    expr: COUNT(DISTINCT source.approval_request_id)
  - name: Engaged Accounts
    expr: COUNT(DISTINCT source.account_id)
  - name: Total UCOs
    expr: COUNT(DISTINCT uco.usecase_id)
  - name: Production UCOs
    expr: COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.stage IN ('U5', 'U6'))
  - name: Lakeflow UCOs
    expr: COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.use_case_product LIKE '%Lakeflow%' OR uco.use_case_product LIKE '%DLT%')
  - name: Serverless UCOs
    expr: COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.use_case_product LIKE '%Serverless%' OR uco.use_case_product LIKE '%SQL Warehouse%')
  - name: Lakeflow Influenced Accounts
    expr: COUNT(DISTINCT source.account_id) FILTER (WHERE COALESCE(ao.dlt_dbu_dollars_qtd, 0) > 0)
  - name: Serverless Influenced Accounts
    expr: COUNT(DISTINCT source.account_id) FILTER (WHERE COALESCE(ao.dbsql_serverless_dbu_dollars_qtd, 0) > 0)
  - name: Lakeflow Adoption Rate
    expr: COUNT(DISTINCT source.account_id) FILTER (WHERE COALESCE(ao.dlt_dbu_dollars_qtd, 0) > 0) * 1.0 / NULLIF(COUNT(DISTINCT source.account_id), 0)
  - name: Serverless Adoption Rate
    expr: COUNT(DISTINCT source.account_id) FILTER (WHERE COALESCE(ao.dbsql_serverless_dbu_dollars_qtd, 0) > 0) * 1.0 / NULLIF(COUNT(DISTINCT source.account_id), 0)
  - name: Lakeflow DBU QTD
    expr: SUM(ao.dlt_dbu_dollars_qtd)
  - name: Serverless DBU QTD
    expr: SUM(ao.dbsql_serverless_dbu_dollars_qtd)
  - name: Total Effort Days
    expr: SUM(source.actual_effort_in_days)
""",

    # Charter Metric #8: Customer Risk Reduction
    "mv_customer_risk_reduction": """
version: 1.1
comment: "Customer risk reduction metrics. Charter Metric #8. Tracks competitive wins and mitigation."
source: main.gtm_silver.approval_request_detail
filter: snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)

joins:
  - name: hier
    source: main.gtm_silver.individual_hierarchy_salesforce
    on: source.owner_user_id = hier.user_id
  - name: uco
    source: main.gtm_silver.use_case_detail
    on: source.account_id = uco.account_id AND uco.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.use_case_detail) AND uco.is_active_ind = true

dimensions:
  - name: Business Unit
    expr: source.business_unit
  - name: Region
    expr: source.region_level_1
  - name: Owner
    expr: source.owner_user_name
  - name: Manager L1
    expr: hier.line_manager_name
  - name: Account
    expr: source.account_name
  - name: Risk Context
    expr: |
      CASE
        WHEN LOWER(source.support_type) LIKE '%migration%' THEN 'Migration'
        WHEN LOWER(source.request_description) LIKE '%churn%' THEN 'Churn Risk'
        WHEN LOWER(source.request_description) LIKE '%mitigation%' THEN 'Mitigation'
        WHEN LOWER(source.request_description) LIKE '%competitive%' THEN 'Competitive'
        WHEN LOWER(source.request_description) LIKE '%snowflake%' THEN 'Snowflake Compete'
        WHEN LOWER(source.request_description) LIKE '%fabric%' THEN 'Microsoft Compete'
        WHEN uco.competitor_status = 'Active' THEN 'Active Compete'
        WHEN uco.primary_competitor IS NOT NULL THEN 'Has Competitor'
        ELSE 'Standard'
      END
  - name: Competitor Category
    expr: |
      CASE
        WHEN uco.primary_competitor LIKE '%Microsoft%' OR uco.primary_competitor LIKE '%Fabric%' THEN 'Microsoft'
        WHEN uco.primary_competitor LIKE '%Snowflake%' THEN 'Snowflake'
        WHEN uco.primary_competitor LIKE '%AWS%' OR uco.primary_competitor LIKE '%Redshift%' THEN 'AWS'
        WHEN uco.primary_competitor LIKE '%Google%' OR uco.primary_competitor LIKE '%BigQuery%' THEN 'Google Cloud'
        WHEN uco.primary_competitor IS NOT NULL THEN 'Other Competitor'
        ELSE 'No Competitor'
      END
  - name: Primary Competitor
    expr: uco.primary_competitor
  - name: Competitor Status
    expr: uco.competitor_status
  - name: UCO Stage
    expr: uco.stage
  - name: Fiscal Year
    expr: CASE WHEN MONTH(source.created_date) = 1 THEN YEAR(source.created_date) ELSE YEAR(source.created_date) + 1 END

measures:
  - name: Total ASQs
    expr: COUNT(DISTINCT source.approval_request_id)
  - name: Risk-Related ASQs
    expr: |
      COUNT(DISTINCT source.approval_request_id) FILTER (WHERE
        LOWER(source.support_type) LIKE '%migration%'
        OR LOWER(source.request_description) LIKE '%churn%'
        OR LOWER(source.request_description) LIKE '%mitigation%'
        OR LOWER(source.request_description) LIKE '%competitive%'
        OR uco.competitor_status = 'Active')
  - name: Migration ASQs
    expr: COUNT(DISTINCT source.approval_request_id) FILTER (WHERE LOWER(source.support_type) LIKE '%migration%' OR LOWER(source.request_description) LIKE '%migration%')
  - name: Total UCOs
    expr: COUNT(DISTINCT uco.usecase_id)
  - name: Competitive UCOs
    expr: COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.primary_competitor IS NOT NULL)
  - name: Active Compete UCOs
    expr: COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.competitor_status = 'Active')
  - name: Competitive Wins
    expr: COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.primary_competitor IS NOT NULL AND uco.stage IN ('U5', 'U6'))
    comment: "Competitive wins (UCO at production with competitor)"
  - name: Competitive Losses
    expr: COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.primary_competitor IS NOT NULL AND uco.stage = 'Lost')
  - name: Competitive Win Rate
    expr: |
      COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.primary_competitor IS NOT NULL AND uco.stage IN ('U5', 'U6')) * 1.0
      / NULLIF(COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.primary_competitor IS NOT NULL AND uco.stage IN ('U5', 'U6', 'Lost')), 0)
  - name: Microsoft Displacement Wins
    expr: COUNT(DISTINCT uco.usecase_id) FILTER (WHERE (uco.primary_competitor LIKE '%Microsoft%' OR uco.primary_competitor LIKE '%Fabric%' OR uco.primary_competitor LIKE '%Synapse%') AND uco.stage IN ('U5', 'U6'))
  - name: Snowflake Displacement Wins
    expr: COUNT(DISTINCT uco.usecase_id) FILTER (WHERE uco.primary_competitor LIKE '%Snowflake%' AND uco.stage IN ('U5', 'U6'))
  - name: Total Effort Days
    expr: SUM(source.actual_effort_in_days)
""",
}

def main():
    catalog = "home_christopher_chalcraft"
    schema = "cjc_views"

    print(f"╔{'═' * 58}╗")
    print(f"║  DEPLOYING CHARTER METRIC VIEWS TO LOGFOOD{' ' * 14}║")
    print(f"║  Target: {catalog}.{schema}{' ' * 3}║")
    print(f"╚{'═' * 58}╝")
    print()

    success_count = 0
    for name, yaml_content in METRIC_VIEWS.items():
        if deploy_metric_view(name, yaml_content, catalog, schema):
            success_count += 1

    print()
    print("=" * 60)
    print(f"✅ Deployed {success_count}/{len(METRIC_VIEWS)} metric views")

    if success_count == len(METRIC_VIEWS):
        print("\nAll metric views deployed successfully!")
        return 0
    else:
        print(f"\n⚠️ {len(METRIC_VIEWS) - success_count} metric views failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())
