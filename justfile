# SSA-Ops Deployment Orchestration
# Run `just` to see available recipes

set shell := ["fish", "-c"]

# Default recipe - show help
default:
    @just --list

# === DEPLOYMENT ===

# Deploy all (both workspaces)
deploy-all: deploy-logfood deploy-dev
    @echo "✓ All deployments complete"

# Deploy export job to logfood workspace
deploy-logfood:
    @echo "Deploying export job to logfood..."
    cd bundles/logfood && databricks bundle deploy
    @echo "✓ Logfood deploy complete"

# Deploy to fevm-cjc dev environment
deploy-dev:
    @echo "Deploying to fevm-cjc (dev)..."
    databricks bundle deploy -t dev
    @echo "✓ fevm-cjc deploy complete"

# Deploy to fevm-cjc staging
deploy-staging:
    @echo "Deploying to fevm-cjc (staging)..."
    databricks bundle deploy -t staging
    @echo "✓ Deploy complete"

# Deploy to fevm-cjc production
deploy-prod:
    @echo "Deploying to fevm-cjc (prod)..."
    databricks bundle deploy -t prod
    @echo "✓ Deploy complete"

# Deploy SQL views to logfood (manual - opens SQL editor instructions)
deploy-views:
    @echo "SQL Views must be deployed manually to logfood workspace."
    @echo ""
    @echo "1. Open: https://adb-2548836972759138.18.azuredatabricks.net/sql/editor"
    @echo "2. Select warehouse: Shared SQL Endpoint - Stable (927ac096f9833442)"
    @echo "3. Run: sql/deploy_views.sql"

# === VALIDATION ===

# Validate bundle configuration
validate:
    databricks bundle validate -t dev

# Run data validation tests (all tiers)
validate-data:
    ./scripts/run-validation.sh all

# Validate logfood views only
validate-logfood:
    ./scripts/run-validation.sh logfood

# Validate Delta tables only
validate-delta:
    ./scripts/run-validation.sh delta

# === SYNC JOBS ===

# Run export from logfood to Delta (runs on logfood workspace)
sync-to-delta:
    @echo "Triggering export to Delta job on logfood..."
    cd bundles/logfood && databricks bundle run ssa_dashboard_export_to_delta --no-wait || { \
        echo ""; \
        echo "⚠ Job not available. Deploy first: just deploy-logfood"; \
        echo "  Or run manually on logfood SQL Editor:"; \
        echo "  sql/sync/01_export_to_delta.sql"; \
    }

# Run sync from Delta to Lakebase (runs on fevm-cjc)
sync-to-lakebase:
    @echo "Triggering sync to Lakebase job on fevm-cjc..."
    databricks bundle run -t dev ssa_dashboard_sync_to_lakebase --no-wait || { \
        echo "⚠ Job failed to start. Deploy first: just deploy-dev"; \
    }

# Full sync pipeline: logfood → Delta → Lakebase
sync-all:
    @echo "Starting full sync pipeline..."
    @echo ""
    @echo "Step 1: Export logfood views → Delta tables"
    just sync-to-delta
    @echo ""
    @echo "Waiting 60s for Delta export to complete..."
    @sleep 60
    @echo ""
    @echo "Step 2: Sync Delta → Lakebase"
    just sync-to-lakebase
    @echo ""
    @echo "✓ Sync jobs triggered"
    @echo "  Monitor at: https://fevm-cjc-aws-workspace.cloud.databricks.com/jobs"
    @echo "  Monitor at: https://adb-2548836972759138.18.azuredatabricks.net/jobs"

# === JOBS ===

# List all deployed jobs
jobs:
    databricks bundle summary -t dev | grep -A 20 "Jobs:"

# Run validation job
run-validation-job:
    databricks bundle run -t dev ssa_dashboard_validate_data

# === LAKEBASE ===

# Generate Lakebase OAuth token
lakebase-token:
    databricks postgres generate-database-credential \
        "projects/ssa-ops-dev/branches/production/endpoints/primary" \
        -p fevm-cjc --output json | jq -r '.token'

# Connect to Lakebase via psql
lakebase-connect:
    @set -l TOKEN (databricks postgres generate-database-credential \
        "projects/ssa-ops-dev/branches/production/endpoints/primary" \
        -p fevm-cjc --output json | jq -r '.token'); \
    PGPASSWORD="$TOKEN" psql \
        "host=$LAKEBASE_ENDPOINT port=5432 user=christopher.chalcraft@databricks.com sslmode=require dbname=$LAKEBASE_DATABASE"

# === DEV ===

# Start local dev server
dev:
    pnpm dev

# Run linter
lint:
    pnpm check

# Run tests
test:
    pnpm test

# Build app
build:
    pnpm build

# === DIMENSIONAL MODEL ===

# Set default catalog/schema for SQL commands
catalog := "cjc_aws_workspace_catalog"
schema := "ssa_ops"
source_catalog := "cjc_aws_workspace_catalog"
source_schema := "ssa_ops_dev"

# Create dimensional model schema
create-schema:
    @echo "Creating schema {{catalog}}.{{schema}}..."
    databricks sql execute -q "CREATE SCHEMA IF NOT EXISTS {{catalog}}.{{schema}}"
    @echo "✓ Schema created"

# Create all dimension tables
create-dims:
    @echo "Creating dimension tables..."
    @echo "  Creating dim_date..."
    databricks sql execute -f sql/tables/01_dim_date.sql \
        --var "catalog={{catalog}}" \
        --var "schema={{schema}}"
    @echo "  Creating dim_ssa..."
    databricks sql execute -f sql/tables/01_dim_ssa.sql \
        --var "catalog={{catalog}}" \
        --var "schema={{schema}}" \
        --var "source_catalog={{source_catalog}}" \
        --var "source_schema={{source_schema}}"
    @echo "  Creating dim_account..."
    databricks sql execute -f sql/tables/01_dim_account.sql \
        --var "catalog={{catalog}}" \
        --var "schema={{schema}}" \
        --var "source_catalog={{source_catalog}}" \
        --var "source_schema={{source_schema}}"
    @echo "✓ Dimension tables created"

# Create all fact tables
create-facts:
    @echo "Creating fact tables..."
    @echo "  Creating fact_asq..."
    databricks sql execute -f sql/tables/02_fact_asq.sql \
        --var "catalog={{catalog}}" \
        --var "schema={{schema}}" \
        --var "source_catalog={{source_catalog}}" \
        --var "source_schema={{source_schema}}"
    @echo "  Creating fact_uco..."
    databricks sql execute -f sql/tables/02_fact_uco.sql \
        --var "catalog={{catalog}}" \
        --var "schema={{schema}}" \
        --var "source_catalog={{source_catalog}}" \
        --var "source_schema={{source_schema}}"
    @echo "✓ Fact tables created"

# Create all dimensional model tables
create-tables: create-schema create-dims create-facts
    @echo "✓ All dimensional model tables created"

# Export raw Salesforce data from logfood
export-raw:
    @echo "Exporting raw Salesforce tables from logfood..."
    @echo "Run this manually on logfood SQL Editor:"
    @echo "  sql/sync/00_export_raw_tables.sql"

# === METRIC VIEWS ===

# Deploy all metric views
deploy-metric-views:
    @echo "Deploying metric views..."
    @for f in sql/metric-views/mv_*.sql; echo "  Deploying $f..."; databricks sql execute -f $f --var "catalog={{catalog}}" --var "schema={{schema}}"; end
    @echo "✓ Metric views deployed"

# Deploy a specific metric view
deploy-mv name:
    @echo "Deploying {{name}}..."
    databricks sql execute -f sql/metric-views/{{name}}.sql \
        --var "catalog={{catalog}}" \
        --var "schema={{schema}}"
    @echo "✓ {{name}} deployed"

# Describe a metric view
describe-mv name:
    databricks sql execute -q "DESCRIBE EXTENDED {{catalog}}.{{schema}}.{{name}}"

# Query metric view with BU filter
query-mv-bu name bu="CAN":
    databricks sql execute -q "SELECT * FROM {{catalog}}.{{schema}}.{{name}} WHERE \`Business Unit\` = '{{bu}}' LIMIT 100"

# Query metric view aggregated by BU
query-mv-all name:
    databricks sql execute -q "SELECT \`Business Unit\`, COUNT(1) AS cnt FROM {{catalog}}.{{schema}}.{{name}} GROUP BY ALL"

# === METRIC VIEW TESTING ===
# NOTE: These commands require running SQL manually on logfood SQL Editor
# or using the Databricks MCP tool. The `databricks sql execute` command
# is not available in all CLI versions.

# Deploy charter metrics 4-8 to logfood (automated via Python script)
deploy-charter-metrics:
    @echo "╔════════════════════════════════════════════════════════════════════╗"
    @echo "║            DEPLOYING CHARTER METRICS TO LOGFOOD                    ║"
    @echo "╚════════════════════════════════════════════════════════════════════╝"
    python scripts/deploy_metric_view.py
    @echo ""
    @echo "METRIC VIEWS CREATED in home_christopher_chalcraft.cjc_views:"
    @echo "  - mv_time_to_adopt         (Charter #4)"
    @echo "  - mv_asset_reuse           (Charter #5)"
    @echo "  - mv_self_service_health   (Charter #6 - proxy)"
    @echo "  - mv_product_impact        (Charter #7)"
    @echo "  - mv_customer_risk_reduction (Charter #8)"
    @echo ""
    @echo "Query example:"
    @echo "  SELECT \`Business Unit\`, MEASURE(\`Total UCOs\`), MEASURE(\`Adopted UCOs\`)"
    @echo "  FROM mv_time_to_adopt WHERE \`Region\` = 'CAN' GROUP BY ALL"

# Test charter metrics 4-8 (automated validation)
test-charter-metrics:
    @echo "Testing Charter Metrics 4-8..."
    python scripts/test_charter_metrics.py

# Deploy and test charter metrics in one command
charter-metrics: deploy-charter-metrics test-charter-metrics
    @echo ""
    @echo "✓ Charter metrics deployed and validated"

# Run all metric view validation tests
test-metric-views:
    @echo "Running metric view validation tests..."
    @echo "  Open logfood SQL Editor and run: sql/tests/validate_metric_views.sql"
    @echo "  Or use Databricks MCP: mcp__databricks-v2__execute_parameterized_sql"
    @echo ""
    @echo "  Workspace: https://adb-2548836972759138.18.azuredatabricks.net/sql/editor"
    @echo "  Warehouse: Shared SQL Endpoint - Stable (927ac096f9833442)"

# Run data quality tests
test-data-quality:
    @echo "Running data quality tests..."
    databricks sql execute -f sql/tests/validate_data_quality.sql -p logfood
    @echo "✓ Data quality tests complete"

# Run cross-BU consistency tests
test-cross-bu:
    @echo "Running cross-BU consistency tests..."
    databricks sql execute -f sql/tests/validate_cross_bu.sql -p logfood
    @echo "✓ Cross-BU tests complete"

# Run performance benchmarks
test-performance:
    @echo "Running performance benchmarks..."
    @echo "  Check query profile for execution times"
    databricks sql execute -f sql/tests/benchmark_performance.sql -p logfood
    @echo "✓ Performance benchmarks complete"

# Run all validation tests
test-all: test-metric-views test-data-quality test-cross-bu
    @echo "✓ All validation tests complete"

# === MATERIALIZATION ===

# Default warehouse ID for materialization
warehouse_id := "your-warehouse-id"

# Enable materialization on all metric views
enable-materialization:
    @echo "Enabling materialization on metric views..."
    databricks sql execute -f sql/metric-views/materialization_config.sql \
        --var "catalog={{catalog}}" \
        --var "schema={{schema}}" \
        --var "warehouse_id={{warehouse_id}}"
    @echo "✓ Materialization enabled"

# Check materialization status
materialization-status:
    @echo "Materialization status:"
    databricks sql execute -q "SELECT table_name, comment \
        FROM {{catalog}}.information_schema.tables \
        WHERE table_schema = '{{schema}}' AND table_name LIKE 'mv_%' \
        ORDER BY table_name"

# Trigger refresh of a specific metric view
refresh-mv name:
    @echo "Refreshing {{name}}..."
    databricks sql execute -q "REFRESH METRIC VIEW {{catalog}}.{{schema}}.{{name}}"
    @echo "✓ Refresh triggered"

# Refresh all core metric views
refresh-all-mv:
    @echo "Refreshing all metric views..."
    databricks sql execute -q "REFRESH METRIC VIEW {{catalog}}.{{schema}}.mv_asq_operations"
    databricks sql execute -q "REFRESH METRIC VIEW {{catalog}}.{{schema}}.mv_sla_compliance"
    databricks sql execute -q "REFRESH METRIC VIEW {{catalog}}.{{schema}}.mv_effort_capacity"
    databricks sql execute -q "REFRESH METRIC VIEW {{catalog}}.{{schema}}.mv_focus_discipline"
    databricks sql execute -q "REFRESH METRIC VIEW {{catalog}}.{{schema}}.mv_uco_velocity"
    databricks sql execute -q "REFRESH METRIC VIEW {{catalog}}.{{schema}}.mv_competitive_analysis"
    @echo "✓ Core metric views refreshed"

# === CHARTER METRICS ===

# Check focus discipline (80% L400+ target)
check-focus-discipline manager="Christopher Chalcraft":
    @echo "Focus & Discipline for {{manager}}:"
    databricks sql execute -q "SELECT \`Owner\`, \
        SUM(MEASURE(\`Total Effort Days\`)) AS total_effort, \
        SUM(MEASURE(\`Priority Effort Days\`)) AS priority_effort, \
        AVG(MEASURE(\`Priority Effort Rate\`)) AS priority_rate, \
        SUM(MEASURE(\`Meeting 80% Goal\`)) AS meeting_goal \
        FROM {{catalog}}.{{schema}}.mv_focus_discipline \
        WHERE \`Manager L1\` = '{{manager}}' \
        GROUP BY \`Owner\` ORDER BY priority_rate DESC"

# Check UCO velocity (time-to-production)
check-uco-velocity manager="Christopher Chalcraft":
    @echo "UCO Velocity for {{manager}}:"
    databricks sql execute -q "SELECT \`Owner\`, \
        SUM(MEASURE(\`Total UCOs\`)) AS total_ucos, \
        SUM(MEASURE(\`Production+ UCOs\`)) AS production_ucos, \
        AVG(MEASURE(\`Production Rate\`)) AS production_rate, \
        AVG(MEASURE(\`Avg Days in Stage\`)) AS avg_days \
        FROM {{catalog}}.{{schema}}.mv_uco_velocity \
        WHERE \`Manager L1\` = '{{manager}}' \
        GROUP BY \`Owner\` ORDER BY production_rate DESC"

# Check competitive win rate
check-competitive-win-rate manager="Christopher Chalcraft":
    @echo "Competitive Win Rate for {{manager}}:"
    databricks sql execute -q "SELECT \`Owner\`, \
        SUM(MEASURE(\`Total Closed UCOs\`)) AS total_closed, \
        SUM(MEASURE(\`Won UCOs\`)) AS won, \
        AVG(MEASURE(\`Win Rate\`)) AS win_rate, \
        AVG(MEASURE(\`Competitive Win Rate\`)) AS competitive_win_rate \
        FROM {{catalog}}.{{schema}}.mv_competitive_analysis \
        WHERE \`Manager L1\` = '{{manager}}' \
        GROUP BY \`Owner\` ORDER BY win_rate DESC"

# Charter metrics summary
check-charter-metrics manager="Christopher Chalcraft":
    @echo "=== CHARTER METRICS SUMMARY for {{manager}} ==="
    @echo ""
    @echo "📊 Focus & Discipline (80% L400+ Target):"
    @just check-focus-discipline "{{manager}}"
    @echo ""
    @echo "🚀 UCO Velocity (Time-to-Production):"
    @just check-uco-velocity "{{manager}}"
    @echo ""
    @echo "🏆 Competitive Win Rate:"
    @just check-competitive-win-rate "{{manager}}"

# === DAB WORKFLOWS ===

# Deploy metric view workflows via DAB
dab-deploy-metric-views:
    @echo "Deploying metric view workflows via DAB..."
    databricks bundle deploy -t dev
    @echo "✓ DAB deployment complete"

# Run metric view deployment job via DAB
dab-run-deploy:
    @echo "Running metric view deployment job..."
    databricks bundle run -t dev ssa_metric_view_deploy
    @echo "✓ Deployment job triggered"

# Run metric view validation job via DAB
dab-run-validation:
    @echo "Running metric view validation job..."
    databricks bundle run -t dev ssa_metric_view_validation
    @echo "✓ Validation job triggered"

# Run performance benchmark job via DAB
dab-run-benchmarks:
    @echo "Running performance benchmark job..."
    databricks bundle run -t dev ssa_metric_view_performance
    @echo "✓ Benchmark job triggered"

# Show DAB job status
dab-status:
    databricks bundle summary -t dev | grep -A 50 "Jobs:"

# === VALIDATION (Dimensional Model) ===

# Validate dimensional model
validate-dims:
    @echo "Validating dimensional model..."
    databricks sql execute -q "SELECT 'dim_date' AS tbl, COUNT(*) FROM {{catalog}}.{{schema}}.dim_date"
    databricks sql execute -q "SELECT 'dim_ssa' AS tbl, COUNT(*) FROM {{catalog}}.{{schema}}.dim_ssa"
    databricks sql execute -q "SELECT 'dim_account' AS tbl, COUNT(*) FROM {{catalog}}.{{schema}}.dim_account"
    databricks sql execute -q "SELECT 'fact_asq' AS tbl, COUNT(*) FROM {{catalog}}.{{schema}}.fact_asq"
    @echo "✓ Validation complete"

# Check hierarchy distribution
check-hierarchy:
    @echo "SSA hierarchy distribution:"
    databricks sql execute -q "SELECT business_unit, COUNT(*) FROM {{catalog}}.{{schema}}.dim_ssa WHERE is_active GROUP BY business_unit ORDER BY 2 DESC"

# Check SLA compliance rates
check-sla:
    @echo "SLA compliance rates:"
    databricks sql execute -q "SELECT \
        COUNT(*) AS total, \
        SUM(CASE WHEN review_sla_met THEN 1 ELSE 0 END) AS review_met, \
        SUM(CASE WHEN assignment_sla_met THEN 1 ELSE 0 END) AS assign_met, \
        SUM(CASE WHEN completion_sla_met THEN 1 ELSE 0 END) AS complete_met \
        FROM {{catalog}}.{{schema}}.fact_asq"
