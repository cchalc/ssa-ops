# SSA-Ops Deployment Orchestration
# Run `just` to see available recipes

set shell := ["fish", "-c"]

# Default recipe - show help
default:
    @just --list

# === DEPLOYMENT ===

# Deploy to fevm-cjc dev environment
deploy-dev:
    @echo "Deploying to fevm-cjc (dev)..."
    databricks bundle deploy -t dev
    @echo "✓ Deploy complete"

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
    @echo ""
    @echo "Or use databricks CLI with logfood profile:"
    @echo "  databricks sql query --profile logfood -w 927ac096f9833442 -f sql/deploy_views.sql"

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

# Run export from logfood to Delta (via job)
sync-to-delta:
    @echo "Triggering export to Delta job..."
    databricks bundle run -t dev ssa_dashboard_export_to_delta --no-wait || { \
        echo ""; \
        echo "⚠ Job not available or failed. Run manually:"; \
        echo "  1. Open: https://adb-2548836972759138.18.azuredatabricks.net/sql/editor"; \
        echo "  2. Run: sql/sync/01_export_to_delta.sql"; \
    }

# Run sync from Delta to Lakebase
sync-to-lakebase:
    @echo "Triggering sync to Lakebase job..."
    databricks bundle run -t dev ssa_dashboard_sync_to_lakebase --no-wait || { \
        echo "⚠ Job failed to start"; \
    }

# Full sync pipeline: logfood → Delta → Lakebase
sync-all:
    @echo "Starting full sync pipeline..."
    @echo ""
    just sync-to-delta
    @echo ""
    @echo "Waiting 30s for Delta export to complete..."
    @sleep 30
    just sync-to-lakebase
    @echo ""
    @echo "✓ Sync jobs triggered (check Databricks for status)"

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
