#!/bin/bash
# Post-deploy configuration for Lakebase Autoscaling
# Called after bundle deployment to configure permissions, protection, and run migrations
#
# Required environment variables:
#   DATABRICKS_HOST - Workspace URL (or use profile)
#   DATABRICKS_TOKEN - PAT or OAuth token (or use profile)
#   LAKEBASE_PROJECT_NAME - Lakebase project name (e.g., "ssa-ops-dev")
#   DATABASE_NAME - Application database name (e.g., "ssa_ops_dev")
#   DATABRICKS_PROFILE - Optional: Databricks CLI profile to use

set -euo pipefail

PROJECT_NAME="${LAKEBASE_PROJECT_NAME:?LAKEBASE_PROJECT_NAME is required}"
DATABASE_NAME="${DATABASE_NAME:?DATABASE_NAME is required}"
PROFILE_FLAG="${DATABRICKS_PROFILE:+--profile $DATABRICKS_PROFILE}"

echo "=== Lakebase Autoscaling Post-Deploy Configuration ==="
echo "Project: $PROJECT_NAME"
echo "Database: $DATABASE_NAME"

# 1. Get project info
echo ""
echo ">>> Getting project info..."
PROJECT_INFO=$(databricks postgres get-project "projects/$PROJECT_NAME" $PROFILE_FLAG --output json)
PROJECT_UID=$(echo "$PROJECT_INFO" | jq -r '.uid')
echo "Project UID: $PROJECT_UID"

# 2. Get production branch info and protect it
echo ""
echo ">>> Checking production branch protection..."
BRANCH_INFO=$(databricks postgres get-branch "projects/$PROJECT_NAME/branches/production" $PROFILE_FLAG --output json 2>/dev/null || echo '{}')
IS_PROTECTED=$(echo "$BRANCH_INFO" | jq -r '.status.is_protected // false')

if [ "$IS_PROTECTED" = "false" ]; then
    echo "Setting production branch as protected..."
    databricks postgres update-branch "projects/$PROJECT_NAME/branches/production" \
        --json '{"is_protected": true}' $PROFILE_FLAG \
        2>/dev/null || echo "Note: Branch protection may require additional privileges"
else
    echo "Production branch is already protected"
fi

# 3. Get connection details
echo ""
echo ">>> Getting connection details..."
ENDPOINTS=$(databricks postgres list-endpoints "projects/$PROJECT_NAME/branches/production" $PROFILE_FLAG --output json)
ENDPOINT_HOST=$(echo "$ENDPOINTS" | jq -r '.[] | select(.status.endpoint_type == "ENDPOINT_TYPE_READ_WRITE") | .status.hosts.host')

if [ -z "$ENDPOINT_HOST" ] || [ "$ENDPOINT_HOST" = "null" ]; then
    echo "ERROR: Could not find production read-write endpoint"
    exit 1
fi
echo "Endpoint host: $ENDPOINT_HOST"

# 4. Generate OAuth credential for database access
echo ""
echo ">>> Generating database credential..."
# Get the primary endpoint ID
ENDPOINT_NAME=$(echo "$ENDPOINTS" | jq -r '.[] | select(.status.endpoint_type == "ENDPOINT_TYPE_READ_WRITE") | .name')
CRED_RESPONSE=$(databricks postgres generate-database-credential "$ENDPOINT_NAME" $PROFILE_FLAG --output json)
DB_TOKEN=$(echo "$CRED_RESPONSE" | jq -r '.token')
DB_USER=$(databricks current-user me $PROFILE_FLAG --output json | jq -r '.userName')

if [ -z "$DB_TOKEN" ] || [ "$DB_TOKEN" = "null" ]; then
    echo "ERROR: Could not generate database credential"
    exit 1
fi
echo "Database user: $DB_USER"

# 5. Create database if not exists
echo ""
echo ">>> Creating database $DATABASE_NAME if not exists..."
PGPASSWORD="$DB_TOKEN" psql "host=$ENDPOINT_HOST port=5432 user=$DB_USER sslmode=require dbname=databricks_postgres" \
    -c "CREATE DATABASE $DATABASE_NAME;" 2>/dev/null || echo "Database may already exist"

# 6. Run migrations
echo ""
echo ">>> Running schema migrations..."
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PGPASSWORD="$DB_TOKEN" psql "host=$ENDPOINT_HOST port=5432 user=$DB_USER sslmode=require dbname=$DATABASE_NAME" \
    -f "$SCRIPT_DIR/migrations.sql"

echo ""
echo "=== Post-deploy configuration complete ==="
echo ""
echo "Connection string:"
echo "  postgresql://$DB_USER@$ENDPOINT_HOST:5432/$DATABASE_NAME?sslmode=require"
echo ""
echo "Connect with CLI:"
echo "  databricks postgres generate-database-credential projects/$PROJECT_NAME $PROFILE_FLAG"
echo "  psql \"host=$ENDPOINT_HOST port=5432 user=$DB_USER sslmode=require dbname=$DATABASE_NAME\""
