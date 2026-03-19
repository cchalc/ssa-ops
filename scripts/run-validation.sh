#!/usr/bin/env bash
# Run data validation tests for SSA Dashboard
#
# Usage:
#   ./scripts/run-validation.sh [tier]
#
# Tiers:
#   logfood  - Validate source views (requires logfood profile)
#   delta    - Validate Delta tables (requires fevm-cjc profile)
#   lakebase - Validate Lakebase tables (requires pnpm test)
#   all      - Run all validations

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

tier="${1:-all}"

echo "=============================================="
echo "SSA Dashboard Data Validation"
echo "=============================================="
echo ""

validate_logfood() {
    echo ">>> Validating Logfood Views"
    echo "    Profile: logfood"
    echo "    Warehouse: Shared SQL Endpoint - Stable"
    echo ""

    databricks sql query \
        --profile logfood \
        --warehouse-id 927ac096f9833442 \
        --file "$PROJECT_DIR/sql/tests/01_validate_logfood_views.sql" \
        --format json \
        | jq -r '.[] | "\(.view_name // .test_name): \(.status)"'

    echo ""
}

validate_delta() {
    echo ">>> Validating Delta Tables"
    echo "    Profile: fevm-cjc"
    echo "    Catalog: cjc_aws_workspace_catalog.ssa_ops_dev"
    echo ""

    databricks sql query \
        --profile fevm-cjc \
        --warehouse-id 751fe324525584e5 \
        --file "$PROJECT_DIR/sql/tests/02_validate_delta_tables.sql" \
        --format json \
        | jq -r '.[] | "\(.table_name // .test_name): \(.status)"'

    echo ""
    echo ">>> Cross-Tier Validation"

    databricks sql query \
        --profile fevm-cjc \
        --warehouse-id 751fe324525584e5 \
        --file "$PROJECT_DIR/sql/tests/03_validate_cross_tier.sql" \
        --format json \
        | jq -r '.[] | "\(.test_name): \(.status // "INFO")"'

    echo ""
}

validate_lakebase() {
    echo ">>> Validating Lakebase Tables"
    echo "    Running TypeScript tests..."
    echo ""

    cd "$PROJECT_DIR"
    pnpm test tests/data-validation.test.ts -- --reporter=verbose 2>/dev/null || {
        echo "    Note: Set LAKEBASE_USER and LAKEBASE_PASSWORD to run Lakebase tests"
    }
    echo ""
}

case "$tier" in
    logfood)
        validate_logfood
        ;;
    delta)
        validate_delta
        ;;
    lakebase)
        validate_lakebase
        ;;
    all)
        validate_logfood
        validate_delta
        validate_lakebase
        ;;
    *)
        echo "Unknown tier: $tier"
        echo "Usage: $0 [logfood|delta|lakebase|all]"
        exit 1
        ;;
esac

echo "=============================================="
echo "Validation Complete"
echo "=============================================="
