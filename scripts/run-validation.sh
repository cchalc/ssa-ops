#!/usr/bin/env bash
# Run data validation tests for SSA Dashboard
#
# Usage:
#   ./scripts/run-validation.sh [tier]
#
# Tiers:
#   logfood  - Show instructions for logfood validation
#   delta    - Run Delta table validation via job
#   lakebase - Run Lakebase tests via pnpm
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
    echo ">>> Logfood Views Validation"
    echo "    Profile: logfood"
    echo "    Warehouse: Shared SQL Endpoint - Stable"
    echo ""
    echo "    Run manually in Databricks SQL Editor:"
    echo "    1. Open: https://adb-2548836972759138.18.azuredatabricks.net/sql/editor"
    echo "    2. Select warehouse: Shared SQL Endpoint - Stable (927ac096f9833442)"
    echo "    3. Run: sql/tests/01_validate_logfood_views.sql"
    echo ""
}

validate_delta() {
    echo ">>> Validating Delta Tables"
    echo "    Running validation job on fevm-cjc..."
    echo ""

    # Run the deployed validation job
    if databricks bundle run -t dev ssa_dashboard_validate_data --no-wait 2>/dev/null; then
        echo "    ✓ Validation job started"
        echo "    Check results at: https://fevm-cjc-aws-workspace.cloud.databricks.com/jobs"
    else
        echo "    ⚠ Job not available. Run validation manually:"
        echo "    1. Open: https://fevm-cjc-aws-workspace.cloud.databricks.com/sql/editor"
        echo "    2. Run: sql/tests/02_validate_delta_tables.sql"
        echo "    3. Run: sql/tests/03_validate_cross_tier.sql"
    fi
    echo ""
}

validate_lakebase() {
    echo ">>> Validating Lakebase Tables"
    echo "    Running TypeScript tests..."
    echo ""

    cd "$PROJECT_DIR"
    if [ -n "${LAKEBASE_USER:-}" ] && [ -n "${LAKEBASE_PASSWORD:-}" ]; then
        pnpm test tests/data-validation.test.ts -- --reporter=verbose 2>/dev/null || {
            echo "    ⚠ Tests failed or skipped"
        }
    else
        echo "    ⚠ Skipped: Set LAKEBASE_USER and LAKEBASE_PASSWORD to run"
        echo ""
        echo "    To generate credentials:"
        echo "    export LAKEBASE_USER=christopher.chalcraft@databricks.com"
        echo "    export LAKEBASE_PASSWORD=\$(just lakebase-token)"
    fi
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
