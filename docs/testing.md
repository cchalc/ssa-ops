# SSA Dashboard - Data Validation & Testing

## Overview

The SSA Dashboard uses a three-tier data architecture that requires validation at each layer:

```
┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│  Logfood Views  │ ───▶ │  Delta Tables   │ ───▶ │    Lakebase     │
│  (Source)       │      │  (fevm-cjc)     │      │  (PostgreSQL)   │
└─────────────────┘      └─────────────────┘      └─────────────────┘
        ▲                        ▲                        ▲
        │                        │                        │
   01_validate_           02_validate_           data-validation
   logfood_views.sql      delta_tables.sql       .test.ts
```

## Test Files

| File | Location | Validates |
|------|----------|-----------|
| `01_validate_logfood_views.sql` | `sql/tests/` | Source views exist and return valid data |
| `02_validate_delta_tables.sql` | `sql/tests/` | Delta tables are synced with correct schema |
| `03_validate_cross_tier.sql` | `sql/tests/` | Row counts match across tiers |
| `data-validation.test.ts` | `tests/` | Lakebase schema, data freshness, integrity |

## Running Validation Tests

### Quick Start

```bash
# Run all validations
./scripts/run-validation.sh all

# Run specific tier
./scripts/run-validation.sh logfood
./scripts/run-validation.sh delta
./scripts/run-validation.sh lakebase
```

### Manual SQL Execution

**Logfood Views (source):**
```bash
databricks sql query \
    --profile logfood \
    --warehouse-id 927ac096f9833442 \
    --file sql/tests/01_validate_logfood_views.sql
```

**Delta Tables (fevm-cjc):**
```bash
databricks sql query \
    --profile fevm-cjc \
    --warehouse-id 751fe324525584e5 \
    --file sql/tests/02_validate_delta_tables.sql
```

### TypeScript Tests (Lakebase)

```bash
# Set credentials
export LAKEBASE_USER="your-user"
export LAKEBASE_PASSWORD="your-password"

# Run tests
pnpm test tests/data-validation.test.ts
```

## Test Categories

### 1. View/Table Existence
- Verifies all expected tables exist
- Checks row counts are non-zero (except team_summary = 1 row)

### 2. Schema Validation
- Required columns are present
- Data types are correct
- Primary keys are unique

### 3. Data Quality
- No null values in required fields
- Values are within expected ranges
- Enum fields contain valid values

### 4. Data Freshness
- `synced_at` timestamp within 24-48 hours
- Recent ASQ creation dates (within 7 days)

### 5. Referential Integrity
- SSA names consistent across tables
- Account IDs valid
- Foreign key relationships hold

### 6. Cross-Tier Consistency
- Row counts match between tiers
- Aggregate checksums align
- Sample data spot-checks

## Automated Validation Job

A weekly validation job runs automatically:

- **Schedule:** Monday 8:00 AM UTC
- **Job:** `[dev] SSA Dashboard - Validate Data`
- **Notifications:** Email on failure/success

Enable the job after initial deployment:
```bash
databricks jobs update --job-id <JOB_ID> --json '{"schedule": {"pause_status": "UNPAUSED"}}'
```

## Troubleshooting

### Common Issues

**"No data" failures:**
- Check if sync job ran successfully
- Verify source views have data
- Check for query timeouts

**"Sync > 24 hours old" warnings:**
- Verify scheduled jobs are running
- Check job failure notifications
- Manual sync: Run `sql/sync/01_export_to_delta.sql`

**Row count mismatches:**
- Check for filter differences
- Verify manager_id filter is consistent
- Look for timing issues (mid-sync)

### Manual Data Sync

If sync jobs fail, run manually:

1. **Export to Delta (logfood → fevm-cjc):**
   ```bash
   databricks sql query --profile logfood \
       --warehouse-id 927ac096f9833442 \
       --file sql/sync/01_export_to_delta.sql
   ```

2. **Verify Delta tables:**
   ```bash
   databricks sql query --profile fevm-cjc \
       --warehouse-id 751fe324525584e5 \
       --file sql/sync/02_sync_to_lakebase.sql
   ```

## Adding New Tests

### SQL Tests

Add to `sql/tests/` with naming convention:
- `XX_validate_<scope>.sql`

Follow the pattern:
```sql
SELECT
    'test_name' as test_name,
    <metric columns>,
    CASE
        WHEN <condition> THEN 'PASS'
        ELSE 'FAIL: <message>'
    END as status
FROM <table>;
```

### TypeScript Tests

Add to `tests/data-validation.test.ts`:

```typescript
describe("New Validation Category", () => {
  it("should validate something", async () => {
    const result = await client.query(`SELECT ...`);
    expect(result.rows.length).toBeGreaterThan(0);
  });
});
```

## CI/CD Integration

The GitHub Actions workflow includes validation:

```yaml
# .github/workflows/deploy-pipeline.yml
- name: Validate Delta Tables
  run: |
    databricks sql query --profile fevm-cjc \
        --warehouse-id 751fe324525584e5 \
        --file sql/tests/02_validate_delta_tables.sql
```
