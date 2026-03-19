/**
 * Data Validation Test Suite for SSA Dashboard
 *
 * Validates data integrity across the three-tier sync:
 *   1. Logfood views (home_christopher_chalcraft.cjc_views)
 *   2. Delta tables (cjc_aws_workspace_catalog.ssa_ops_dev)
 *   3. Lakebase tables (ssa_ops_dev.dashboard)
 *
 * Run with: pnpm test tests/data-validation.test.ts
 */

import { describe, it, expect, beforeAll } from "vitest";
import { Client } from "pg";

// Test configuration
const LAKEBASE_CONFIG = {
  host: process.env.LAKEBASE_HOST || "ssa-ops-dev.lakebase.databricks.com",
  port: parseInt(process.env.LAKEBASE_PORT || "5432"),
  database: process.env.LAKEBASE_DATABASE || "ssa_ops_dev",
  user: process.env.LAKEBASE_USER || "",
  password: process.env.LAKEBASE_PASSWORD || "",
  ssl: { rejectUnauthorized: false },
};

// Expected tables in dashboard schema
const DASHBOARD_TABLES = [
  "team_summary",
  "asq_completed_metrics",
  "asq_sla_metrics",
  "asq_effort_accuracy",
  "asq_reengagement",
  "ssa_performance",
];

// Required columns per table
const TABLE_COLUMNS: Record<string, string[]> = {
  team_summary: [
    "total_open_asqs",
    "overdue_asqs",
    "missing_notes_asqs",
    "completed_qtd",
    "synced_at",
  ],
  asq_completed_metrics: [
    "asq_id",
    "asq_number",
    "owner_name",
    "completion_date",
    "days_total",
    "delivered_on_time",
    "synced_at",
  ],
  asq_sla_metrics: [
    "asq_id",
    "asq_number",
    "owner_name",
    "review_sla_met",
    "assignment_sla_met",
    "response_sla_met",
    "synced_at",
  ],
  asq_effort_accuracy: [
    "asq_id",
    "asq_number",
    "owner_name",
    "estimated_days",
    "actual_days",
    "effort_ratio",
    "synced_at",
  ],
  asq_reengagement: [
    "account_id",
    "account_name",
    "total_asqs",
    "engagement_tier",
    "is_repeat_customer",
    "synced_at",
  ],
  ssa_performance: [
    "owner_name",
    "total_open_asqs",
    "overdue_count",
    "pct_overdue",
    "synced_at",
  ],
};

let client: Client;

beforeAll(async () => {
  // Skip tests if no credentials configured
  if (!LAKEBASE_CONFIG.user || !LAKEBASE_CONFIG.password) {
    console.warn("Skipping Lakebase tests: No credentials configured");
    return;
  }

  client = new Client(LAKEBASE_CONFIG);
  await client.connect();
});

describe("Lakebase Schema Validation", () => {
  it("should have dashboard schema", async () => {
    if (!client) return;

    const result = await client.query(`
      SELECT schema_name
      FROM information_schema.schemata
      WHERE schema_name = 'dashboard'
    `);

    expect(result.rows.length).toBe(1);
  });

  it.each(DASHBOARD_TABLES)("should have %s table", async (tableName) => {
    if (!client) return;

    const result = await client.query(
      `
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = 'dashboard' AND table_name = $1
    `,
      [tableName]
    );

    expect(result.rows.length).toBe(1);
  });
});

describe("Column Validation", () => {
  it.each(Object.entries(TABLE_COLUMNS))(
    "table %s should have required columns",
    async (tableName, requiredColumns) => {
      if (!client) return;

      const result = await client.query(
        `
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'dashboard' AND table_name = $1
      `,
        [tableName]
      );

      const actualColumns = result.rows.map((r) => r.column_name);

      for (const col of requiredColumns) {
        expect(actualColumns).toContain(col);
      }
    }
  );
});

describe("Data Freshness", () => {
  it.each(DASHBOARD_TABLES)(
    "table %s should have recent synced_at",
    async (tableName) => {
      if (!client) return;

      const result = await client.query(`
        SELECT MAX(synced_at) as last_sync
        FROM dashboard.${tableName}
      `);

      if (result.rows[0].last_sync) {
        const lastSync = new Date(result.rows[0].last_sync);
        const hoursSinceSync =
          (Date.now() - lastSync.getTime()) / (1000 * 60 * 60);

        // Data should be less than 48 hours old
        expect(hoursSinceSync).toBeLessThan(48);
      }
    }
  );
});

describe("Data Integrity", () => {
  it("team_summary should have exactly one row", async () => {
    if (!client) return;

    const result = await client.query(`
      SELECT COUNT(*) as count FROM dashboard.team_summary
    `);

    expect(parseInt(result.rows[0].count)).toBe(1);
  });

  it("asq_completed_metrics should have positive row count", async () => {
    if (!client) return;

    const result = await client.query(`
      SELECT COUNT(*) as count FROM dashboard.asq_completed_metrics
    `);

    expect(parseInt(result.rows[0].count)).toBeGreaterThan(0);
  });

  it("ssa_performance should have valid percentage values", async () => {
    if (!client) return;

    const result = await client.query(`
      SELECT
        MIN(pct_overdue) as min_pct,
        MAX(pct_overdue) as max_pct
      FROM dashboard.ssa_performance
      WHERE pct_overdue IS NOT NULL
    `);

    if (result.rows[0].min_pct !== null) {
      expect(parseFloat(result.rows[0].min_pct)).toBeGreaterThanOrEqual(0);
      expect(parseFloat(result.rows[0].max_pct)).toBeLessThanOrEqual(100);
    }
  });

  it("asq_reengagement tiers should be valid", async () => {
    if (!client) return;

    const validTiers = ["Single", "Light", "Moderate", "Heavy", "Strategic"];

    const result = await client.query(`
      SELECT DISTINCT engagement_tier
      FROM dashboard.asq_reengagement
      WHERE engagement_tier IS NOT NULL
    `);

    for (const row of result.rows) {
      expect(validTiers).toContain(row.engagement_tier);
    }
  });
});

describe("Cross-Table Consistency", () => {
  it("team_summary totals should match aggregated data", async () => {
    if (!client) return;

    // Get team summary values
    const summaryResult = await client.query(`
      SELECT completed_qtd FROM dashboard.team_summary LIMIT 1
    `);

    // Get actual completed count this quarter
    const completedResult = await client.query(`
      SELECT COUNT(*) as count
      FROM dashboard.asq_completed_metrics
      WHERE completion_quarter = (
        SELECT 'Q' || EXTRACT(QUARTER FROM CURRENT_DATE) || '-' || EXTRACT(YEAR FROM CURRENT_DATE)
      )
    `);

    const summaryCount = parseInt(summaryResult.rows[0]?.completed_qtd || "0");
    const actualCount = parseInt(completedResult.rows[0]?.count || "0");

    // Allow some variance due to timing
    expect(Math.abs(summaryCount - actualCount)).toBeLessThan(10);
  });

  it("ssa_performance owner names should be in completed metrics", async () => {
    if (!client) return;

    const result = await client.query(`
      SELECT sp.owner_name
      FROM dashboard.ssa_performance sp
      LEFT JOIN dashboard.asq_completed_metrics acm
        ON sp.owner_name = acm.owner_name
      WHERE acm.owner_name IS NULL
      LIMIT 5
    `);

    // All SSAs with performance data should have at least one completed ASQ
    // (or be new - allow up to 2 without history)
    expect(result.rows.length).toBeLessThanOrEqual(2);
  });
});

describe("Data Source Validation", () => {
  it("should have ASQ IDs in Salesforce format", async () => {
    if (!client) return;

    const result = await client.query(`
      SELECT asq_id
      FROM dashboard.asq_completed_metrics
      WHERE asq_id NOT LIKE 'a%'
      LIMIT 5
    `);

    // Salesforce IDs start with 'a' prefix
    expect(result.rows.length).toBe(0);
  });

  it("ASQ numbers should follow naming convention", async () => {
    if (!client) return;

    const result = await client.query(`
      SELECT asq_number
      FROM dashboard.asq_completed_metrics
      WHERE asq_number NOT LIKE 'AR-%'
      LIMIT 5
    `);

    // All ASQ numbers should start with AR-
    expect(result.rows.length).toBe(0);
  });
});
