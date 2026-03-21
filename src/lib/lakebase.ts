import { execSync } from "node:child_process";
import { homedir } from "node:os";
import pg from "pg";

const { Pool } = pg;

const PROFILE = "fevm-cjc";
const PROJECT_NAME = "ssa-ops-dev";
const DATABASE_NAME = "ssa_ops_dev";
const ENDPOINT_HOST =
	"ep-bold-block-d8nx4viy.database.us-east-2.cloud.databricks.com";
const ENDPOINT_NAME = `projects/${PROJECT_NAME}/branches/production/endpoints/primary`;

let poolInstance: pg.Pool | null = null;

// Execution options to ensure databricks CLI finds config
// IMPORTANT: We unset DATABRICKS_HOST/TOKEN to force profile usage
const execOptions = {
	encoding: "utf-8" as const,
	env: {
		...process.env,
		HOME: homedir(),
		PATH: process.env.PATH || "/usr/local/bin:/usr/bin:/bin",
		// Clear any env vars that would override profile
		DATABRICKS_HOST: undefined,
		DATABRICKS_TOKEN: undefined,
	},
	shell: "/bin/bash" as const,
};

/**
 * Generate OAuth token for Lakebase connection
 */
function generateToken(): string {
	try {
		const result = execSync(
			`databricks postgres generate-database-credential "${ENDPOINT_NAME}" -p ${PROFILE} --output json`,
			execOptions,
		);
		const parsed = JSON.parse(result);
		return parsed.token;
	} catch (error) {
		console.error("Failed to generate token:", (error as Error).message);
		throw new Error(
			`Failed to generate Lakebase credential: ${(error as Error).message}`,
		);
	}
}

/**
 * Get current Databricks user email
 */
function getCurrentUser(): string {
	try {
		const result = execSync(
			`databricks current-user me -p ${PROFILE} --output json`,
			execOptions,
		);
		const parsed = JSON.parse(result);
		return parsed.userName;
	} catch (error) {
		console.error("Failed to get user:", (error as Error).message);
		throw new Error(
			`Failed to get Databricks user: ${(error as Error).message}`,
		);
	}
}

/**
 * Get or create a connection pool
 * Note: Token expires after 1 hour, so we recreate pool periodically
 */
export function getPool(): pg.Pool {
	if (!poolInstance) {
		poolInstance = new Pool({
			host: ENDPOINT_HOST,
			port: 5432,
			database: DATABASE_NAME,
			user: getCurrentUser(),
			password: generateToken(),
			ssl: { rejectUnauthorized: false },
			max: 5,
			idleTimeoutMillis: 30000,
			connectionTimeoutMillis: 10000,
		});
	}
	return poolInstance;
}

/**
 * Reset pool (call when token expires)
 */
export async function resetPool(): Promise<void> {
	if (poolInstance) {
		await poolInstance.end();
		poolInstance = null;
	}
}

export interface TestItem {
	id: number;
	name: string;
	description: string | null;
	price: string | null;
	quantity: number | null;
	created_at: Date;
	updated_at: Date;
}

export interface LakebaseStats {
	totalItems: number;
	totalValue: number;
	totalQuantity: number;
	lastUpdated: Date | null;
}

/**
 * Fetch all test items from Lakebase
 */
export async function fetchTestItems(): Promise<TestItem[]> {
	const pool = getPool();
	try {
		const result = await pool.query<TestItem>(
			"SELECT * FROM app.test_items ORDER BY id ASC",
		);
		return result.rows;
	} catch (error) {
		// If auth error, try resetting pool
		if ((error as Error).message?.includes("authentication")) {
			await resetPool();
			const pool = getPool();
			const result = await pool.query<TestItem>(
				"SELECT * FROM app.test_items ORDER BY id ASC",
			);
			return result.rows;
		}
		throw error;
	}
}

/**
 * Get aggregate stats from Lakebase
 */
export async function fetchStats(): Promise<LakebaseStats> {
	const pool = getPool();
	const result = await pool.query(`
    SELECT
      COUNT(*) as total_items,
      COALESCE(SUM(price * quantity), 0) as total_value,
      COALESCE(SUM(quantity), 0) as total_quantity,
      MAX(updated_at) as last_updated
    FROM app.test_items
  `);
	const row = result.rows[0];
	return {
		totalItems: Number.parseInt(row.total_items, 10),
		totalValue: Number.parseFloat(row.total_value),
		totalQuantity: Number.parseInt(row.total_quantity, 10),
		lastUpdated: row.last_updated,
	};
}

/**
 * Add a new test item
 */
export async function addTestItem(item: {
	name: string;
	description?: string;
	price?: number;
	quantity?: number;
}): Promise<TestItem> {
	const pool = getPool();
	const result = await pool.query<TestItem>(
		`INSERT INTO app.test_items (name, description, price, quantity)
     VALUES ($1, $2, $3, $4)
     RETURNING *`,
		[
			item.name,
			item.description || null,
			item.price || null,
			item.quantity || 0,
		],
	);
	return result.rows[0];
}

/**
 * Delete a test item
 */
export async function deleteTestItem(id: number): Promise<boolean> {
	const pool = getPool();
	const result = await pool.query(
		"DELETE FROM app.test_items WHERE id = $1 RETURNING id",
		[id],
	);
	return result.rowCount !== null && result.rowCount > 0;
}
