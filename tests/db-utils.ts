import { execSync } from 'node:child_process'
import { Pool } from 'pg'

const PROFILE = 'fevm-cjc'
const PROJECT_NAME = 'ssa-ops-dev'
const DATABASE_NAME = 'ssa_ops_dev'
const ENDPOINT_HOST = 'ep-bold-block-d8nx4viy.database.us-east-2.cloud.databricks.com'
const ENDPOINT_NAME = `projects/${PROJECT_NAME}/branches/production/endpoints/primary`

export interface LakebaseConfig {
  host: string
  port: number
  database: string
  user: string
  password: string
  ssl: boolean
}

/**
 * Generate OAuth token for Lakebase connection
 */
export function generateToken(): string {
  const result = execSync(
    `databricks postgres generate-database-credential "${ENDPOINT_NAME}" -p ${PROFILE} --output json`,
    { encoding: 'utf-8' }
  )
  const parsed = JSON.parse(result)
  return parsed.token
}

/**
 * Get current Databricks user email
 */
export function getCurrentUser(): string {
  const result = execSync(
    `databricks current-user me -p ${PROFILE} --output json`,
    { encoding: 'utf-8' }
  )
  const parsed = JSON.parse(result)
  return parsed.userName
}

/**
 * Get Lakebase connection config
 */
export function getLakebaseConfig(): LakebaseConfig {
  return {
    host: ENDPOINT_HOST,
    port: 5432,
    database: DATABASE_NAME,
    user: getCurrentUser(),
    password: generateToken(),
    ssl: true,
  }
}

/**
 * Create a connection pool for tests
 */
export function createPool(): Pool {
  const config = getLakebaseConfig()
  return new Pool({
    host: config.host,
    port: config.port,
    database: config.database,
    user: config.user,
    password: config.password,
    ssl: {
      rejectUnauthorized: false, // Lakebase uses self-signed certs
    },
    max: 5,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 10000,
  })
}

/**
 * Check if Lakebase is reachable
 */
export async function checkLakebaseHealth(pool: Pool): Promise<boolean> {
  try {
    const result = await pool.query('SELECT 1 as health')
    return result.rows[0].health === 1
  } catch {
    return false
  }
}

/**
 * Get Lakebase project info via CLI
 */
export function getProjectInfo(): Record<string, unknown> {
  const result = execSync(
    `databricks postgres get-project "projects/${PROJECT_NAME}" -p ${PROFILE} --output json`,
    { encoding: 'utf-8' }
  )
  return JSON.parse(result)
}

/**
 * Get Lakebase endpoints info via CLI
 */
export function getEndpoints(): Array<Record<string, unknown>> {
  const result = execSync(
    `databricks postgres list-endpoints "projects/${PROJECT_NAME}/branches/production" -p ${PROFILE} --output json`,
    { encoding: 'utf-8' }
  )
  return JSON.parse(result)
}
