import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import type { Pool } from 'pg'
import {
  createPool,
  checkLakebaseHealth,
  getProjectInfo,
  getEndpoints,
} from './db-utils'

describe('Lakebase Infrastructure', () => {
  describe('Project Configuration', () => {
    it('should have a valid project with PG version 17', () => {
      const project = getProjectInfo()
      expect(project.status).toBeDefined()
      expect((project.status as Record<string, unknown>).pg_version).toBe(17)
    })

    it('should have history retention configured', () => {
      const project = getProjectInfo()
      const status = project.status as Record<string, unknown>
      expect(status.history_retention_duration).toBe('1209600s') // 14 days
    })

    it('should have autoscaling limits configured', () => {
      const project = getProjectInfo()
      const status = project.status as Record<string, unknown>
      const settings = status.default_endpoint_settings as Record<string, unknown>
      expect(settings.autoscaling_limit_min_cu).toBe(0.5)
      expect(settings.autoscaling_limit_max_cu).toBe(2)
    })
  })

  describe('Endpoints', () => {
    it('should have a primary read-write endpoint', () => {
      const endpoints = getEndpoints()
      const primary = endpoints.find(
        (e) => (e.status as Record<string, unknown>).endpoint_type === 'ENDPOINT_TYPE_READ_WRITE'
      )
      expect(primary).toBeDefined()
      expect((primary!.status as Record<string, unknown>).current_state).toBe('ACTIVE')
    })

    it('should have a read replica endpoint', () => {
      const endpoints = getEndpoints()
      const replica = endpoints.find(
        (e) => (e.status as Record<string, unknown>).endpoint_type === 'ENDPOINT_TYPE_READ_ONLY'
      )
      expect(replica).toBeDefined()
      // Read replica may be IDLE when not in use (scale-to-zero)
      expect(['ACTIVE', 'IDLE']).toContain(
        (replica!.status as Record<string, unknown>).current_state
      )
    })

    it('should have suspend timeout on read replica', () => {
      const endpoints = getEndpoints()
      const replica = endpoints.find(
        (e) => (e.status as Record<string, unknown>).endpoint_type === 'ENDPOINT_TYPE_READ_ONLY'
      )
      expect(replica).toBeDefined()
      expect((replica!.status as Record<string, unknown>).suspend_timeout_duration).toBe('300s')
    })
  })
})

describe('Database Connectivity', () => {
  let pool: Pool

  beforeAll(() => {
    pool = createPool()
  })

  afterAll(async () => {
    await pool.end()
  })

  it('should connect to Lakebase', async () => {
    const isHealthy = await checkLakebaseHealth(pool)
    expect(isHealthy).toBe(true)
  })

  it('should return PostgreSQL 17', async () => {
    const result = await pool.query('SELECT version()')
    expect(result.rows[0].version).toContain('PostgreSQL 17')
  })

  it('should have app schema', async () => {
    const result = await pool.query(`
      SELECT schema_name FROM information_schema.schemata
      WHERE schema_name = 'app'
    `)
    expect(result.rows.length).toBe(1)
  })

  it('should have test_items table', async () => {
    const result = await pool.query(`
      SELECT table_name FROM information_schema.tables
      WHERE table_schema = 'app' AND table_name = 'test_items'
    `)
    expect(result.rows.length).toBe(1)
  })

  it('should have required indexes', async () => {
    const result = await pool.query(`
      SELECT indexname FROM pg_indexes
      WHERE schemaname = 'app' AND tablename = 'test_items'
      ORDER BY indexname
    `)
    const indexNames = result.rows.map((r) => r.indexname)
    expect(indexNames).toContain('idx_test_items_name')
    expect(indexNames).toContain('idx_test_items_created_at')
    expect(indexNames).toContain('test_items_pkey')
  })
})

describe('Sample Data', () => {
  let pool: Pool

  beforeAll(() => {
    pool = createPool()
  })

  afterAll(async () => {
    await pool.end()
  })

  it('should have sample items', async () => {
    const result = await pool.query('SELECT COUNT(*) as count FROM app.test_items')
    expect(Number.parseInt(result.rows[0].count)).toBeGreaterThanOrEqual(3)
  })

  it('should have correct sample data structure', async () => {
    const result = await pool.query('SELECT * FROM app.test_items LIMIT 1')
    const item = result.rows[0]

    expect(item).toHaveProperty('id')
    expect(item).toHaveProperty('name')
    expect(item).toHaveProperty('description')
    expect(item).toHaveProperty('price')
    expect(item).toHaveProperty('quantity')
    expect(item).toHaveProperty('created_at')
    expect(item).toHaveProperty('updated_at')
  })

  it('should have valid price values', async () => {
    const result = await pool.query(`
      SELECT name, price FROM app.test_items ORDER BY id
    `)
    expect(result.rows.length).toBeGreaterThanOrEqual(3)
    // Check prices are valid decimals
    for (const row of result.rows) {
      expect(Number.parseFloat(row.price)).toBeGreaterThan(0)
    }
  })
})
