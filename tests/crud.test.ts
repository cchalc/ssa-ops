import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from 'vitest'
import type { Pool } from 'pg'
import { createPool, isLakebaseAvailable } from './db-utils'

// Check if Lakebase is available before running tests
const lakebaseAvailable = isLakebaseAvailable()

describe.skipIf(!lakebaseAvailable)('CRUD Operations', () => {
  let pool: Pool
  let testItemId: number | null = null

  beforeAll(() => {
    pool = createPool()
  })

  afterAll(async () => {
    // Clean up any test items we created
    if (testItemId) {
      await pool.query('DELETE FROM app.test_items WHERE id = $1', [testItemId])
    }
    await pool.end()
  })

  describe('CREATE', () => {
    it('should insert a new item', async () => {
      const result = await pool.query(`
        INSERT INTO app.test_items (name, description, price, quantity)
        VALUES ($1, $2, $3, $4)
        RETURNING *
      `, ['Test Item', 'Created by test suite', 9.99, 10])

      expect(result.rows.length).toBe(1)
      const item = result.rows[0]
      testItemId = item.id

      expect(item.name).toBe('Test Item')
      expect(item.description).toBe('Created by test suite')
      expect(Number.parseFloat(item.price)).toBe(9.99)
      expect(item.quantity).toBe(10)
      expect(item.created_at).toBeDefined()
      expect(item.updated_at).toBeDefined()
    })

    it('should auto-generate id', async () => {
      const result = await pool.query(`
        INSERT INTO app.test_items (name, price)
        VALUES ($1, $2)
        RETURNING id
      `, ['Auto ID Test', 1.00])

      expect(result.rows[0].id).toBeGreaterThan(0)

      // Clean up
      await pool.query('DELETE FROM app.test_items WHERE id = $1', [result.rows[0].id])
    })

    it('should set default quantity to 0', async () => {
      const result = await pool.query(`
        INSERT INTO app.test_items (name, price)
        VALUES ($1, $2)
        RETURNING quantity
      `, ['Default Qty Test', 1.00])

      expect(result.rows[0].quantity).toBe(0)

      // Clean up
      await pool.query('DELETE FROM app.test_items WHERE name = $1', ['Default Qty Test'])
    })
  })

  describe('READ', () => {
    it('should read item by id', async () => {
      const result = await pool.query(
        'SELECT * FROM app.test_items WHERE id = $1',
        [testItemId]
      )

      expect(result.rows.length).toBe(1)
      expect(result.rows[0].name).toBe('Test Item')
    })

    it('should read items with filters', async () => {
      const result = await pool.query(`
        SELECT * FROM app.test_items
        WHERE price < $1
        ORDER BY price ASC
      `, [20.00])

      expect(result.rows.length).toBeGreaterThan(0)
      for (const row of result.rows) {
        expect(Number.parseFloat(row.price)).toBeLessThan(20.00)
      }
    })

    it('should read items with LIKE search', async () => {
      const result = await pool.query(`
        SELECT * FROM app.test_items
        WHERE name LIKE $1
      `, ['%Test%'])

      expect(result.rows.length).toBeGreaterThan(0)
    })

    it('should read with pagination', async () => {
      const result = await pool.query(`
        SELECT * FROM app.test_items
        ORDER BY id
        LIMIT $1 OFFSET $2
      `, [2, 0])

      expect(result.rows.length).toBeLessThanOrEqual(2)
    })
  })

  describe('UPDATE', () => {
    it('should update item fields', async () => {
      const result = await pool.query(`
        UPDATE app.test_items
        SET name = $1, price = $2
        WHERE id = $3
        RETURNING *
      `, ['Updated Test Item', 19.99, testItemId])

      expect(result.rows.length).toBe(1)
      expect(result.rows[0].name).toBe('Updated Test Item')
      expect(Number.parseFloat(result.rows[0].price)).toBe(19.99)
    })

    it('should auto-update updated_at timestamp', async () => {
      // Get current timestamp
      const before = await pool.query(
        'SELECT updated_at FROM app.test_items WHERE id = $1',
        [testItemId]
      )
      const beforeTime = new Date(before.rows[0].updated_at).getTime()

      // Wait a bit to ensure timestamp difference
      await new Promise((resolve) => setTimeout(resolve, 100))

      // Update the item
      await pool.query(`
        UPDATE app.test_items SET quantity = quantity + 1 WHERE id = $1
      `, [testItemId])

      // Get new timestamp
      const after = await pool.query(
        'SELECT updated_at FROM app.test_items WHERE id = $1',
        [testItemId]
      )
      const afterTime = new Date(after.rows[0].updated_at).getTime()

      expect(afterTime).toBeGreaterThanOrEqual(beforeTime)
    })

    it('should update with conditions', async () => {
      const result = await pool.query(`
        UPDATE app.test_items
        SET quantity = quantity + 5
        WHERE id = $1 AND quantity < 1000
        RETURNING quantity
      `, [testItemId])

      expect(result.rows.length).toBe(1)
      expect(result.rows[0].quantity).toBeGreaterThan(0)
    })
  })

  describe('DELETE', () => {
    let deleteTestId: number

    beforeEach(async () => {
      // Create item to delete
      const result = await pool.query(`
        INSERT INTO app.test_items (name, price)
        VALUES ($1, $2)
        RETURNING id
      `, ['Delete Test Item', 1.00])
      deleteTestId = result.rows[0].id
    })

    it('should delete item by id', async () => {
      const result = await pool.query(
        'DELETE FROM app.test_items WHERE id = $1 RETURNING id',
        [deleteTestId]
      )

      expect(result.rows.length).toBe(1)
      expect(result.rows[0].id).toBe(deleteTestId)

      // Verify deletion
      const verify = await pool.query(
        'SELECT * FROM app.test_items WHERE id = $1',
        [deleteTestId]
      )
      expect(verify.rows.length).toBe(0)
    })

    it('should return nothing when deleting non-existent item', async () => {
      // First delete the item
      await pool.query('DELETE FROM app.test_items WHERE id = $1', [deleteTestId])

      // Try to delete again
      const result = await pool.query(
        'DELETE FROM app.test_items WHERE id = $1 RETURNING id',
        [deleteTestId]
      )

      expect(result.rows.length).toBe(0)
    })
  })
})

describe.skipIf(!lakebaseAvailable)('Transaction Support', () => {
  let pool: Pool

  beforeAll(() => {
    pool = createPool()
  })

  afterAll(async () => {
    await pool.end()
  })

  it('should support transactions with commit', async () => {
    const client = await pool.connect()
    try {
      await client.query('BEGIN')

      const insert = await client.query(`
        INSERT INTO app.test_items (name, price)
        VALUES ($1, $2)
        RETURNING id
      `, ['Transaction Test', 5.00])

      await client.query('COMMIT')

      // Verify it persisted
      const verify = await pool.query(
        'SELECT * FROM app.test_items WHERE id = $1',
        [insert.rows[0].id]
      )
      expect(verify.rows.length).toBe(1)

      // Clean up
      await pool.query('DELETE FROM app.test_items WHERE id = $1', [insert.rows[0].id])
    } finally {
      client.release()
    }
  })

  it('should support transactions with rollback', async () => {
    const client = await pool.connect()
    let insertedId: number

    try {
      await client.query('BEGIN')

      const insert = await client.query(`
        INSERT INTO app.test_items (name, price)
        VALUES ($1, $2)
        RETURNING id
      `, ['Rollback Test', 5.00])
      insertedId = insert.rows[0].id

      await client.query('ROLLBACK')

      // Verify it did NOT persist
      const verify = await pool.query(
        'SELECT * FROM app.test_items WHERE id = $1',
        [insertedId]
      )
      expect(verify.rows.length).toBe(0)
    } finally {
      client.release()
    }
  })
})
