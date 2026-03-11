import { createCollection } from '@tanstack/react-db'
import { electricCollectionOptions } from '@tanstack/electric-db-collection'

// Test items schema matching Lakebase table
export interface TestItem {
  id: number
  name: string
  description: string | null
  price: number | null
  quantity: number | null
  created_at: Date | null
  updated_at: Date | null
}

// Connect directly to Electric in development
// In production, you'd use a proxy route instead
const electricUrl = typeof window !== 'undefined'
  ? 'http://localhost:3000'
  : (process.env.ELECTRIC_URL || 'http://localhost:3000')

export const testItemsCollection = createCollection(
  electricCollectionOptions({
    id: 'test_items',
    getKey: (item) => item.id as number,
    shapeOptions: {
      url: `${electricUrl}/v1/shape`,
      params: {
        table: 'test_items',
      },
      parser: {
        // Parse timestamp strings to Date objects
        timestamptz: (value: string) => new Date(value),
        timestamp: (value: string) => new Date(value),
      },
    },
  })
)
