import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    include: ['tests/**/*.test.ts'],
    testTimeout: 30000, // 30 seconds for DB operations
    hookTimeout: 30000,
    teardownTimeout: 10000,
  },
})
