import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    globals: true,
    coverage: {
      provider: 'v8',
      include: ['src/**/*.ts'],
      exclude: [
        'src/**/__tests__/**',
        'src/**/*.test.ts',
        'src/storage/sqlite-bun.ts',
        'src/index.ts',
        'src/core/types.ts',
      ],
      thresholds: {
        statements: 93,
        branches: 85,
        functions: 97,
        lines: 93,
      },
    },
  },
})
