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
        // Single-runtime adapters: not loadable in the default (Bun) coverage
        // run. Each is exercised in its own runtime — sqlite-bun via the Bun
        // smoke test (`bun run test:bun`), sqlite-node-builtin via the
        // `node-sqlite` CI job on Node — so neither can be measured here.
        'src/storage/sqlite-bun.ts',
        'src/storage/sqlite-node-builtin.ts',
        'src/index.ts',
        'src/core/types.ts',
      ],
      thresholds: {
        statements: 93,
        branches: 84,
        functions: 97,
        lines: 93,
      },
    },
  },
})
