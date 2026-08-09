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
        // Test infrastructure, not library code. It lives outside __tests__ so
        // the Bun smoke script can import it, but it ships with neither.
        'src/storage/conformance.ts',
        // Single-runtime adapters: not loadable in the default (Bun) coverage
        // run. Each is exercised in its own runtime — sqlite-bun via the Bun
        // smoke test (`bun run test:bun`), sqlite-node-builtin via the
        // `node-sqlite` CI job on Node — so neither can be measured here.
        'src/storage/sqlite-bun.ts',
        'src/storage/sqlite-node-builtin.ts',
        'src/index.ts',
        // Type-only modules: these compile to nothing, so the instrumenter
        // reports them as 0% covered no matter how thoroughly the types are used.
        'src/core/types.ts',
        'src/core/execution/types.ts',
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
