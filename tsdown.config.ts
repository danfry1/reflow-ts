import { defineConfig } from 'tsdown'

export default defineConfig({
  entry: {
    index: 'src/index.ts',
    'storage/sqlite-node': 'src/storage/sqlite-node.ts',
    'storage/sqlite-bun': 'src/storage/sqlite-bun.ts',
    'storage/sqlite-node-builtin': 'src/storage/sqlite-node-builtin.ts',
    'test/index': 'src/test/index.ts',
    conformance: 'src/storage/conformance.ts',
  },
  format: 'esm',
  dts: true,
  sourcemap: true,
  clean: true,
  deps: { neverBundle: ['better-sqlite3', 'bun:sqlite', 'node:sqlite'] },
})
