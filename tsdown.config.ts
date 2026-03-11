import { defineConfig } from 'tsdown'

export default defineConfig({
  entry: {
    index: 'src/index.ts',
    'storage/sqlite-node': 'src/storage/sqlite-node.ts',
    'storage/sqlite-bun': 'src/storage/sqlite-bun.ts',
    'test/index': 'src/test/index.ts',
  },
  format: 'esm',
  dts: true,
  sourcemap: true,
  clean: true,
  deps: { neverBundle: ['better-sqlite3', 'bun:sqlite'] },
})
