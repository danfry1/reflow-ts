import { describe, it, afterEach } from 'vitest'
import { existsSync, unlinkSync } from 'node:fs'
import type { StorageAdapter } from '../../core/types'
import { MemoryStorage } from '../memory'
import { SQLiteStorage as BetterSqliteStorage } from '../sqlite-node'
import { SQLiteStorage as NodeSqliteStorage } from '../sqlite-node-builtin'
import { storageConformanceCases } from '../conformance'

/**
 * Runs the shared `StorageAdapter` contract against every adapter loadable
 * under Node.
 *
 * `sqlite-bun` is absent by necessity — `bun:sqlite` cannot load in this
 * runtime — so the same cases are driven against it by
 * `scripts/smoke-bun-adapter.ts`, which CI runs under Bun.
 */

// node:sqlite arrived in Node 22.5 and is unavailable on Bun / older Node.
const nodeSqliteAvailable = await import('node:sqlite').then(() => true, () => false)

const BETTER_SQLITE_PATH = '/tmp/reflow-conformance-better.db'
const NODE_SQLITE_PATH = '/tmp/reflow-conformance-node.db'

function removeIfPresent(path: string): void {
  if (existsSync(path)) unlinkSync(path)
}

interface AdapterUnderTest {
  readonly name: string
  readonly skip: boolean
  create(): Promise<StorageAdapter>
}

const adapters: readonly AdapterUnderTest[] = [
  {
    name: 'MemoryStorage',
    skip: false,
    create: async () => {
      const storage = new MemoryStorage()
      await storage.initialize()
      return storage
    },
  },
  {
    name: 'SQLiteStorage (better-sqlite3)',
    skip: false,
    create: async () => {
      // A file per case, so no case can observe another's rows.
      removeIfPresent(BETTER_SQLITE_PATH)
      const storage = new BetterSqliteStorage(BETTER_SQLITE_PATH)
      await storage.initialize()
      return storage
    },
  },
  {
    name: 'SQLiteStorage (node:sqlite)',
    skip: !nodeSqliteAvailable,
    create: async () => {
      removeIfPresent(NODE_SQLITE_PATH)
      const storage = new NodeSqliteStorage(NODE_SQLITE_PATH)
      await storage.initialize()
      return storage
    },
  },
]

for (const adapter of adapters) {
  describe.skipIf(adapter.skip)(`storage contract: ${adapter.name}`, () => {
    let open: StorageAdapter | null = null

    afterEach(() => {
      open?.close()
      open = null
    })

    for (const testCase of storageConformanceCases) {
      it(testCase.name, async () => {
        const storage = await adapter.create()
        open = storage
        await testCase.run(storage)
      })
    }
  })
}

afterEach(() => {
  removeIfPresent(BETTER_SQLITE_PATH)
  removeIfPresent(NODE_SQLITE_PATH)
})
