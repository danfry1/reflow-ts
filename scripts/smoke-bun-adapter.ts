/**
 * Runs the shared storage contract against the `reflow-ts/sqlite-bun` adapter.
 *
 * The Vitest suite runs under Node, where `bun:sqlite` cannot load, so this
 * adapter is invisible to the main test run — which is how `bun:sqlite`
 * reporting affected-row counts on the `run()` result rather than on
 * `Database.changes` went unnoticed until it had broken every heartbeat and
 * claim guard on Bun.
 *
 * The cases come from `src/storage/conformance.ts`, the same list the Node
 * adapters are held to, so this adapter cannot quietly drift from them again.
 * Run with `bun run test:bun`; it exits non-zero on the first failure.
 */
import { SQLiteStorage } from '../src/storage/sqlite-bun'
import { storageConformanceCases } from '../src/storage/conformance'

let failures = 0

for (const testCase of storageConformanceCases) {
  // A fresh in-memory database per case, so none can observe another's rows.
  const storage = new SQLiteStorage(':memory:')
  await storage.initialize()

  try {
    await testCase.run(storage)
  } catch (error) {
    failures++
    const detail = error instanceof Error ? error.message : String(error)
    // eslint-disable-next-line no-console
    console.error(`FAIL  ${testCase.name}\n      ${detail}`)
  } finally {
    storage.close()
  }
}

if (failures > 0) {
  // eslint-disable-next-line no-console
  console.error(
    `\nsqlite-bun: ${failures} of ${storageConformanceCases.length} contract cases failed`,
  )
  process.exit(1)
}

// eslint-disable-next-line no-console
console.log(
  `OK: sqlite-bun satisfies all ${storageConformanceCases.length} storage contract cases under Bun`,
)
