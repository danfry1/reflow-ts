/**
 * Bun-native smoke test for the `reflow-ts/sqlite-bun` adapter.
 *
 * The Vitest suite runs under Node, where `bun:sqlite` cannot load, so the Bun
 * adapter is otherwise uncovered. This script exercises the change-count paths
 * (claim guard, heartbeat, status updates) that depend on reading affected-row
 * counts from `run()` — the class of bug that `db.changes` (undefined on Bun)
 * silently broke. Run with `bun run scripts/smoke-bun-adapter.ts`; it throws on
 * the first failed assertion so CI fails loudly.
 */
import { SQLiteStorage } from '../src/storage/sqlite-bun'
import type { WorkflowRun } from '../src/core/types'

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(`smoke-bun-adapter: ${message}`)
}

function makeRun(overrides: Partial<WorkflowRun> = {}): WorkflowRun {
  const now = Date.now()
  return {
    id: 'run_1',
    workflow: 'test',
    input: {},
    idempotencyKey: null,
    status: 'pending',
    createdAt: now,
    updatedAt: now,
    ...overrides,
  }
}

const storage = new SQLiteStorage(':memory:')
await storage.initialize()

// Claim guard: a claimed run must not be claimable again (depends on the
// UPDATE's affected-row count, which `db.changes` reported as undefined).
await storage.createRun(makeRun({ id: 'run_1' }))
const claimed = await storage.claimNextRun(['test'])
assert(claimed, 'first claim should succeed')
assert((await storage.claimNextRun(['test'])) === null, 'double-claim must be prevented')

// Heartbeat must reflect lease ownership.
assert(await storage.heartbeatRun('run_1', claimed.leaseId), 'heartbeat with held lease should succeed')
assert(!(await storage.heartbeatRun('run_1', 'wrong-lease')), 'heartbeat with wrong lease should fail')

// Lease-checked status update.
assert(
  !(await storage.updateClaimedRunStatus('run_1', 'wrong-lease', 'completed')),
  'claimed-status update with wrong lease should fail',
)
assert(
  await storage.updateClaimedRunStatus('run_1', claimed.leaseId, 'completed'),
  'claimed-status update with held lease should succeed',
)
assert((await storage.getRun('run_1'))?.status === 'completed', 'run should be completed')

// updateRunStatus existence reporting.
assert(!(await storage.updateRunStatus('missing', 'cancelled')), 'updateRunStatus on missing run should return false')

storage.close()
// eslint-disable-next-line no-console
console.log('OK: sqlite-bun adapter change-count paths verified under Bun')
