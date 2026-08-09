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

// Event buffering + waiting/wake paths.
await storage.createRun(makeRun({ id: 'run_2' }))
const claimed2 = await storage.claimNextRun(['test'])
assert(claimed2, 'second claim should succeed')
assert(await storage.deliverEvent('run_2', 'e', { n: 1 }), 'deliverEvent should accept an existing run')
assert(!(await storage.deliverEvent('missing', 'e', {})), 'deliverEvent on missing run should return false')
const taken = await storage.takeEvent('run_2', 'e')
assert(taken && JSON.stringify(taken.payload) === JSON.stringify({ n: 1 }), 'takeEvent should return the buffered payload')
assert((await storage.takeEvent('run_2', 'e')) === null, 'takeEvent should return null when drained')
assert(await storage.waitRun('run_2', claimed2.leaseId, 'e', null), 'waitRun should suspend with the held lease')
assert((await storage.getRun('run_2'))?.status === 'waiting', 'run should be waiting')
await storage.deliverEvent('run_2', 'e', { n: 2 })
assert((await storage.claimNextRun(['test']))?.id === 'run_2', 'delivering an event should wake the waiting run')

// Durable schedules: the upsert cadence rule and the claim-and-advance
// transaction both hinge on affected-row counts, the same class of bug.
const scheduleAt = (nextRunAt: number, intervalMs = 100) => ({
  key: 'nightly',
  workflow: 'test',
  input: { olderThanDays: 30 },
  intervalMs,
  nextRunAt,
  createdAt: 0,
  updatedAt: 0,
})

await storage.upsertSchedule(scheduleAt(5_000))
assert(
  (await storage.upsertSchedule(scheduleAt(900_000))).nextRunAt === 5_000,
  're-registering with the same interval must preserve the cadence',
)
assert(
  (await storage.upsertSchedule(scheduleAt(900_000, 200))).nextRunAt === 900_000,
  'changing the interval must reset the cadence',
)

await storage.upsertSchedule(scheduleAt(1_000))
const due = await storage.claimDueSchedule(['test'], 1_250)
assert(due?.nextRunAt === 1_000, 'claim should report the occurrence it fired for')
assert(
  (await storage.listSchedules())[0]?.nextRunAt === 1_300,
  'claim should advance the stored schedule past now',
)
assert(
  (await storage.claimDueSchedule(['test'], 1_250)) === null,
  'an occurrence must only be claimable once',
)
assert(
  (await storage.claimDueSchedule(['elsewhere'], 5_000)) === null,
  'claim must filter by workflow name',
)
assert(await storage.deleteSchedule('nightly'), 'deleteSchedule should report the removal')
assert(!(await storage.deleteSchedule('nightly')), 'deleteSchedule on a missing key should return false')

storage.close()
// eslint-disable-next-line no-console
console.log('OK: sqlite-bun adapter change-count and schedule paths verified under Bun')
