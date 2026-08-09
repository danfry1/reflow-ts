import { randomUUID } from 'node:crypto'
import { existsSync, unlinkSync } from 'node:fs'
import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import type { StepResult, WorkflowRun, WorkflowSchedule } from '../../core/types'
import { SQLiteStorage } from '../sqlite-node-builtin'
import { at } from '../../__tests__/helpers'

// node:sqlite was added in Node 22.5 and is unavailable on Bun / older Node.
// Skip the whole suite where it can't load so the default (Bun) test run is green;
// the dedicated Node CI job exercises it for real.
const nodeSqliteAvailable = await import('node:sqlite').then(() => true, () => false)

function expectPresent<T>(value: T | null | undefined): T {
  expect(value).not.toBeNull()
  expect(value).not.toBeUndefined()
  if (value == null) {
    throw new Error('Expected value to be present')
  }
  return value
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

function makeStep(overrides: Partial<StepResult> = {}): StepResult {
  const now = Date.now()
  return {
    id: 'step_1',
    runId: 'run_1',
    name: 'step-a',
    status: 'completed',
    output: { result: true },
    error: null,
    attempts: 1,
    createdAt: now,
    updatedAt: now,
    ...overrides,
  }
}

describe.skipIf(!nodeSqliteAvailable)('SQLiteStorage (node:sqlite)', () => {
  let dbPath: string
  let storage: SQLiteStorage

  beforeEach(async () => {
    dbPath = `/tmp/reflow-nodesqlite-${randomUUID()}.db`
    storage = new SQLiteStorage(dbPath)
    await storage.initialize()
  })

  afterEach(() => {
    storage.close()
    for (const suffix of ['', '-wal', '-shm', '-journal']) {
      const path = `${dbPath}${suffix}`
      if (existsSync(path)) unlinkSync(path)
    }
  })

  it('initialize is idempotent', async () => {
    await expect(storage.initialize()).resolves.toBeUndefined()
  })

  it('creates and reads back a run', async () => {
    const run = makeRun()
    const { run: stored, created } = await storage.createRun(run)
    expect(created).toBe(true)
    expect(stored.id).toBe('run_1')

    const fetched = expectPresent(await storage.getRun('run_1'))
    expect(fetched.workflow).toBe('test')
    expect(fetched.status).toBe('pending')
  })

  it('returns null for a missing run', async () => {
    expect(await storage.getRun('nope')).toBeNull()
  })

  it('round-trips a Date in the input', async () => {
    const when = new Date('2026-01-02T03:04:05.000Z')
    await storage.createRun(makeRun({ input: { when } }))
    const fetched = expectPresent(await storage.getRun('run_1'))
    const input = fetched.input as { when: Date }
    expect(input.when).toBeInstanceOf(Date)
    expect(input.when.toISOString()).toBe(when.toISOString())
  })

  it('idempotency: same key + workflow returns the existing run without duplicating', async () => {
    const first = await storage.createRun(makeRun({ id: 'a', idempotencyKey: 'k' }))
    expect(first.created).toBe(true)

    const second = await storage.createRun(makeRun({ id: 'b', idempotencyKey: 'k' }))
    expect(second.created).toBe(false)
    expect(second.run.id).toBe('a') // the original, not 'b'
  })

  it('claims a pending run and returns a lease', async () => {
    await storage.createRun(makeRun())
    const claimed = expectPresent(await storage.claimNextRun(['test']))
    expect(claimed.id).toBe('run_1')
    expect(claimed.status).toBe('running')
    expect(claimed.leaseId).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/)

    // A second claim finds nothing — the run is no longer pending.
    expect(await storage.claimNextRun(['test'])).toBeNull()
  })

  it('returns null when there is nothing to claim', async () => {
    expect(await storage.claimNextRun(['test'])).toBeNull()
    expect(await storage.claimNextRun([])).toBeNull()
  })

  it('reclaims a stale running run but not a fresh one', async () => {
    await storage.createRun(makeRun())
    const claimed = expectPresent(await storage.claimNextRun(['test']))

    // Fresh: not reclaimable when staleBefore is in the past.
    expect(await storage.claimNextRun(['test'], Date.now() - 60_000)).toBeNull()

    // Stale: reclaimable when staleBefore is in the future.
    const reclaimed = expectPresent(await storage.claimNextRun(['test'], Date.now() + 60_000))
    expect(reclaimed.id).toBe('run_1')
    expect(reclaimed.leaseId).not.toBe(claimed.leaseId) // a new lease
  })

  it('heartbeat succeeds with the held lease and fails otherwise', async () => {
    await storage.createRun(makeRun())
    const claimed = expectPresent(await storage.claimNextRun(['test']))

    expect(await storage.heartbeatRun('run_1', claimed.leaseId)).toBe(true)
    expect(await storage.heartbeatRun('run_1', 'wrong-lease')).toBe(false)
  })

  it('saves and reads step results in creation order', async () => {
    await storage.createRun(makeRun())
    await storage.saveStepResult(makeStep({ id: 's1', name: 'a', createdAt: 1 }))
    await storage.saveStepResult(makeStep({ id: 's2', name: 'b', createdAt: 2 }))

    const steps = await storage.getStepResults('run_1')
    expect(steps.map((s) => s.name)).toStrictEqual(['a', 'b'])
    expect(at(steps, 0).output).toStrictEqual({ result: true })
  })

  it('persists an undefined step output', async () => {
    await storage.createRun(makeRun())
    await storage.saveStepResult(makeStep({ output: undefined }))
    const steps = await storage.getStepResults('run_1')
    expect(at(steps, 0).output).toBeUndefined()
  })

  it('upserts a step result on the same id', async () => {
    await storage.createRun(makeRun())
    await storage.saveStepResult(makeStep({ id: 's', status: 'failed', error: 'boom', output: null }))
    await storage.saveStepResult(makeStep({ id: 's', status: 'completed', error: null, output: { ok: 1 } }))

    const steps = await storage.getStepResults('run_1')
    expect(steps).toHaveLength(1)
    expect(at(steps, 0).status).toBe('completed')
    expect(at(steps, 0).output).toStrictEqual({ ok: 1 })
  })

  it('saveStepResult honors the lease check', async () => {
    await storage.createRun(makeRun())
    const claimed = expectPresent(await storage.claimNextRun(['test']))

    expect(await storage.saveStepResult(makeStep({ id: 's1' }), claimed.leaseId)).toBe(true)
    expect(await storage.saveStepResult(makeStep({ id: 's2' }), 'wrong-lease')).toBe(false)

    const steps = await storage.getStepResults('run_1')
    expect(steps.map((s) => s.id)).toStrictEqual(['s1'])
  })

  it('updateRunStatus updates status and clears the lease', async () => {
    await storage.createRun(makeRun())
    await storage.claimNextRun(['test'])

    expect(await storage.updateRunStatus('run_1', 'cancelled')).toBe(true)
    const run = expectPresent(await storage.getRun('run_1'))
    expect(run.status).toBe('cancelled')

    // Lease was cleared, so a claimed-status update no longer applies.
    expect(await storage.updateClaimedRunStatus('run_1', 'any', 'completed')).toBe(false)
  })

  it('updateClaimedRunStatus only applies with the matching lease', async () => {
    await storage.createRun(makeRun())
    const claimed = expectPresent(await storage.claimNextRun(['test']))

    expect(await storage.updateClaimedRunStatus('run_1', 'wrong', 'completed')).toBe(false)
    expect(await storage.updateClaimedRunStatus('run_1', claimed.leaseId, 'completed')).toBe(true)

    const run = expectPresent(await storage.getRun('run_1'))
    expect(run.status).toBe('completed')
  })

  it('sleepRun suspends a run and rejects a non-matching lease', async () => {
    await storage.createRun(makeRun({ id: 'run_1' }))
    const claimed = expectPresent(await storage.claimNextRun(['test']))

    expect(await storage.sleepRun('run_1', 'wrong', Date.now() + 60_000)).toBe(false)
    expect(await storage.sleepRun('run_1', claimed.leaseId, Date.now() + 60_000)).toBe(true)
    expect(expectPresent(await storage.getRun('run_1')).status).toBe('sleeping')

    expect(await storage.claimNextRun(['test'])).toBeNull()
  })

  it('claimNextRun reclaims a sleeping run after its wake time', async () => {
    await storage.createRun(makeRun({ id: 'run_1' }))
    const claimed = expectPresent(await storage.claimNextRun(['test']))
    expect(await storage.sleepRun('run_1', claimed.leaseId, Date.now() - 1)).toBe(true)

    const woken = expectPresent(await storage.claimNextRun(['test']))
    expect(woken.id).toBe('run_1')
    expect(woken.status).toBe('running')
    expect(woken.leaseId).not.toBe(claimed.leaseId)
  })

  it('delivers/takes events FIFO and waitRun + deliverEvent wake a run', async () => {
    await storage.createRun(makeRun({ id: 'run_1' }))
    expect(await storage.deliverEvent('nope', 'e', {})).toBe(false)
    expect(await storage.deliverEvent('run_1', 'e', { n: 1 })).toBe(true)
    expect(await storage.deliverEvent('run_1', 'e', { n: 2 })).toBe(true)
    expect(await storage.takeEvent('run_1', 'e')).toStrictEqual({ payload: { n: 1 } })
    expect(await storage.takeEvent('run_1', 'e')).toStrictEqual({ payload: { n: 2 } })
    expect(await storage.takeEvent('run_1', 'e')).toBeNull()

    const claimed = expectPresent(await storage.claimNextRun(['test']))
    expect(await storage.waitRun('run_1', claimed.leaseId, 'e', null)).toBe(true)
    expect(expectPresent(await storage.getRun('run_1')).status).toBe('waiting')
    await storage.deliverEvent('run_1', 'e', { ok: true })
    expect(expectPresent(await storage.claimNextRun(['test'])).id).toBe('run_1')
  })

  it('waitRun stays reclaimable when a matching event is already buffered', async () => {
    await storage.createRun(makeRun({ id: 'run_1' }))
    const claimed = expectPresent(await storage.claimNextRun(['test']))
    await storage.deliverEvent('run_1', 'e', { ok: true })
    expect(await storage.waitRun('run_1', claimed.leaseId, 'e', null)).toBe(true)
    expect(expectPresent(await storage.getRun('run_1')).status).toBe('pending')
  })

  describe('schedules', () => {
    const makeSchedule = (overrides: Partial<WorkflowSchedule> = {}): WorkflowSchedule => {
      const now = Date.now()
      return {
        key: 'nightly',
        workflow: 'test',
        input: { olderThanDays: 30 },
        intervalMs: 60_000,
        nextRunAt: now + 60_000,
        createdAt: now,
        updatedAt: now,
        ...overrides,
      }
    }

    it('registers, lists, and deletes a schedule', async () => {
      await storage.upsertSchedule(makeSchedule())
      expect(await storage.listSchedules()).toHaveLength(1)

      expect(await storage.deleteSchedule('nightly')).toBe(true)
      expect(await storage.deleteSchedule('nightly')).toBe(false)
      expect(await storage.listSchedules()).toStrictEqual([])
    })

    it('preserves the cadence on re-registration, resets it when the interval changes', async () => {
      const first = await storage.upsertSchedule(makeSchedule({ nextRunAt: 5_000 }))

      expect((await storage.upsertSchedule(makeSchedule({ nextRunAt: 900_000 }))).nextRunAt)
        .toBe(first.nextRunAt)
      expect((await storage.upsertSchedule(
        makeSchedule({ intervalMs: 120_000, nextRunAt: 900_000 }),
      )).nextRunAt).toBe(900_000)
    })

    it('claims a due occurrence exactly once, advancing past now', async () => {
      await storage.upsertSchedule(makeSchedule({ nextRunAt: 1_000, intervalMs: 100 }))

      const claimed = await storage.claimDueSchedule(['test'], 1_250)
      expect(claimed?.nextRunAt).toBe(1_000)
      expect(at(await storage.listSchedules(), 0).nextRunAt).toBe(1_300)
      expect(await storage.claimDueSchedule(['test'], 1_250)).toBeNull()
    })

    it('only claims schedules for the given workflows', async () => {
      await storage.upsertSchedule(makeSchedule({ workflow: 'elsewhere', nextRunAt: 1_000 }))

      expect(await storage.claimDueSchedule(['test'], 5_000)).toBeNull()
      expect(await storage.claimDueSchedule(['elsewhere'], 5_000)).not.toBeNull()
    })
  })

})
