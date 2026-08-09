import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import Database from 'better-sqlite3'
import { SQLiteStorage } from '../sqlite-node'
import { unlinkSync, existsSync } from 'node:fs'
import type { WorkflowRun, StepResult, WorkflowSchedule } from '../../core/types'
import { at } from '../../__tests__/helpers'

const DB_PATH = '/tmp/reflow-test.db'

function expectPresent<T>(value: T | null | undefined): T {
  expect(value).not.toBeNull()
  expect(value).not.toBeUndefined()

  if (value == null) {
    throw new Error('Expected value to be present')
  }

  return value
}

function makeRun(overrides: Partial<WorkflowRun> = {}): WorkflowRun {
  return {
    id: 'run_1',
    workflow: 'test',
    input: {},
    idempotencyKey: null,
    status: 'pending',
    createdAt: Date.now(),
    updatedAt: Date.now(),
    ...overrides,
  }
}

function makeStep(overrides: Partial<StepResult> = {}): StepResult {
  return {
    id: 'step_1',
    runId: 'run_1',
    name: 'step-a',
    status: 'completed',
    output: { result: true },
    error: null,
    attempts: 1,
    createdAt: Date.now(),
    updatedAt: Date.now(),
    ...overrides,
  }
}

describe('SQLiteStorage', () => {
  let storage: SQLiteStorage

  beforeEach(async () => {
    if (existsSync(DB_PATH)) unlinkSync(DB_PATH)
    storage = new SQLiteStorage(DB_PATH)
    await storage.initialize()
  })

  afterEach(() => {
    vi.restoreAllMocks()
    storage.close()
    if (existsSync(DB_PATH)) unlinkSync(DB_PATH)
  })

  describe('initialize', () => {
    it('creates tables without error', () => {
      // If we got here, initialization succeeded in beforeEach
      expect(true).toBe(true)
    })

    it('is idempotent — calling initialize twice does not error', async () => {
      await storage.initialize()
    })
  })

  describe('createRun / claimNextRun', () => {
    it('returns an existing run when the same idempotency key is reused', async () => {
      const first = await storage.createRun(makeRun({ id: 'run_1', idempotencyKey: 'same-key' }))
      const second = await storage.createRun(makeRun({ id: 'run_2', idempotencyKey: 'same-key' }))

      expect(first.created).toBe(true)
      expect(second.created).toBe(false)
      expect(second.run.id).toBe('run_1')
    })

    it('returns the existing run when an idempotent insert loses a unique-key race', async () => {
      await storage.createRun(makeRun({ id: 'run_1', idempotencyKey: 'same-key', input: { x: 1 } }))

      const db = (storage as any).db
      const originalPrepare = db.prepare.bind(db)
      let firstLookupMissed = false

      vi.spyOn(db, 'prepare').mockImplementation(((sql: unknown) => {
        const sqlText = String(sql)
        const statement = originalPrepare(sqlText)
        if (!sqlText.includes('SELECT * FROM workflow_runs') || !sqlText.includes('idempotency_key')) {
          return statement
        }

        return new Proxy(statement, {
          get(target, property, receiver) {
            if (property === 'get') {
              return (...args: unknown[]) => {
                if (!firstLookupMissed) {
                  firstLookupMissed = true
                  return undefined
                }

                return Reflect.get(target, property, receiver).apply(target, args)
              }
            }

            const value = Reflect.get(target, property, receiver)
            return typeof value === 'function' ? value.bind(target) : value
          },
        })
      }) as any)

      const result = await storage.createRun(makeRun({ id: 'run_2', idempotencyKey: 'same-key', input: { x: 1 } }))

      expect(result.created).toBe(false)
      expect(result.run.id).toBe('run_1')
    })

    it('creates and claims a run with serialized input', async () => {
      await storage.createRun(makeRun({ id: 'run_1', input: { x: 1 } }))
      const claimed = expectPresent(await storage.claimNextRun(['test']))

      expect(claimed.id).toBe('run_1')
      expect(claimed.status).toBe('running')
      expect(claimed.input).toStrictEqual({ x: 1 })
    })

    it('returns null when no workflow names are provided', async () => {
      await storage.createRun(makeRun({ id: 'run_1' }))
      const claimed = await storage.claimNextRun([])
      expect(claimed).toBeNull()
    })

    it('returns null when no pending runs exist', async () => {
      const claimed = await storage.claimNextRun(['test'])
      expect(claimed).toBeNull()
    })

    it('does not double-claim', async () => {
      await storage.createRun(makeRun({ id: 'run_1' }))
      await storage.claimNextRun(['test'])
      const second = await storage.claimNextRun(['test'])
      expect(second).toBeNull()
    })

    it('allows only one claim across separate SQLite connections', async () => {
      const storage2 = new SQLiteStorage(DB_PATH)
      await storage2.initialize()

      try {
        await storage.createRun(makeRun({ id: 'run_1' }))

        const [claim1, claim2] = await Promise.all([
          storage.claimNextRun(['test']),
          storage2.claimNextRun(['test']),
        ])

        const claims = [claim1, claim2].filter((claim): claim is NonNullable<typeof claim> => claim !== null)
        expect(claims).toHaveLength(1)
        expect(at(claims, 0).id).toBe('run_1')
      } finally {
        storage2.close()
      }
    })

    it('reclaims stale running runs when a stale threshold is provided', async () => {
      await storage.createRun(makeRun({ id: 'stale', status: 'running', updatedAt: 1 }))

      const claimed = expectPresent(await storage.claimNextRun(['test'], 10))

      expect(claimed.id).toBe('stale')
      expect(claimed.status).toBe('running')
      expect(claimed.leaseId).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/)
    })

    it('claims in FIFO order (oldest first)', async () => {
      await storage.createRun(makeRun({ id: 'first', createdAt: 1000 }))
      await storage.createRun(makeRun({ id: 'second', createdAt: 2000 }))

      const claimed = expectPresent(await storage.claimNextRun(['test']))
      expect(claimed.id).toBe('first')
    })

    it('only claims runs matching the requested workflow names', async () => {
      await storage.createRun(makeRun({ id: 'run_1', workflow: 'alpha' }))

      const claimed = await storage.claimNextRun(['beta'])
      expect(claimed).toBeNull()
    })

    it('handles multiple workflows in the same database', async () => {
      await storage.createRun(makeRun({ id: 'run_a', workflow: 'alpha' }))
      await storage.createRun(makeRun({ id: 'run_b', workflow: 'beta' }))

      const claimedAlpha = expectPresent(await storage.claimNextRun(['alpha']))
      const claimedBeta = expectPresent(await storage.claimNextRun(['beta']))

      expect(claimedAlpha.id).toBe('run_a')
      expect(claimedBeta.id).toBe('run_b')
    })

    it('roundtrips complex nested JSON input', async () => {
      const complexInput = {
        user: { name: 'Alice', roles: ['admin', 'user'] },
        metadata: { nested: { deeply: { value: 42 } } },
        tags: [1, 'two', null, true],
      }

      await storage.createRun(makeRun({ id: 'run_1', input: complexInput }))
      const claimed = expectPresent(await storage.claimNextRun(['test']))
      expect(claimed.input).toStrictEqual(complexInput)
    })
  })

  describe('saveStepResult / getStepResults', () => {
    it('rejects lease-bound writes when the lease does not match', async () => {
      await storage.createRun(makeRun({ id: 'run_1' }))
      const claimed = expectPresent(await storage.claimNextRun(['test']))

      const saved = await storage.saveStepResult(makeStep(), 'wrong-lease')
      expect(saved).toBe(false)

      const savedWithLease = await storage.saveStepResult(makeStep(), claimed.leaseId)
      expect(savedWithLease).toBe(true)
    })

    it('saves and retrieves a step result', async () => {
      await storage.saveStepResult(makeStep({ output: { chargeId: 'ch_123' } }))

      const results = await storage.getStepResults('run_1')
      expect(results).toHaveLength(1)
      expect(at(results, 0).output).toStrictEqual({ chargeId: 'ch_123' })
      expect(at(results, 0).error).toBeNull()
    })

    it('returns an empty array for a run with no steps', async () => {
      const results = await storage.getStepResults('nonexistent')
      expect(results).toStrictEqual([])
    })

    it('preserves step order by creation time', async () => {
      await storage.saveStepResult(makeStep({ id: 's1', name: 'first', createdAt: 1000 }))
      await storage.saveStepResult(makeStep({ id: 's2', name: 'second', createdAt: 2000 }))
      await storage.saveStepResult(makeStep({ id: 's3', name: 'third', createdAt: 3000 }))

      const results = await storage.getStepResults('run_1')
      expect(results.map((s) => s.name)).toStrictEqual(['first', 'second', 'third'])
    })

    it('roundtrips null output (failed step)', async () => {
      await storage.saveStepResult(makeStep({
        id: 'step_1',
        status: 'failed',
        output: null,
        error: 'something went wrong',
      }))

      const results = await storage.getStepResults('run_1')
      expect(at(results, 0).output).toBeNull()
      expect(at(results, 0).error).toBe('something went wrong')
    })

    it('roundtrips null error (successful step)', async () => {
      await storage.saveStepResult(makeStep({
        id: 'step_1',
        status: 'completed',
        output: { ok: true },
        error: null,
      }))

      const results = await storage.getStepResults('run_1')
      expect(at(results, 0).error).toBeNull()
      expect(at(results, 0).output).toStrictEqual({ ok: true })
    })

    it('roundtrips falsy outputs without collapsing them to null', async () => {
      await storage.saveStepResult(makeStep({ id: 'step_false', output: false }))
      await storage.saveStepResult(makeStep({ id: 'step_zero', name: 'zero', output: 0 }))
      await storage.saveStepResult(makeStep({ id: 'step_empty', name: 'empty', output: '' }))

      const results = await storage.getStepResults('run_1')

      expect(at(results, 0).output).toBe(false)
      expect(at(results, 1).output).toBe(0)
      expect(at(results, 2).output).toBe('')
    })

    it('roundtrips undefined outputs through the persistence codec', async () => {
      await storage.saveStepResult(makeStep({ id: 'step_undefined', output: undefined }))

      const results = await storage.getStepResults('run_1')
      expect(at(results, 0).output).toBeUndefined()
    })

    it('upserts — INSERT OR REPLACE updates existing step', async () => {
      await storage.saveStepResult(makeStep({ id: 'step_1', status: 'failed', output: null, error: 'fail' }))
      await storage.saveStepResult(makeStep({ id: 'step_1', status: 'completed', output: { ok: true }, error: null }))

      const results = await storage.getStepResults('run_1')
      expect(results).toHaveLength(1)
      expect(at(results, 0).status).toBe('completed')
    })

    it('isolates step results by run id', async () => {
      await storage.saveStepResult(makeStep({ id: 's1', runId: 'run_1', name: 'a' }))
      await storage.saveStepResult(makeStep({ id: 's2', runId: 'run_2', name: 'b' }))

      expect(await storage.getStepResults('run_1')).toHaveLength(1)
      expect(await storage.getStepResults('run_2')).toHaveLength(1)
    })
  })

  describe('getRun', () => {
    it('returns a run by id', async () => {
      await storage.createRun(makeRun({ id: 'run_1', workflow: 'test', input: { x: 1 } }))
      const run = expectPresent(await storage.getRun('run_1'))

      expect(run.id).toBe('run_1')
      expect(run.workflow).toBe('test')
      expect(run.input).toStrictEqual({ x: 1 })
      expect(run.status).toBe('pending')
    })

    it('returns null for a nonexistent run', async () => {
      const run = await storage.getRun('nonexistent')
      expect(run).toBeNull()
    })

    it('reflects status updates', async () => {
      await storage.createRun(makeRun({ id: 'run_1' }))
      await storage.updateRunStatus('run_1', 'completed')

      const run = expectPresent(await storage.getRun('run_1'))
      expect(run.status).toBe('completed')
    })

    it('roundtrips complex JSON input', async () => {
      const input = { nested: { deep: [1, 'two', null] } }
      await storage.createRun(makeRun({ id: 'run_1', input }))

      const run = expectPresent(await storage.getRun('run_1'))
      expect(run.input).toStrictEqual(input)
    })
  })

  describe('heartbeatRun', () => {
    it('renews the lease on a running run', async () => {
      await storage.createRun(makeRun({ id: 'run_1' }))
      const claimed = expectPresent(await storage.claimNextRun(['test']))

      const ok = await storage.heartbeatRun('run_1', claimed.leaseId)
      expect(ok).toBe(true)
    })

    it('returns false when the lease does not match', async () => {
      await storage.createRun(makeRun({ id: 'run_1' }))
      await storage.claimNextRun(['test'])

      const ok = await storage.heartbeatRun('run_1', 'wrong-lease')
      expect(ok).toBe(false)
    })

    it('returns false for a nonexistent run', async () => {
      const ok = await storage.heartbeatRun('nonexistent', 'some-lease')
      expect(ok).toBe(false)
    })
  })

  describe('updateRunStatus', () => {
    it('updates the status of a run', async () => {
      await storage.createRun(makeRun({ id: 'run_1' }))
      await storage.updateRunStatus('run_1', 'completed')

      const claimed = await storage.claimNextRun(['test'])
      expect(claimed).toBeNull()
    })

    it('transitions through all status values', async () => {
      await storage.createRun(makeRun({ id: 'run_1' }))
      await storage.updateRunStatus('run_1', 'running')
      await storage.updateRunStatus('run_1', 'failed')
      // Should not throw
    })

    it('returns false for a nonexistent run', async () => {
      const ok = await storage.updateRunStatus('nonexistent', 'completed')
      expect(ok).toBe(false)
    })

    it('updates a claimed run only when the lease matches', async () => {
      await storage.createRun(makeRun({ id: 'run_1' }))
      const claimed = expectPresent(await storage.claimNextRun(['test']))

      expect(await storage.updateClaimedRunStatus('run_1', 'wrong-lease', 'completed')).toBe(false)
      expect(await storage.updateClaimedRunStatus('run_1', claimed.leaseId, 'completed')).toBe(true)
    })
  })

  describe('persistence', () => {
    it('data survives across separate SQLiteStorage instances', async () => {
      await storage.createRun(makeRun({ id: 'run_1', input: { hello: 'world' } }))
      await storage.saveStepResult(makeStep({ id: 'step_1', runId: 'run_1' }))
      storage.close()

      const storage2 = new SQLiteStorage(DB_PATH)
      await storage2.initialize()

      const claimed = expectPresent(await storage2.claimNextRun(['test']))
      expect(claimed.input).toStrictEqual({ hello: 'world' })

      const steps = await storage2.getStepResults('run_1')
      expect(steps).toHaveLength(1)

      storage2.close()

      // Re-assign for afterEach cleanup
      storage = new SQLiteStorage(DB_PATH)
      await storage.initialize()
    })
  })

  describe('sleepRun / wake', () => {
    it('suspends a claimed run; a non-matching lease cannot', async () => {
      await storage.createRun(makeRun({ id: 'run_1' }))
      const claimed = expectPresent(await storage.claimNextRun(['test']))

      expect(await storage.sleepRun('run_1', 'wrong-lease', Date.now() + 60_000)).toBe(false)
      expect(await storage.sleepRun('run_1', claimed.leaseId, Date.now() + 60_000)).toBe(true)
      expect(expectPresent(await storage.getRun('run_1')).status).toBe('sleeping')

      expect(await storage.claimNextRun(['test'])).toBeNull()
    })

    it('reclaims a sleeping run after its wake time with a fresh lease', async () => {
      await storage.createRun(makeRun({ id: 'run_1' }))
      const claimed = expectPresent(await storage.claimNextRun(['test']))
      expect(await storage.sleepRun('run_1', claimed.leaseId, Date.now() - 1)).toBe(true)

      const woken = expectPresent(await storage.claimNextRun(['test']))
      expect(woken.id).toBe('run_1')
      expect(woken.status).toBe('running')
      expect(woken.leaseId).not.toBe(claimed.leaseId)
    })

    it('persists the sleeping state across separate storage instances', async () => {
      await storage.createRun(makeRun({ id: 'run_1' }))
      const claimed = expectPresent(await storage.claimNextRun(['test']))
      await storage.sleepRun('run_1', claimed.leaseId, Date.now() - 1)
      storage.close()

      const storage2 = new SQLiteStorage(DB_PATH)
      await storage2.initialize()
      const woken = expectPresent(await storage2.claimNextRun(['test']))
      expect(woken.id).toBe('run_1')
      storage2.close()

      storage = new SQLiteStorage(DB_PATH)
      await storage.initialize()
    })
  })

  describe('wake_at migration', () => {
    it('adds wake_at to a pre-existing database that lacks it, then sleeps/wakes', async () => {
      // Close the beforeEach storage and recreate the DB with the *old* schema
      // (no wake_at column), as a database created before durable sleep would be.
      storage.close()
      if (existsSync(DB_PATH)) unlinkSync(DB_PATH)

      const legacy = new Database(DB_PATH)
      legacy.exec(`
        CREATE TABLE workflow_runs (
          id TEXT PRIMARY KEY, workflow TEXT NOT NULL, input TEXT NOT NULL,
          idempotency_key TEXT, lease_id TEXT, status TEXT NOT NULL,
          created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
        );
        CREATE TABLE workflow_steps (
          id TEXT PRIMARY KEY, run_id TEXT NOT NULL, name TEXT NOT NULL,
          status TEXT NOT NULL, output TEXT, error TEXT, attempts INTEGER DEFAULT 0,
          created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
        );
      `)
      legacy.prepare(
        `INSERT INTO workflow_runs (id, workflow, input, idempotency_key, lease_id, status, created_at, updated_at)
         VALUES ('legacy', 'test', '{}', NULL, NULL, 'pending', 1, 1)`,
      ).run()
      legacy.close()

      // initialize() must migrate the column in without dropping the existing row.
      storage = new SQLiteStorage(DB_PATH)
      await storage.initialize()

      const claimed = expectPresent(await storage.claimNextRun(['test']))
      expect(claimed.id).toBe('legacy')

      // The migrated column is usable for sleep/wake.
      expect(await storage.sleepRun('legacy', claimed.leaseId, Date.now() - 1)).toBe(true)
      const woken = expectPresent(await storage.claimNextRun(['test']))
      expect(woken.id).toBe('legacy')
    })

    it('is idempotent — initialize on an already-migrated database is a no-op', async () => {
      await storage.initialize()
      await storage.initialize()
      // Still functional.
      await storage.createRun(makeRun({ id: 'ok' }))
      expect(expectPresent(await storage.claimNextRun(['test'])).id).toBe('ok')
    })
  })

  describe('waitRun / events', () => {
    it('delivers and takes events FIFO; returns null when none remain', async () => {
      await storage.createRun(makeRun({ id: 'run_1' }))

      expect(await storage.deliverEvent('run_1', 'e', { n: 1 })).toBe(true)
      expect(await storage.deliverEvent('run_1', 'e', { n: 2 })).toBe(true)

      expect(await storage.takeEvent('run_1', 'e')).toStrictEqual({ payload: { n: 1 } })
      expect(await storage.takeEvent('run_1', 'e')).toStrictEqual({ payload: { n: 2 } })
      expect(await storage.takeEvent('run_1', 'e')).toBeNull()
    })

    it('deliverEvent returns false for a missing run', async () => {
      expect(await storage.deliverEvent('nope', 'e', {})).toBe(false)
    })

    it('waitRun suspends, and a later delivery wakes it', async () => {
      await storage.createRun(makeRun({ id: 'run_1' }))
      const claimed = expectPresent(await storage.claimNextRun(['test']))

      expect(await storage.waitRun('run_1', 'wrong', 'e', null)).toBe(false)
      expect(await storage.waitRun('run_1', claimed.leaseId, 'e', null)).toBe(true)
      expect(expectPresent(await storage.getRun('run_1')).status).toBe('waiting')
      expect(await storage.claimNextRun(['test'])).toBeNull()

      await storage.deliverEvent('run_1', 'e', { ok: true })
      const woken = expectPresent(await storage.claimNextRun(['test']))
      expect(woken.id).toBe('run_1')
    })

    it('waitRun stays reclaimable when a matching event is already buffered (race)', async () => {
      await storage.createRun(makeRun({ id: 'run_1' }))
      const claimed = expectPresent(await storage.claimNextRun(['test']))
      await storage.deliverEvent('run_1', 'e', { ok: true })

      expect(await storage.waitRun('run_1', claimed.leaseId, 'e', null)).toBe(true)
      expect(expectPresent(await storage.getRun('run_1')).status).toBe('pending')
    })

    it('claim reclaims a waiting run after its timeout deadline', async () => {
      await storage.createRun(makeRun({ id: 'run_1' }))
      const claimed = expectPresent(await storage.claimNextRun(['test']))
      await storage.waitRun('run_1', claimed.leaseId, 'e', Date.now() - 1)

      const woken = expectPresent(await storage.claimNextRun(['test']))
      expect(woken.id).toBe('run_1')
      expect(woken.leaseId).not.toBe(claimed.leaseId)
    })
  })

  describe('cron migration from 0.6', () => {
    it('rebuilds a 0.6-shaped schedules table so cron rows can be stored', async () => {
      // 0.6.0 shipped workflow_schedules with `interval_ms INTEGER NOT NULL`
      // and no cron column. Adding cron alone is not enough — a cron schedule
      // leaves interval_ms null, which that constraint rejects — so the table
      // is rebuilt. This is the published schema, so the path is real.
      storage.close()
      if (existsSync(DB_PATH)) unlinkSync(DB_PATH)

      const legacy = new Database(DB_PATH)
      legacy.exec(`
        CREATE TABLE workflow_schedules (
          key          TEXT PRIMARY KEY,
          workflow     TEXT NOT NULL,
          input        TEXT NOT NULL,
          interval_ms  INTEGER NOT NULL,
          next_run_at  INTEGER NOT NULL,
          created_at   INTEGER NOT NULL,
          updated_at   INTEGER NOT NULL
        );
      `)
      legacy.prepare(
        `INSERT INTO workflow_schedules (key, workflow, input, interval_ms, next_run_at, created_at, updated_at)
         VALUES ('legacy', 'test', '{"olderThanDays":30}', 60000, 5000, 1, 1)`,
      ).run()
      legacy.close()

      storage = new SQLiteStorage(DB_PATH)
      await storage.initialize()

      // The existing interval schedule survives the rebuild intact.
      const migrated = at(await storage.listSchedules(), 0)
      expect(migrated.key).toBe('legacy')
      expect(migrated.recurrence).toStrictEqual({ kind: 'interval', intervalMs: 60_000 })
      expect(migrated.nextRunAt).toBe(5_000)
      expect(migrated.input).toStrictEqual({ olderThanDays: 30 })

      // ...and a cron schedule, impossible under the old NOT NULL, now stores.
      await storage.upsertSchedule({
        key: 'nightly',
        workflow: 'test',
        input: {},
        recurrence: { kind: 'cron', expression: '0 9 * * *' },
        nextRunAt: 10_000,
        createdAt: 1,
        updatedAt: 1,
      })

      const schedules = await storage.listSchedules()
      expect(schedules).toHaveLength(2)
      expect(at(schedules, 1).recurrence).toStrictEqual({ kind: 'cron', expression: '0 9 * * *' })
    })

    it('is idempotent — initializing twice does not rebuild again or lose rows', async () => {
      await storage.upsertSchedule({
        key: 'keep', workflow: 'test', input: {},
        recurrence: { kind: 'cron', expression: '0 9 * * *' },
        nextRunAt: 10_000, createdAt: 1, updatedAt: 1,
      })

      await storage.initialize()
      await storage.initialize()

      expect(await storage.listSchedules()).toHaveLength(1)
    })
  })

  describe('schedules', () => {
    const makeSchedule = (overrides: Partial<WorkflowSchedule> = {}): WorkflowSchedule => {
      const now = Date.now()
      return {
        key: 'nightly',
        workflow: 'test',
        input: { olderThanDays: 30 },
        recurrence: { kind: 'interval', intervalMs: 60_000 },
        nextRunAt: now + 60_000,
        createdAt: now,
        updatedAt: now,
        ...overrides,
      }
    }

    it('survives closing and reopening the database', async () => {
      // The reason schedules moved into storage at all.
      await storage.upsertSchedule(makeSchedule({ nextRunAt: 1_000 }))
      storage.close()

      storage = new SQLiteStorage(DB_PATH)
      await storage.initialize()

      const schedules = await storage.listSchedules()
      expect(schedules).toHaveLength(1)
      expect(at(schedules, 0).input).toStrictEqual({ olderThanDays: 30 })
      expect(await storage.claimDueSchedule(['test'], 5_000)).not.toBeNull()
    })
  })

  describe('listRuns', () => {
    it('returns runs most-recent-first', async () => {
      await storage.createRun(makeRun({ id: 'a', createdAt: 1 }))
      await storage.createRun(makeRun({ id: 'b', createdAt: 2 }))
      await storage.createRun(makeRun({ id: 'c', createdAt: 3 }))

      const runs = await storage.listRuns()
      expect(runs.map((r) => r.id)).toEqual(['c', 'b', 'a'])
    })

    it('filters by status', async () => {
      await storage.createRun(makeRun({ id: 'a', status: 'completed', createdAt: 1 }))
      await storage.createRun(makeRun({ id: 'b', status: 'failed', createdAt: 2 }))

      const failed = await storage.listRuns({ status: 'failed' })
      expect(failed.map((r) => r.id)).toEqual(['b'])
    })

    it('filters by workflow', async () => {
      await storage.createRun(makeRun({ id: 'a', workflow: 'alpha', createdAt: 1 }))
      await storage.createRun(makeRun({ id: 'b', workflow: 'beta', createdAt: 2 }))

      const alpha = await storage.listRuns({ workflow: 'alpha' })
      expect(alpha.map((r) => r.id)).toEqual(['a'])
    })

    it('applies limit and before for pagination', async () => {
      await storage.createRun(makeRun({ id: 'a', createdAt: 1 }))
      await storage.createRun(makeRun({ id: 'b', createdAt: 2 }))
      await storage.createRun(makeRun({ id: 'c', createdAt: 3 }))

      const page1 = await storage.listRuns({ limit: 2 })
      expect(page1.map((r) => r.id)).toEqual(['c', 'b'])

      const page2 = await storage.listRuns({ limit: 2, before: at(page1, page1.length - 1).createdAt })
      expect(page2.map((r) => r.id)).toEqual(['a'])
    })

    it('paginates without losing rows when createdAt is tied, via the (before, beforeId) cursor', async () => {
      await storage.createRun(makeRun({ id: 'a', createdAt: 1000 }))
      await storage.createRun(makeRun({ id: 'b', createdAt: 1000 }))
      await storage.createRun(makeRun({ id: 'c', createdAt: 1000 }))

      const page1 = await storage.listRuns({ limit: 2 })
      const last = at(page1, page1.length - 1)
      const page2 = await storage.listRuns({ limit: 2, before: last.createdAt, beforeId: last.id })

      expect(page1.length + page2.length).toBe(3)
      expect([...page1, ...page2].map((r) => r.id).sort()).toEqual(['a', 'b', 'c'])
    })

    it('returns an empty array when nothing matches', async () => {
      expect(await storage.listRuns({ status: 'completed' })).toEqual([])
    })
  })

  describe('requeueRun', () => {
    it('resets a failed run to pending, drops failed steps, and is claimable again', async () => {
      await storage.createRun(makeRun({ id: 'run_1', status: 'failed' }))
      await storage.saveStepResult(makeStep({ id: 's1', name: 'ok', status: 'completed' }))
      await storage.saveStepResult(makeStep({ id: 's2', name: 'boom', status: 'failed', output: null, error: 'x' }))

      expect(await storage.requeueRun('run_1')).toBe(true)
      expect(expectPresent(await storage.getRun('run_1')).status).toBe('pending')

      const steps = await storage.getStepResults('run_1')
      expect(steps.map((s) => s.name)).toEqual(['ok'])

      const claimed = expectPresent(await storage.claimNextRun(['test']))
      expect(claimed.id).toBe('run_1')
    })

    it('resets a cancelled run to pending', async () => {
      await storage.createRun(makeRun({ id: 'run_1', status: 'cancelled' }))
      expect(await storage.requeueRun('run_1')).toBe(true)
      expect(expectPresent(await storage.getRun('run_1')).status).toBe('pending')
    })

    it('returns false for a nonexistent run', async () => {
      expect(await storage.requeueRun('nope')).toBe(false)
    })

    it('returns false for runs that are not failed or cancelled', async () => {
      await storage.createRun(makeRun({ id: 'p', status: 'pending' }))
      await storage.createRun(makeRun({ id: 'c', status: 'completed' }))

      expect(await storage.requeueRun('p')).toBe(false)
      expect(await storage.requeueRun('c')).toBe(false)
    })
  })
})
