import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { SQLiteStorage } from '../sqlite-node'
import { unlinkSync, existsSync } from 'node:fs'
import type { WorkflowRun, StepResult } from '../../core/types'

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
      expect(claimed.input).toEqual({ x: 1 })
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
        expect(claims[0].id).toBe('run_1')
      } finally {
        storage2.close()
      }
    })

    it('reclaims stale running runs when a stale threshold is provided', async () => {
      await storage.createRun(makeRun({ id: 'stale', status: 'running', updatedAt: 1 }))

      const claimed = expectPresent(await storage.claimNextRun(['test'], 10))

      expect(claimed.id).toBe('stale')
      expect(claimed.status).toBe('running')
      expect(claimed.leaseId).toBeTruthy()
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
      expect(claimed.input).toEqual(complexInput)
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
      expect(results[0].output).toEqual({ chargeId: 'ch_123' })
      expect(results[0].error).toBeNull()
    })

    it('returns an empty array for a run with no steps', async () => {
      const results = await storage.getStepResults('nonexistent')
      expect(results).toEqual([])
    })

    it('preserves step order by creation time', async () => {
      await storage.saveStepResult(makeStep({ id: 's1', name: 'first', createdAt: 1000 }))
      await storage.saveStepResult(makeStep({ id: 's2', name: 'second', createdAt: 2000 }))
      await storage.saveStepResult(makeStep({ id: 's3', name: 'third', createdAt: 3000 }))

      const results = await storage.getStepResults('run_1')
      expect(results.map((s) => s.name)).toEqual(['first', 'second', 'third'])
    })

    it('roundtrips null output (failed step)', async () => {
      await storage.saveStepResult(makeStep({
        id: 'step_1',
        status: 'failed',
        output: null,
        error: 'something went wrong',
      }))

      const results = await storage.getStepResults('run_1')
      expect(results[0].output).toBeNull()
      expect(results[0].error).toBe('something went wrong')
    })

    it('roundtrips null error (successful step)', async () => {
      await storage.saveStepResult(makeStep({
        id: 'step_1',
        status: 'completed',
        output: { ok: true },
        error: null,
      }))

      const results = await storage.getStepResults('run_1')
      expect(results[0].error).toBeNull()
      expect(results[0].output).toEqual({ ok: true })
    })

    it('roundtrips falsy outputs without collapsing them to null', async () => {
      await storage.saveStepResult(makeStep({ id: 'step_false', output: false }))
      await storage.saveStepResult(makeStep({ id: 'step_zero', name: 'zero', output: 0 }))
      await storage.saveStepResult(makeStep({ id: 'step_empty', name: 'empty', output: '' }))

      const results = await storage.getStepResults('run_1')

      expect(results[0].output).toBe(false)
      expect(results[1].output).toBe(0)
      expect(results[2].output).toBe('')
    })

    it('roundtrips undefined outputs through the persistence codec', async () => {
      await storage.saveStepResult(makeStep({ id: 'step_undefined', output: undefined }))

      const results = await storage.getStepResults('run_1')
      expect(results[0].output).toBeUndefined()
    })

    it('upserts — INSERT OR REPLACE updates existing step', async () => {
      await storage.saveStepResult(makeStep({ id: 'step_1', status: 'failed', output: null, error: 'fail' }))
      await storage.saveStepResult(makeStep({ id: 'step_1', status: 'completed', output: { ok: true }, error: null }))

      const results = await storage.getStepResults('run_1')
      expect(results).toHaveLength(1)
      expect(results[0].status).toBe('completed')
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
      expect(run.input).toEqual({ x: 1 })
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
      expect(run.input).toEqual(input)
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
      expect(claimed.input).toEqual({ hello: 'world' })

      const steps = await storage2.getStepResults('run_1')
      expect(steps).toHaveLength(1)

      storage2.close()

      // Re-assign for afterEach cleanup
      storage = new SQLiteStorage(DB_PATH)
      await storage.initialize()
    })
  })
})
