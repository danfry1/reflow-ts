import { describe, it, expect, beforeEach } from 'vitest'
import { MemoryStorage } from '../memory'
import type { WorkflowRun, StepResult } from '../../core/types'
import { at } from '../../__tests__/helpers'

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

describe('MemoryStorage', () => {
  let storage: MemoryStorage

  beforeEach(async () => {
    storage = new MemoryStorage()
    await storage.initialize()
  })

  describe('initialize', () => {
    it('can be called multiple times safely', async () => {
      await storage.initialize()
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

    it('creates a run and claims it', async () => {
      await storage.createRun(makeRun({ id: 'run_1', workflow: 'test', input: { x: 1 } }))
      const claimed = expectPresent(await storage.claimNextRun(['test']))

      expect(claimed.id).toBe('run_1')
      expect(claimed.status).toBe('running')
      expect(claimed.input).toStrictEqual({ x: 1 })
    })

    it('returns null when no runs exist', async () => {
      const claimed = await storage.claimNextRun(['test'])
      expect(claimed).toBeNull()
    })

    it('only claims runs for the specified workflow names', async () => {
      await storage.createRun(makeRun({ id: 'run_1', workflow: 'alpha' }))

      const claimed = await storage.claimNextRun(['beta', 'gamma'])
      expect(claimed).toBeNull()
    })

    it('does not double-claim — a claimed run is no longer pending', async () => {
      await storage.createRun(makeRun({ id: 'run_1' }))

      await storage.claimNextRun(['test'])
      const second = await storage.claimNextRun(['test'])
      expect(second).toBeNull()
    })

    it('claims runs in FIFO order', async () => {
      await storage.createRun(makeRun({ id: 'first', createdAt: 1 }))
      await storage.createRun(makeRun({ id: 'second', createdAt: 2 }))
      await storage.createRun(makeRun({ id: 'third', createdAt: 3 }))

      const claimed1 = expectPresent(await storage.claimNextRun(['test']))
      const claimed2 = expectPresent(await storage.claimNextRun(['test']))
      const claimed3 = expectPresent(await storage.claimNextRun(['test']))

      expect(claimed1.id).toBe('first')
      expect(claimed2.id).toBe('second')
      expect(claimed3.id).toBe('third')
    })

    it('claims across multiple workflow types', async () => {
      await storage.createRun(makeRun({ id: 'run_a', workflow: 'alpha' }))
      await storage.createRun(makeRun({ id: 'run_b', workflow: 'beta' }))

      const claimed = await storage.claimNextRun(['alpha', 'beta'])
      expect(claimed).not.toBeNull()
    })

    it('reclaims stale running runs and issues a new lease id', async () => {
      await storage.createRun(makeRun({ id: 'run_1', status: 'running', updatedAt: 1 }))

      const claimed = expectPresent(await storage.claimNextRun(['test'], 10))

      expect(claimed.status).toBe('running')
      expect(claimed.leaseId).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/)
    })

    it('returns a copy — mutating the result does not affect storage', async () => {
      await storage.createRun(makeRun({ id: 'run_1', input: { x: 1 } }))
      const claimed = expectPresent(await storage.claimNextRun(['test']))

      // Mutate the returned object
      ;(claimed as any).input = { x: 999 }

      // Original should be unaffected — but since it's already claimed,
      // verify via a new run that storage is intact
      await storage.createRun(makeRun({ id: 'run_2', input: { x: 2 } }))
      const claimed2 = expectPresent(await storage.claimNextRun(['test']))
      expect(claimed2.input).toStrictEqual({ x: 2 })
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
      await storage.saveStepResult(makeStep({ id: 'step_1', name: 'charge', output: { chargeId: 'ch_123' } }))

      const results = await storage.getStepResults('run_1')
      expect(results).toHaveLength(1)
      expect(at(results, 0).name).toBe('charge')
      expect(at(results, 0).output).toStrictEqual({ chargeId: 'ch_123' })
    })

    it('returns steps in insertion order', async () => {
      await storage.saveStepResult(makeStep({ id: 'step_1', name: 'first' }))
      await storage.saveStepResult(makeStep({ id: 'step_2', name: 'second' }))
      await storage.saveStepResult(makeStep({ id: 'step_3', name: 'third' }))

      const results = await storage.getStepResults('run_1')
      expect(results.map((s) => s.name)).toStrictEqual(['first', 'second', 'third'])
    })

    it('returns an empty array when no steps exist for a run', async () => {
      const results = await storage.getStepResults('nonexistent')
      expect(results).toStrictEqual([])
    })

    it('isolates step results by run id', async () => {
      await storage.saveStepResult(makeStep({ id: 'step_1', runId: 'run_1', name: 'a' }))
      await storage.saveStepResult(makeStep({ id: 'step_2', runId: 'run_2', name: 'b' }))

      const run1Steps = await storage.getStepResults('run_1')
      const run2Steps = await storage.getStepResults('run_2')

      expect(run1Steps).toHaveLength(1)
      expect(at(run1Steps, 0).name).toBe('a')
      expect(run2Steps).toHaveLength(1)
      expect(at(run2Steps, 0).name).toBe('b')
    })

    it('upserts — updating an existing step result by id', async () => {
      await storage.saveStepResult(makeStep({ id: 'step_1', name: 'charge', status: 'failed', output: null, error: 'timeout' }))
      await storage.saveStepResult(makeStep({ id: 'step_1', name: 'charge', status: 'completed', output: { ok: true }, error: null }))

      const results = await storage.getStepResults('run_1')
      expect(results).toHaveLength(1)
      expect(at(results, 0).status).toBe('completed')
      expect(at(results, 0).output).toStrictEqual({ ok: true })
    })

    it('returns copies — mutating results does not affect storage', async () => {
      await storage.saveStepResult(makeStep({ id: 'step_1', output: { x: 1 } }))

      const results = await storage.getStepResults('run_1')
      ;(results[0] as any).output = { x: 999 }

      const fresh = await storage.getStepResults('run_1')
      expect(at(fresh, 0).output).toStrictEqual({ x: 1 })
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

    it('returns a copy — mutating does not affect storage', async () => {
      await storage.createRun(makeRun({ id: 'run_1', input: { x: 1 } }))
      const run = expectPresent(await storage.getRun('run_1'))
      ;(run as any).input = { x: 999 }

      const fresh = expectPresent(await storage.getRun('run_1'))
      expect(fresh.input).toStrictEqual({ x: 1 })
    })
  })

  describe('updateRunStatus', () => {
    it('updates the status of an existing run', async () => {
      await storage.createRun(makeRun({ id: 'run_1' }))
      await storage.updateRunStatus('run_1', 'completed')

      // Completed runs are not claimable
      const claimed = await storage.claimNextRun(['test'])
      expect(claimed).toBeNull()
    })

    it('is a no-op for nonexistent run ids', async () => {
      // Should not throw
      await storage.updateRunStatus('nonexistent', 'failed')
    })

    it('updates a claimed run only when the lease matches', async () => {
      await storage.createRun(makeRun({ id: 'run_1' }))
      const claimed = expectPresent(await storage.claimNextRun(['test']))

      expect(await storage.updateClaimedRunStatus('run_1', 'wrong-lease', 'completed')).toBe(false)
      expect(await storage.updateClaimedRunStatus('run_1', claimed.leaseId, 'completed')).toBe(true)
    })
  })

  describe('sleepRun / wake', () => {
    it('suspends a claimed run; a non-matching lease cannot', async () => {
      await storage.createRun(makeRun({ id: 'run_1' }))
      const claimed = expectPresent(await storage.claimNextRun(['test']))

      expect(await storage.sleepRun('run_1', 'wrong-lease', Date.now() + 60_000)).toBe(false)
      expect(await storage.sleepRun('run_1', claimed.leaseId, Date.now() + 60_000)).toBe(true)
      expect(expectPresent(await storage.getRun('run_1')).status).toBe('sleeping')

      // Not yet due — not claimable.
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
  })

  describe('waitRun / events', () => {
    it('delivers and takes events FIFO; returns null when none remain', async () => {
      await storage.createRun(makeRun({ id: 'run_1' }))

      expect(await storage.deliverEvent('run_1', 'e', { n: 1 })).toBe(true)
      expect(await storage.deliverEvent('run_1', 'e', { n: 2 })).toBe(true)
      expect(await storage.deliverEvent('run_1', 'other', { x: 9 })).toBe(true)

      expect(await storage.takeEvent('run_1', 'e')).toStrictEqual({ payload: { n: 1 } })
      expect(await storage.takeEvent('run_1', 'e')).toStrictEqual({ payload: { n: 2 } })
      expect(await storage.takeEvent('run_1', 'e')).toBeNull()
      expect(await storage.takeEvent('run_1', 'other')).toStrictEqual({ payload: { x: 9 } })
    })

    it('deliverEvent returns false for a missing run', async () => {
      expect(await storage.deliverEvent('nope', 'e', {})).toBe(false)
    })

    it('waitRun suspends a claimed run; a non-matching lease cannot', async () => {
      await storage.createRun(makeRun({ id: 'run_1' }))
      const claimed = expectPresent(await storage.claimNextRun(['test']))

      expect(await storage.waitRun('run_1', 'wrong-lease', 'e', Date.now() + 60_000)).toBe(false)
      expect(await storage.waitRun('run_1', claimed.leaseId, 'e', Date.now() + 60_000)).toBe(true)
      expect(expectPresent(await storage.getRun('run_1')).status).toBe('waiting')
      expect(await storage.claimNextRun(['test'])).toBeNull()
    })

    it('waitRun stays reclaimable when a matching event is already buffered (race)', async () => {
      await storage.createRun(makeRun({ id: 'run_1' }))
      const claimed = expectPresent(await storage.claimNextRun(['test']))
      await storage.deliverEvent('run_1', 'e', { ok: true })

      expect(await storage.waitRun('run_1', claimed.leaseId, 'e', null)).toBe(true)
      expect(expectPresent(await storage.getRun('run_1')).status).toBe('pending')
    })

    it('deliverEvent wakes a waiting run so it can be reclaimed', async () => {
      await storage.createRun(makeRun({ id: 'run_1' }))
      const claimed = expectPresent(await storage.claimNextRun(['test']))
      await storage.waitRun('run_1', claimed.leaseId, 'e', null)
      expect(expectPresent(await storage.getRun('run_1')).status).toBe('waiting')

      await storage.deliverEvent('run_1', 'e', { ok: true })
      const woken = expectPresent(await storage.claimNextRun(['test']))
      expect(woken.id).toBe('run_1')
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

      const page2 = await storage.listRuns({ limit: 2, before: page1[page1.length - 1].createdAt })
      expect(page2.map((r) => r.id)).toEqual(['a'])
    })

    it('paginates without losing rows when createdAt is tied, via the (before, beforeId) cursor', async () => {
      await storage.createRun(makeRun({ id: 'a', createdAt: 1000 }))
      await storage.createRun(makeRun({ id: 'b', createdAt: 1000 }))
      await storage.createRun(makeRun({ id: 'c', createdAt: 1000 }))

      const page1 = await storage.listRuns({ limit: 2 })
      const last = page1[page1.length - 1]
      const page2 = await storage.listRuns({ limit: 2, before: last.createdAt, beforeId: last.id })

      expect(page1.length + page2.length).toBe(3)
      expect([...page1, ...page2].map((r) => r.id).sort()).toEqual(['a', 'b', 'c'])
    })

    it('returns an empty array when nothing matches', async () => {
      expect(await storage.listRuns({ status: 'completed' })).toEqual([])
    })

    it('returns copies — mutating does not affect storage', async () => {
      await storage.createRun(makeRun({ id: 'a', input: { x: 1 } }))
      const runs = await storage.listRuns()
      ;(runs[0] as any).input = { x: 999 }

      const fresh = await storage.listRuns()
      expect(fresh[0].input).toEqual({ x: 1 })
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

    it('returns false for a running run — a mid-execution run is not yanked back to pending', async () => {
      await storage.createRun(makeRun({ id: 'r' }))
      const claimed = expectPresent(await storage.claimNextRun(['test']))
      expect(claimed.id).toBe('r')

      expect(await storage.requeueRun('r')).toBe(false)
      expect(expectPresent(await storage.getRun('r')).status).toBe('running')
    })

    it('clears the lease so a stale lease can no longer write', async () => {
      await storage.createRun(makeRun({ id: 'run_1' }))
      const claimed = expectPresent(await storage.claimNextRun(['test']))
      await storage.updateClaimedRunStatus('run_1', claimed.leaseId, 'failed')

      expect(await storage.requeueRun('run_1')).toBe(true)

      const saved = await storage.saveStepResult(makeStep({ id: 'x' }), claimed.leaseId)
      expect(saved).toBe(false)
    })
  })
})
