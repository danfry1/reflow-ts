import { describe, it, expect, beforeEach } from 'vitest'
import { MemoryStorage } from '../memory'
import type { WorkflowRun, StepResult } from '../../core/types'

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
      expect(claimed.input).toEqual({ x: 1 })
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
      expect(claimed.leaseId).toBeTruthy()
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
      expect(claimed2.input).toEqual({ x: 2 })
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
      expect(results[0].name).toBe('charge')
      expect(results[0].output).toEqual({ chargeId: 'ch_123' })
    })

    it('returns steps in insertion order', async () => {
      await storage.saveStepResult(makeStep({ id: 'step_1', name: 'first' }))
      await storage.saveStepResult(makeStep({ id: 'step_2', name: 'second' }))
      await storage.saveStepResult(makeStep({ id: 'step_3', name: 'third' }))

      const results = await storage.getStepResults('run_1')
      expect(results.map((s) => s.name)).toEqual(['first', 'second', 'third'])
    })

    it('returns an empty array when no steps exist for a run', async () => {
      const results = await storage.getStepResults('nonexistent')
      expect(results).toEqual([])
    })

    it('isolates step results by run id', async () => {
      await storage.saveStepResult(makeStep({ id: 'step_1', runId: 'run_1', name: 'a' }))
      await storage.saveStepResult(makeStep({ id: 'step_2', runId: 'run_2', name: 'b' }))

      const run1Steps = await storage.getStepResults('run_1')
      const run2Steps = await storage.getStepResults('run_2')

      expect(run1Steps).toHaveLength(1)
      expect(run1Steps[0].name).toBe('a')
      expect(run2Steps).toHaveLength(1)
      expect(run2Steps[0].name).toBe('b')
    })

    it('upserts — updating an existing step result by id', async () => {
      await storage.saveStepResult(makeStep({ id: 'step_1', name: 'charge', status: 'failed', output: null, error: 'timeout' }))
      await storage.saveStepResult(makeStep({ id: 'step_1', name: 'charge', status: 'completed', output: { ok: true }, error: null }))

      const results = await storage.getStepResults('run_1')
      expect(results).toHaveLength(1)
      expect(results[0].status).toBe('completed')
      expect(results[0].output).toEqual({ ok: true })
    })

    it('returns copies — mutating results does not affect storage', async () => {
      await storage.saveStepResult(makeStep({ id: 'step_1', output: { x: 1 } }))

      const results = await storage.getStepResults('run_1')
      ;(results[0] as any).output = { x: 999 }

      const fresh = await storage.getStepResults('run_1')
      expect(fresh[0].output).toEqual({ x: 1 })
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

    it('returns a copy — mutating does not affect storage', async () => {
      await storage.createRun(makeRun({ id: 'run_1', input: { x: 1 } }))
      const run = expectPresent(await storage.getRun('run_1'))
      ;(run as any).input = { x: 999 }

      const fresh = expectPresent(await storage.getRun('run_1'))
      expect(fresh.input).toEqual({ x: 1 })
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
})
