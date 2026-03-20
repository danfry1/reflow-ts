import { randomUUID } from 'node:crypto'
import { existsSync, unlinkSync } from 'node:fs'
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { z } from 'zod'
import { createWorkflow } from '../workflow'
import { createEngine } from '../engine'
import type { StorageAdapter } from '../types'
import { MemoryStorage } from '../../storage/memory'
import { SQLiteStorage } from '../../storage/sqlite-node'

function expectPresent<T>(value: T | null | undefined): T {
  expect(value).not.toBeNull()
  expect(value).not.toBeUndefined()

  if (value == null) {
    throw new Error('Expected value to be present')
  }

  return value
}

function makeTempDbPath(prefix: string): string {
  return `/tmp/${prefix}-${randomUUID()}.db`
}

describe('Engine', () => {
  let storage: MemoryStorage

  beforeEach(async () => {
    storage = new MemoryStorage()
    await storage.initialize()
  })

  describe('config validation', () => {
    const wf = createWorkflow({
      name: 'test',
      input: z.object({}),
    }).step('a', async () => ({}))

    it('rejects non-positive concurrency', () => {
      expect(() => createEngine({ storage, workflows: [wf], concurrency: 0 })).toThrow(/positive integer/)
      expect(() => createEngine({ storage, workflows: [wf], concurrency: -1 })).toThrow(/positive integer/)
      expect(() => createEngine({ storage, workflows: [wf], concurrency: 1.5 })).toThrow(/positive integer/)
    })

    it('rejects non-positive runLeaseDurationMs', () => {
      expect(() => createEngine({ storage, workflows: [wf], runLeaseDurationMs: 0 })).toThrow(/positive number/)
      expect(() => createEngine({ storage, workflows: [wf], runLeaseDurationMs: -1 })).toThrow(/positive number/)
    })

    it('rejects heartbeatIntervalMs >= runLeaseDurationMs', () => {
      expect(() =>
        createEngine({ storage, workflows: [wf], runLeaseDurationMs: 100, heartbeatIntervalMs: 100 }),
      ).toThrow(/smaller than/)
      expect(() =>
        createEngine({ storage, workflows: [wf], runLeaseDurationMs: 100, heartbeatIntervalMs: 200 }),
      ).toThrow(/smaller than/)
    })

    it('rejects non-positive heartbeatIntervalMs', () => {
      expect(() =>
        createEngine({ storage, workflows: [wf], runLeaseDurationMs: 100, heartbeatIntervalMs: 0 }),
      ).toThrow(/positive number/)
    })

    it('rejects non-positive pollIntervalMs on start()', async () => {
      const engine = createEngine({ storage, workflows: [wf] })
      await expect(engine.start(0)).rejects.toThrow(/positive number/)
      await expect(engine.start(-1)).rejects.toThrow(/positive number/)
    })
  })

  describe('enqueue', () => {
    it('rejects duplicate workflow names at engine creation', () => {
      const wf1 = createWorkflow({
        name: 'duplicate',
        input: z.object({ x: z.number() }),
      }).step('a', async ({ input }) => ({ x: input.x }))

      const wf2 = createWorkflow({
        name: 'duplicate',
        input: z.object({ y: z.number() }),
      }).step('b', async ({ input }) => ({ y: input.y }))

      expect(() => createEngine({ storage, workflows: [wf1, wf2] })).toThrow(
        'Workflow "duplicate" is registered more than once',
      )
    })

    it('creates a pending run with a unique id', async () => {
      const wf = createWorkflow({
        name: 'test',
        input: z.object({ x: z.number() }),
      }).step('a', async ({ input }) => input)

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('test', { x: 1 })

      expect(run.id).toBeTruthy()
      expect(run.workflow).toBe('test')
      expect(run.status).toBe('pending')
      expect(run.input).toEqual({ x: 1 })
    })

    it('returns the existing run when the same idempotency key is reused', async () => {
      const wf = createWorkflow({
        name: 'dedupe',
        input: z.object({ x: z.number() }),
      }).step('a', async ({ input }) => input)

      const engine = createEngine({ storage, workflows: [wf] })
      const run1 = await engine.enqueue('dedupe', { x: 1 }, { idempotencyKey: 'same-key' })
      const run2 = await engine.enqueue('dedupe', { x: 1 }, { idempotencyKey: 'same-key' })

      expect(run1.id).toBe(run2.id)
      expect(run2.idempotencyKey).toBe('same-key')
    })

    it('rejects reusing an idempotency key with different input', async () => {
      const wf = createWorkflow({
        name: 'dedupe',
        input: z.object({ x: z.number() }),
      }).step('a', async ({ input }) => input)

      const engine = createEngine({ storage, workflows: [wf] })
      await engine.enqueue('dedupe', { x: 1 }, { idempotencyKey: 'same-key' })

      await expect(
        engine.enqueue('dedupe', { x: 2 }, { idempotencyKey: 'same-key' }),
      ).rejects.toThrow(/different input/i)
    })

    it('rejects an empty idempotency key', async () => {
      const wf = createWorkflow({
        name: 'empty-key',
        input: z.object({}),
      }).step('a', async () => ({}))

      const engine = createEngine({ storage, workflows: [wf] })

      await expect(
        engine.enqueue('empty-key', {}, { idempotencyKey: '' }),
      ).rejects.toThrow(/must not be empty/i)
    })

    it('deduplicates the same idempotency key across SQLite-backed engine instances', async () => {
      const dbPath = makeTempDbPath('reflow-enqueue-race')
      const storage1 = new SQLiteStorage(dbPath)
      const storage2 = new SQLiteStorage(dbPath)
      await storage1.initialize()
      await storage2.initialize()

      try {
        const wf = createWorkflow({
          name: 'dedupe-race',
          input: z.object({ x: z.number() }),
        }).step('a', async ({ input }) => input)

        const engine1 = createEngine({ storage: storage1, workflows: [wf] })
        const engine2 = createEngine({ storage: storage2, workflows: [wf] })

        const [run1, run2] = await Promise.all([
          engine1.enqueue('dedupe-race', { x: 1 }, { idempotencyKey: 'shared-key' }),
          engine2.enqueue('dedupe-race', { x: 1 }, { idempotencyKey: 'shared-key' }),
        ])

        expect(run1.id).toBe(run2.id)
        expect(run1.idempotencyKey).toBe('shared-key')
        expect(expectPresent(await storage1.claimNextRun(['dedupe-race'])).id).toBe(run1.id)
        expect(await storage2.claimNextRun(['dedupe-race'])).toBeNull()
      } finally {
        storage1.close()
        storage2.close()

        if (existsSync(dbPath)) {
          unlinkSync(dbPath)
        }
      }
    })

    it('generates unique ids for each run', async () => {
      const wf = createWorkflow({
        name: 'test',
        input: z.object({}),
      }).step('a', async () => ({}))

      const engine = createEngine({ storage, workflows: [wf] })
      const run1 = await engine.enqueue('test', {})
      const run2 = await engine.enqueue('test', {})

      expect(run1.id).not.toBe(run2.id)
    })

    it('validates input against the workflow schema', async () => {
      const wf = createWorkflow({
        name: 'strict',
        input: z.object({ email: z.email() }),
      }).step('a', async ({ input }) => input)

      const engine = createEngine({ storage, workflows: [wf] })

      await expect(
        engine.enqueue('strict', { email: 'not-an-email' } as any),
      ).rejects.toThrow()
    })

    it('throws for an unknown workflow name', async () => {
      const engine = createEngine({ storage, workflows: [] })

      await expect(
        (engine as any).enqueue('nonexistent', {}),
      ).rejects.toThrow(/workflow "nonexistent" not found/i)
    })
  })

  describe('execution', () => {
    it('executes a single-step workflow', async () => {
      const handler = vi.fn(async ({ input }) => ({
        doubled: input.x * 2,
      }))

      const wf = createWorkflow({
        name: 'double',
        input: z.object({ x: z.number() }),
      }).step('calc', handler)

      const engine = createEngine({ storage, workflows: [wf] })
      await engine.enqueue('double', { x: 5 })
      await engine.tick()

      expect(handler).toHaveBeenCalledOnce()
      expect(handler).toHaveBeenCalledWith(
        expect.objectContaining({ input: { x: 5 } }),
      )
    })

    it('passes undefined as prev to the first step', async () => {
      const handler = vi.fn(async ({ prev }) => {
        expect(prev).toBeUndefined()
        return {}
      })

      const wf = createWorkflow({
        name: 'test',
        input: z.object({}),
      }).step('first', handler)

      const engine = createEngine({ storage, workflows: [wf] })
      await engine.enqueue('test', {})
      await engine.tick()

      expect(handler).toHaveBeenCalledOnce()
    })

    it('chains step outputs — each step receives the previous output as prev', async () => {
      const results: unknown[] = []

      const wf = createWorkflow({
        name: 'chain',
        input: z.object({ x: z.number() }),
      })
        .step('first', async ({ input }) => {
          const out = { a: input.x + 1 }
          results.push(out)
          return out
        })
        .step('second', async ({ prev }) => {
          const out = { b: prev.a * 2 }
          results.push(out)
          return out
        })

      const engine = createEngine({ storage, workflows: [wf] })
      await engine.enqueue('chain', { x: 10 })
      await engine.tick()

      expect(results).toEqual([{ a: 11 }, { b: 22 }])
    })

    it('marks a successful run as completed', async () => {
      const wf = createWorkflow({
        name: 'test',
        input: z.object({}),
      }).step('a', async () => ({ done: true }))

      const engine = createEngine({ storage, workflows: [wf] })
      await engine.enqueue('test', {})
      await engine.tick()

      // Completed runs should not be claimable
      const claimed = await storage.claimNextRun(['test'])
      expect(claimed).toBeNull()
    })

    it('persists step results to storage after each step', async () => {
      const wf = createWorkflow({
        name: 'persist',
        input: z.object({}),
      })
        .step('a', async () => ({ x: 1 }))
        .step('b', async () => ({ y: 2 }))

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('persist', {})
      await engine.tick()

      const steps = await storage.getStepResults(run.id)
      expect(steps).toHaveLength(2)
      expect(steps[0].name).toBe('a')
      expect(steps[0].output).toEqual({ x: 1 })
      expect(steps[0].status).toBe('completed')
      expect(steps[1].name).toBe('b')
      expect(steps[1].output).toEqual({ y: 2 })
    })

    it('completes a workflow with zero steps', async () => {
      const wf = createWorkflow({
        name: 'empty',
        input: z.object({}),
      })

      const engine = createEngine({ storage, workflows: [wf] })
      await engine.enqueue('empty', {})
      await engine.tick()

      // Should be completed (not stuck as pending)
      const claimed = await storage.claimNextRun(['empty'])
      expect(claimed).toBeNull()
    })

    it('handles a step that returns undefined', async () => {
      const wf = createWorkflow({
        name: 'void-step',
        input: z.object({}),
      })
        .step('no-return', async () => {
          // intentionally returns undefined
        })
        .step('after', async ({ prev }) => ({ received: prev }))

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('void-step', {})
      await engine.tick()

      const steps = await storage.getStepResults(run.id)
      expect(steps).toHaveLength(2)
      expect(steps[1].output).toEqual({ received: undefined })
    })

    it('handles a step that returns null', async () => {
      const wf = createWorkflow({
        name: 'null-step',
        input: z.object({}),
      }).step('nil', async () => null)

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('null-step', {})
      await engine.tick()

      const steps = await storage.getStepResults(run.id)
      expect(steps[0].output).toBeNull()
    })

    it('does nothing when tick finds no pending runs', async () => {
      const wf = createWorkflow({
        name: 'test',
        input: z.object({}),
      }).step('a', async () => ({}))

      const engine = createEngine({ storage, workflows: [wf] })
      // No runs enqueued — tick should be a no-op
      await engine.tick()
    })

    it('processes only one run per tick', async () => {
      let execCount = 0
      const wf = createWorkflow({
        name: 'counter',
        input: z.object({}),
      }).step('count', async () => {
        execCount++
        return {}
      })

      const engine = createEngine({ storage, workflows: [wf] })
      await engine.enqueue('counter', {})
      await engine.enqueue('counter', {})
      await engine.tick()

      expect(execCount).toBe(1)
    })

    it('processes multiple runs across multiple ticks', async () => {
      let execCount = 0
      const wf = createWorkflow({
        name: 'counter',
        input: z.object({}),
      }).step('count', async () => {
        execCount++
        return {}
      })

      const engine = createEngine({ storage, workflows: [wf] })
      await engine.enqueue('counter', {})
      await engine.enqueue('counter', {})
      await engine.tick()
      await engine.tick()

      expect(execCount).toBe(2)
    })
  })

  describe('resume', () => {
    it('skips completed steps and resumes from the first incomplete step', async () => {
      const firstHandler = vi.fn(async () => ({ a: 1 }))
      const secondHandler = vi.fn(async () => ({ b: 2 }))

      const wf = createWorkflow({
        name: 'resume',
        input: z.object({}),
      })
        .step('first', firstHandler)
        .step('second', secondHandler)

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('resume', {})

      // Simulate: first step completed before crash
      await storage.saveStepResult({
        id: 'step_0',
        runId: run.id,
        name: 'first',
        status: 'completed',
        output: { a: 1 },
        error: null,
        attempts: 1,
        createdAt: Date.now(),
        updatedAt: Date.now(),
      })

      await engine.tick()

      expect(firstHandler).not.toHaveBeenCalled()
      expect(secondHandler).toHaveBeenCalledWith(
        expect.objectContaining({ prev: { a: 1 } }),
      )
    })

    it('re-executes a previously failed step on resume', async () => {
      let callCount = 0
      const wf = createWorkflow({
        name: 'retry-resume',
        input: z.object({}),
      })
        .step('flaky', async () => {
          callCount++
          return { ok: true }
        })

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('retry-resume', {})

      // Simulate: step was saved as failed in a previous run
      await storage.saveStepResult({
        id: 'step_0',
        runId: run.id,
        name: 'flaky',
        status: 'failed',
        output: null,
        error: 'previous failure',
        attempts: 1,
        createdAt: Date.now(),
        updatedAt: Date.now(),
      })

      await engine.tick()

      // Failed steps should be re-executed
      expect(callCount).toBe(1)
    })

    it('reclaims stale running runs after the lease expires', async () => {
      const handler = vi.fn(async () => ({ ok: true }))
      const wf = createWorkflow({
        name: 'lease-recovery',
        input: z.object({}),
      }).step('resume', handler)

      const engine = createEngine({ storage, workflows: [wf], runLeaseDurationMs: 5 })
      const run = await engine.enqueue('lease-recovery', {})
      await storage.updateRunStatus(run.id, 'running')

      await new Promise((resolve) => setTimeout(resolve, 10))
      await engine.tick()

      expect(handler).toHaveBeenCalledOnce()
      expect(expectPresent(await engine.getRunStatus(run.id)).run.status).toBe('completed')
    })

    it('keeps a long-running claimed run alive with heartbeats', async () => {
      const slowHandler = vi.fn(async ({ signal }) => {
        await new Promise((resolve, reject) => {
          const timer = setTimeout(resolve, 70)
          signal.addEventListener('abort', () => {
            clearTimeout(timer)
            reject(signal.reason)
          }, { once: true })
        })
        return { ok: true }
      })
      const secondHandler = vi.fn(async () => ({ ok: true }))

      const wf = createWorkflow({
        name: 'heartbeat',
        input: z.object({}),
      }).step('slow', slowHandler)

      const engine1 = createEngine({
        storage,
        workflows: [wf],
        runLeaseDurationMs: 30,
        heartbeatIntervalMs: 5,
      })
      const engine2 = createEngine({
        storage,
        workflows: [createWorkflow({ name: 'heartbeat', input: z.object({}) }).step('slow', secondHandler)],
        runLeaseDurationMs: 30,
        heartbeatIntervalMs: 5,
      })

      await engine1.enqueue('heartbeat', {})
      const runPromise = engine1.tick()

      await new Promise((resolve) => setTimeout(resolve, 45))
      await engine2.tick()
      await runPromise

      expect(slowHandler).toHaveBeenCalledOnce()
      expect(secondHandler).not.toHaveBeenCalled()
    })

    it('fails the run if heartbeats start erroring mid-step', async () => {
      const delegate = new MemoryStorage()
      await delegate.initialize()

      let heartbeatCalls = 0
      const flakyStorage: StorageAdapter = {
        initialize: () => delegate.initialize(),
        createRun: (run) => delegate.createRun(run),
        claimNextRun: (workflowNames, staleBefore) => delegate.claimNextRun(workflowNames, staleBefore),
        heartbeatRun: async (runId, leaseId) => {
          heartbeatCalls++
          if (heartbeatCalls === 1) {
            throw new Error('heartbeat exploded')
          }

          return delegate.heartbeatRun(runId, leaseId)
        },
        getRun: (runId) => delegate.getRun(runId),
        getStepResults: (runId) => delegate.getStepResults(runId),
        saveStepResult: (result, leaseId) => delegate.saveStepResult(result, leaseId),
        updateRunStatus: (runId, status) => delegate.updateRunStatus(runId, status),
        updateClaimedRunStatus: (runId, leaseId, status) =>
          delegate.updateClaimedRunStatus(runId, leaseId, status),
        close: () => delegate.close(),
      }

      const afterStep = vi.fn(async () => ({ done: true }))
      const wf = createWorkflow({
        name: 'heartbeat-error',
        input: z.object({}),
      })
        .step('slow', async ({ signal }) => {
          await new Promise((_, reject) => {
            signal.addEventListener('abort', () => reject(signal.reason), { once: true })
          })
        })
        .step('after', afterStep)

      const engine = createEngine({
        storage: flakyStorage,
        workflows: [wf],
        runLeaseDurationMs: 30,
        heartbeatIntervalMs: 5,
      })

      const run = await engine.enqueue('heartbeat-error', {})
      await engine.tick()

      const info = expectPresent(await engine.getRunStatus(run.id))
      expect(info.run.status).toBe('failed')
      expect(info.steps).toHaveLength(1)
      expect(info.steps[0].name).toBe('slow')
      expect(info.steps[0].status).toBe('failed')
      expect(info.steps[0].error).toContain('heartbeat exploded')
      expect(afterStep).not.toHaveBeenCalled()
    })
  })

  describe('retry', () => {
    it('retries a failing step up to maxAttempts', async () => {
      let attempts = 0
      const wf = createWorkflow({
        name: 'retry-test',
        input: z.object({}),
      }).step('flaky', {
        retry: { maxAttempts: 3, backoff: 'linear', initialDelayMs: 0 },
        handler: async () => {
          attempts++
          if (attempts < 3) throw new Error('fail')
          return { ok: true }
        },
      })

      const engine = createEngine({ storage, workflows: [wf] })
      await engine.enqueue('retry-test', {})
      await engine.tick()

      expect(attempts).toBe(3)
    })

    it('records the correct attempt count on success', async () => {
      let attempts = 0
      const wf = createWorkflow({
        name: 'attempt-count',
        input: z.object({}),
      }).step('flaky', {
        retry: { maxAttempts: 3, backoff: 'linear', initialDelayMs: 0 },
        handler: async () => {
          attempts++
          if (attempts < 2) throw new Error('fail')
          return { ok: true }
        },
      })

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('attempt-count', {})
      await engine.tick()

      const steps = await storage.getStepResults(run.id)
      expect(steps[0].attempts).toBe(2)
      expect(steps[0].status).toBe('completed')
    })

    it('defaults to 1 attempt when no retry config is set', async () => {
      const wf = createWorkflow({
        name: 'no-retry',
        input: z.object({}),
      }).step('fragile', async () => {
        throw new Error('one and done')
      })

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('no-retry', {})
      await engine.tick()

      const steps = await storage.getStepResults(run.id)
      expect(steps[0].attempts).toBe(1)
      expect(steps[0].status).toBe('failed')
    })

    it('uses exponential backoff when configured', async () => {
      const delays: number[] = []
      const originalSetTimeout = globalThis.setTimeout
      vi.spyOn(globalThis, 'setTimeout').mockImplementation(((fn: () => void, ms?: number) => {
        delays.push(ms ?? 0)
        return originalSetTimeout(fn, 0)
      }) as typeof setTimeout)

      let attempts = 0
      const wf = createWorkflow({
        name: 'exp-backoff',
        input: z.object({}),
      }).step('slow', {
        retry: { maxAttempts: 4, backoff: 'exponential', initialDelayMs: 100 },
        handler: async () => {
          attempts++
          if (attempts < 4) throw new Error('fail')
          return { ok: true }
        },
      })

      const engine = createEngine({ storage, workflows: [wf] })
      await engine.enqueue('exp-backoff', {})
      await engine.tick()

      // Exponential: 100 * 2^0, 100 * 2^1, 100 * 2^2
      expect(delays).toEqual([100, 200, 400])

      vi.restoreAllMocks()
    })

    it('uses linear backoff when configured', async () => {
      const delays: number[] = []
      const originalSetTimeout = globalThis.setTimeout
      vi.spyOn(globalThis, 'setTimeout').mockImplementation(((fn: () => void, ms?: number) => {
        delays.push(ms ?? 0)
        return originalSetTimeout(fn, 0)
      }) as typeof setTimeout)

      let attempts = 0
      const wf = createWorkflow({
        name: 'lin-backoff',
        input: z.object({}),
      }).step('slow', {
        retry: { maxAttempts: 4, backoff: 'linear', initialDelayMs: 50 },
        handler: async () => {
          attempts++
          if (attempts < 4) throw new Error('fail')
          return { ok: true }
        },
      })

      const engine = createEngine({ storage, workflows: [wf] })
      await engine.enqueue('lin-backoff', {})
      await engine.tick()

      // Linear: 50 * 1, 50 * 2, 50 * 3
      expect(delays).toEqual([50, 100, 150])

      vi.restoreAllMocks()
    })

    it('skips delay when initialDelayMs is 0', async () => {
      const setTimeoutSpy = vi.spyOn(globalThis, 'setTimeout')

      let attempts = 0
      const wf = createWorkflow({
        name: 'no-delay',
        input: z.object({}),
      }).step('fast', {
        retry: { maxAttempts: 3, backoff: 'linear', initialDelayMs: 0 },
        handler: async () => {
          attempts++
          if (attempts < 3) throw new Error('fail')
          return { ok: true }
        },
      })

      const engine = createEngine({ storage, workflows: [wf] })
      await engine.enqueue('no-delay', {})
      await engine.tick()

      // setTimeout should not have been called for retries (delay is 0)
      expect(setTimeoutSpy).not.toHaveBeenCalled()

      vi.restoreAllMocks()
    })
  })

  describe('failure handling', () => {
    it('marks the run as failed when a step exhausts retries', async () => {
      const wf = createWorkflow({
        name: 'fail-test',
        input: z.object({}),
      }).step('broken', {
        retry: { maxAttempts: 2, backoff: 'linear', initialDelayMs: 0 },
        handler: async () => {
          throw new Error('always fails')
        },
      })

      const engine = createEngine({ storage, workflows: [wf] })
      await engine.enqueue('fail-test', {})
      await engine.tick()

      const claimed = await storage.claimNextRun(['fail-test'])
      expect(claimed).toBeNull()
    })

    it('saves the error message in the step result', async () => {
      const wf = createWorkflow({
        name: 'error-msg',
        input: z.object({}),
      }).step('broken', async () => {
        throw new Error('specific error message')
      })

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('error-msg', {})
      await engine.tick()

      const steps = await storage.getStepResults(run.id)
      expect(steps[0].error).toBe('specific error message')
      expect(steps[0].status).toBe('failed')
      expect(steps[0].output).toBeNull()
    })

    it('calls the onFailure handler with error context', async () => {
      const failHandler = vi.fn(async () => {})

      const wf = createWorkflow({
        name: 'fail-handler',
        input: z.object({ userId: z.string() }),
      })
        .step('broken', async () => {
          throw new Error('boom')
        })
        .onFailure(failHandler)

      const engine = createEngine({ storage, workflows: [wf] })
      await engine.enqueue('fail-handler', { userId: 'u_123' })
      await engine.tick()

      expect(failHandler).toHaveBeenCalledOnce()
      expect(failHandler).toHaveBeenCalledWith({
        error: expect.any(Error),
        stepName: 'broken',
        input: { userId: 'u_123' },
      })
    })

    it('handles non-Error throws (strings, numbers)', async () => {
      const failHandler = vi.fn(async () => {})

      const wf = createWorkflow({
        name: 'string-throw',
        input: z.object({}),
      })
        .step('throws-string', async () => {
          throw 'raw string error'
        })
        .onFailure(failHandler)

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('string-throw', {})
      await engine.tick()

      // Non-Error values should be wrapped in an Error
      expect(failHandler).toHaveBeenCalledWith(
        expect.objectContaining({
          error: expect.objectContaining({ message: 'raw string error' }),
        }),
      )

      const steps = await storage.getStepResults(run.id)
      expect(steps[0].error).toBe('raw string error')
    })

    it('still marks run as failed even without an onFailure handler', async () => {
      const wf = createWorkflow({
        name: 'no-handler',
        input: z.object({}),
      }).step('broken', async () => {
        throw new Error('no handler')
      })
      // no .onFailure()

      const engine = createEngine({ storage, workflows: [wf] })
      await engine.enqueue('no-handler', {})
      await engine.tick()

      const claimed = await storage.claimNextRun(['no-handler'])
      expect(claimed).toBeNull()
    })

    it('stops execution at the failing step (does not run later steps)', async () => {
      const secondHandler = vi.fn(async () => ({ b: 2 }))

      const wf = createWorkflow({
        name: 'stop-on-fail',
        input: z.object({}),
      })
        .step('fails', async () => {
          throw new Error('stop here')
        })
        .step('never-reached', secondHandler)

      const engine = createEngine({ storage, workflows: [wf] })
      await engine.enqueue('stop-on-fail', {})
      await engine.tick()

      expect(secondHandler).not.toHaveBeenCalled()
    })
  })

  describe('polling lifecycle', () => {
    it('start() initializes storage and begins polling', async () => {
      const wf = createWorkflow({
        name: 'poll-test',
        input: z.object({}),
      }).step('a', async () => ({ ok: true }))

      const engine = createEngine({ storage, workflows: [wf] })
      await engine.enqueue('poll-test', {})

      await engine.start(50)

      // Wait for at least one poll cycle
      await new Promise((resolve) => setTimeout(resolve, 100))
      engine.stop()

      // The run should have been processed
      const claimed = await storage.claimNextRun(['poll-test'])
      expect(claimed).toBeNull()
    })

    it('stop() halts polling', async () => {
      let execCount = 0
      const wf = createWorkflow({
        name: 'stop-test',
        input: z.object({}),
      }).step('count', async () => {
        execCount++
        return {}
      })

      const engine = createEngine({ storage, workflows: [wf] })
      await engine.start(50)
      engine.stop()

      // Enqueue after stop — should never be processed
      await engine.enqueue('stop-test', {})
      await new Promise((resolve) => setTimeout(resolve, 150))

      expect(execCount).toBe(0)
    })

    it('stop() aborts in-flight runs', async () => {
      let capturedSignal: AbortSignal | undefined

      const wf = createWorkflow({
        name: 'abort-on-stop',
        input: z.object({}),
      }).step('slow', async ({ signal }) => {
        capturedSignal = signal
        await new Promise((_, reject) => {
          signal.addEventListener('abort', () => reject(signal.reason), { once: true })
        })
      })

      const engine = createEngine({ storage, workflows: [wf] })
      await engine.enqueue('abort-on-stop', {})
      const tickPromise = engine.tick()

      await vi.waitFor(() => {
        expect(capturedSignal).toBeDefined()
      })

      engine.stop()
      await tickPromise

      expect(capturedSignal?.aborted).toBe(true)
    })

    it('tick() is guarded against concurrent calls', async () => {
      let activeRuns = 0
      let maxActiveRuns = 0

      const wf = createWorkflow({
        name: 'tick-guard',
        input: z.object({}),
      }).step('slow', async () => {
        activeRuns++
        maxActiveRuns = Math.max(maxActiveRuns, activeRuns)
        await new Promise((resolve) => setTimeout(resolve, 50))
        activeRuns--
        return { ok: true }
      })

      const engine = createEngine({ storage, workflows: [wf], concurrency: 1 })
      await engine.enqueue('tick-guard', {})
      await engine.enqueue('tick-guard', {})

      // Call tick() twice concurrently — second should be skipped
      await Promise.all([engine.tick(), engine.tick()])

      expect(maxActiveRuns).toBe(1)
    })

    it('stop() is safe to call multiple times', async () => {
      const wf = createWorkflow({
        name: 'test',
        input: z.object({}),
      }).step('a', async () => ({}))

      const engine = createEngine({ storage, workflows: [wf] })
      await engine.start(50)

      // Should not throw
      engine.stop()
      engine.stop()
      engine.stop()
    })

    it('does not overlap poll cycles while a previous tick is still running', async () => {
      let activeRuns = 0
      let maxActiveRuns = 0

      const wf = createWorkflow({
        name: 'serial-poll',
        input: z.object({}),
      }).step('slow', async () => {
        activeRuns++
        maxActiveRuns = Math.max(maxActiveRuns, activeRuns)
        await new Promise((resolve) => setTimeout(resolve, 60))
        activeRuns--
        return { ok: true }
      })

      const engine = createEngine({ storage, workflows: [wf], concurrency: 1 })
      await engine.enqueue('serial-poll', {})
      await engine.enqueue('serial-poll', {})

      await engine.start(10)
      await new Promise((resolve) => setTimeout(resolve, 180))
      engine.stop()

      expect(maxActiveRuns).toBe(1)
    })

    afterEach(() => {
      vi.restoreAllMocks()
    })
  })

  describe('getRunStatus', () => {
    it('returns run and step details for a completed run', async () => {
      const wf = createWorkflow({
        name: 'status-test',
        input: z.object({ x: z.number() }),
      })
        .step('a', async ({ input }) => ({ doubled: input.x * 2 }))
        .step('b', async ({ prev }) => ({ result: prev.doubled + 1 }))

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('status-test', { x: 5 })
      await engine.tick()

      const info = expectPresent(await engine.getRunStatus(run.id))

      expect(info.run.status).toBe('completed')
      expect(info.run.workflow).toBe('status-test')
      expect(info.steps).toHaveLength(2)
      expect(info.steps[0].name).toBe('a')
      expect(info.steps[0].output).toEqual({ doubled: 10 })
      expect(info.steps[1].name).toBe('b')
      expect(info.steps[1].output).toEqual({ result: 11 })
    })

    it('returns run with pending status before execution', async () => {
      const wf = createWorkflow({
        name: 'pending-test',
        input: z.object({}),
      }).step('a', async () => ({}))

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('pending-test', {})

      const info = expectPresent(await engine.getRunStatus(run.id))

      expect(info.run.status).toBe('pending')
      expect(info.steps).toHaveLength(0)
    })

    it('returns null for a nonexistent run id', async () => {
      const engine = createEngine({ storage, workflows: [] })

      const info = await engine.getRunStatus('nonexistent')
      expect(info).toBeNull()
    })

    it('shows failed status with error details', async () => {
      const wf = createWorkflow({
        name: 'fail-status',
        input: z.object({}),
      }).step('broken', async () => {
        throw new Error('oops')
      })

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('fail-status', {})
      await engine.tick()

      const info = expectPresent(await engine.getRunStatus(run.id))

      expect(info.run.status).toBe('failed')
      expect(info.steps[0].status).toBe('failed')
      expect(info.steps[0].error).toBe('oops')
    })
  })

  describe('hooks', () => {
    it('calls onStepComplete after each step', async () => {
      const onStepComplete = vi.fn()

      const wf = createWorkflow({
        name: 'hook-test',
        input: z.object({}),
      })
        .step('a', async () => ({ x: 1 }))
        .step('b', async () => ({ y: 2 }))

      const engine = createEngine({ storage, workflows: [wf], hooks: { onStepComplete } })
      await engine.enqueue('hook-test', {})
      await engine.tick()

      expect(onStepComplete).toHaveBeenCalledTimes(2)
      expect(onStepComplete).toHaveBeenCalledWith(
        expect.objectContaining({ stepName: 'a', output: { x: 1 }, attempts: 1 }),
      )
      expect(onStepComplete).toHaveBeenCalledWith(
        expect.objectContaining({ stepName: 'b', output: { y: 2 }, attempts: 1 }),
      )
    })

    it('calls onRunComplete when a workflow finishes successfully', async () => {
      const onRunComplete = vi.fn()

      const wf = createWorkflow({
        name: 'complete-hook',
        input: z.object({}),
      }).step('a', async () => ({ done: true }))

      const engine = createEngine({ storage, workflows: [wf], hooks: { onRunComplete } })
      const run = await engine.enqueue('complete-hook', {})
      await engine.tick()

      expect(onRunComplete).toHaveBeenCalledOnce()
      expect(onRunComplete).toHaveBeenCalledWith({
        runId: run.id,
        workflow: 'complete-hook',
      })
    })

    it('calls onRunFailed when a workflow fails', async () => {
      const onRunFailed = vi.fn()

      const wf = createWorkflow({
        name: 'fail-hook',
        input: z.object({}),
      }).step('broken', async () => {
        throw new Error('hook error')
      })

      const engine = createEngine({ storage, workflows: [wf], hooks: { onRunFailed } })
      const run = await engine.enqueue('fail-hook', {})
      await engine.tick()

      expect(onRunFailed).toHaveBeenCalledOnce()
      expect(onRunFailed).toHaveBeenCalledWith({
        runId: run.id,
        workflow: 'fail-hook',
        stepName: 'broken',
        error: expect.objectContaining({ message: 'hook error' }),
      })
    })

    it('does not throw when no hooks are configured', async () => {
      const wf = createWorkflow({
        name: 'no-hooks',
        input: z.object({}),
      }).step('a', async () => ({}))

      const engine = createEngine({ storage, workflows: [wf] })
      await engine.enqueue('no-hooks', {})
      await engine.tick() // should not throw
    })

    it('a throwing onStepComplete does not fail a successful step or run', async () => {
      const wf = createWorkflow({
        name: 'throw-step-hook',
        input: z.object({}),
      })
        .step('a', async () => ({ x: 1 }))
        .step('b', async () => ({ y: 2 }))

      const engine = createEngine({
        storage,
        workflows: [wf],
        hooks: {
          onStepComplete: () => {
            throw new Error('hook exploded')
          },
        },
      })
      const run = await engine.enqueue('throw-step-hook', {})
      await engine.tick()

      const info = expectPresent(await engine.getRunStatus(run.id))
      expect(info.run.status).toBe('completed')
      expect(info.steps).toHaveLength(2)
      expect(info.steps[0].status).toBe('completed')
      expect(info.steps[1].status).toBe('completed')
    })

    it('a throwing onRunComplete does not reject tick()', async () => {
      const wf = createWorkflow({
        name: 'throw-run-hook',
        input: z.object({}),
      }).step('a', async () => ({ done: true }))

      const engine = createEngine({
        storage,
        workflows: [wf],
        hooks: {
          onRunComplete: () => {
            throw new Error('hook exploded')
          },
        },
      })
      const run = await engine.enqueue('throw-run-hook', {})
      await expect(engine.tick()).resolves.toBeUndefined()

      const info = expectPresent(await engine.getRunStatus(run.id))
      expect(info.run.status).toBe('completed')
    })

    it('a throwing onRunFailed does not reject tick()', async () => {
      const wf = createWorkflow({
        name: 'throw-fail-hook',
        input: z.object({}),
      }).step('broken', async () => {
        throw new Error('step error')
      })

      const engine = createEngine({
        storage,
        workflows: [wf],
        hooks: {
          onRunFailed: () => {
            throw new Error('hook exploded')
          },
        },
      })
      const run = await engine.enqueue('throw-fail-hook', {})
      await expect(engine.tick()).resolves.toBeUndefined()

      const info = expectPresent(await engine.getRunStatus(run.id))
      expect(info.run.status).toBe('failed')
    })

    it('a throwing onFailure handler does not reject tick()', async () => {
      const wf = createWorkflow({
        name: 'throw-onfailure',
        input: z.object({}),
      })
        .step('broken', async () => {
          throw new Error('step error')
        })
        .onFailure(async () => {
          throw new Error('onFailure exploded')
        })

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('throw-onfailure', {})
      await expect(engine.tick()).resolves.toBeUndefined()

      const info = expectPresent(await engine.getRunStatus(run.id))
      expect(info.run.status).toBe('failed')
    })

    it('includes attempt count in onStepComplete after retries', async () => {
      const onStepComplete = vi.fn()
      let attempts = 0

      const wf = createWorkflow({
        name: 'retry-hook',
        input: z.object({}),
      }).step('flaky', {
        retry: { maxAttempts: 3, backoff: 'linear', initialDelayMs: 0 },
        handler: async () => {
          attempts++
          if (attempts < 3) throw new Error('fail')
          return { ok: true }
        },
      })

      const engine = createEngine({ storage, workflows: [wf], hooks: { onStepComplete } })
      await engine.enqueue('retry-hook', {})
      await engine.tick()

      expect(onStepComplete).toHaveBeenCalledWith(
        expect.objectContaining({ stepName: 'flaky', attempts: 3 }),
      )
    })
  })

  describe('step timeouts', () => {
    it('fails a step that exceeds its timeout', async () => {
      const wf = createWorkflow({
        name: 'timeout-test',
        input: z.object({}),
      }).step('slow', {
        timeoutMs: 50,
        handler: async () => {
          await new Promise((resolve) => setTimeout(resolve, 5000))
          return { done: true }
        },
      })

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('timeout-test', {})
      await engine.tick()

      const steps = await storage.getStepResults(run.id)
      expect(steps[0].status).toBe('failed')
      expect(steps[0].error).toContain('timed out')
    })

    it('does not timeout a fast step', async () => {
      const wf = createWorkflow({
        name: 'fast-test',
        input: z.object({}),
      }).step('quick', {
        timeoutMs: 5000,
        handler: async () => {
          return { fast: true }
        },
      })

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('fast-test', {})
      await engine.tick()

      const steps = await storage.getStepResults(run.id)
      expect(steps[0].status).toBe('completed')
      expect(steps[0].output).toEqual({ fast: true })
    })

    it('retries after timeout when retry is configured', async () => {
      let attempts = 0

      const wf = createWorkflow({
        name: 'timeout-retry',
        input: z.object({}),
      }).step('flaky-slow', {
        timeoutMs: 50,
        retry: { maxAttempts: 3, backoff: 'linear', initialDelayMs: 0 },
        handler: async () => {
          attempts++
          if (attempts < 3) {
            await new Promise((resolve) => setTimeout(resolve, 5000))
          }
          return { ok: true }
        },
      })

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('timeout-retry', {})
      await engine.tick()

      expect(attempts).toBe(3)
      const steps = await storage.getStepResults(run.id)
      expect(steps[0].status).toBe('completed')
    })

    it('stops retry backoff promptly when a timed-out run is cancelled', async () => {
      let attempts = 0

      const wf = createWorkflow({
        name: 'timeout-cancel-retry',
        input: z.object({}),
      }).step('slow', {
        timeoutMs: 15,
        retry: { maxAttempts: 3, backoff: 'linear', initialDelayMs: 200 },
        handler: async ({ signal }) => {
          attempts++
          await new Promise((resolve, reject) => {
            const timer = setTimeout(resolve, 1000)
            signal.addEventListener('abort', () => {
              clearTimeout(timer)
              reject(signal.reason)
            }, { once: true })
          })
          return { ok: true }
        },
      })

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('timeout-cancel-retry', {})
      const tickPromise = engine.tick()

      await new Promise((resolve) => setTimeout(resolve, 40))

      const cancelStartedAt = Date.now()
      expect(await engine.cancel(run.id)).toBe(true)
      await tickPromise

      expect(Date.now() - cancelStartedAt).toBeLessThan(120)
      expect(attempts).toBe(1)

      const info = expectPresent(await engine.getRunStatus(run.id))
      expect(info.run.status).toBe('cancelled')
      expect(info.steps).toHaveLength(0)
    })

    it('timeout from retry config is used when step-level timeout is not set', async () => {
      const wf = createWorkflow({
        name: 'retry-timeout',
        input: z.object({}),
      }).step('slow', {
        retry: { maxAttempts: 1, backoff: 'linear', timeoutMs: 50 },
        handler: async () => {
          await new Promise((resolve) => setTimeout(resolve, 5000))
          return {}
        },
      })

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('retry-timeout', {})
      await engine.tick()

      const steps = await storage.getStepResults(run.id)
      expect(steps[0].status).toBe('failed')
      expect(steps[0].error).toContain('timed out')
    })

    it('steps without timeout run without time limit', async () => {
      const wf = createWorkflow({
        name: 'no-timeout',
        input: z.object({}),
      }).step('normal', async () => {
        return { noTimeout: true }
      })

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('no-timeout', {})
      await engine.tick()

      const steps = await storage.getStepResults(run.id)
      expect(steps[0].status).toBe('completed')
    })
  })

  describe('cancel', () => {
    it('cancels a pending run', async () => {
      const wf = createWorkflow({
        name: 'test',
        input: z.object({}),
      }).step('a', async () => ({ done: true }))

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('test', {})

      const cancelled = await engine.cancel(run.id)
      expect(cancelled).toBe(true)

      const info = expectPresent(await engine.getRunStatus(run.id))
      expect(info.run.status).toBe('cancelled')
    })

    it('cancels a running run', async () => {
      const wf = createWorkflow({
        name: 'test',
        input: z.object({}),
      }).step('a', async () => ({ done: true }))

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('test', {})
      // Manually set to running
      await storage.updateRunStatus(run.id, 'running')

      const cancelled = await engine.cancel(run.id)
      expect(cancelled).toBe(true)

      const info = expectPresent(await engine.getRunStatus(run.id))
      expect(info.run.status).toBe('cancelled')
    })

    it('stops without running later steps when a running run is cancelled', async () => {
      let markSlowStepStarted: (() => void) | undefined
      const slowStepStarted = new Promise<void>((resolve) => {
        markSlowStepStarted = resolve
      })
      let releaseSlowStep: (() => void) | undefined
      const slowStepDone = new Promise<void>((resolve) => {
        releaseSlowStep = resolve
      })

      const afterStep = vi.fn(async () => ({ done: true }))
      const wf = createWorkflow({
        name: 'cancel-mid-run',
        input: z.object({}),
      })
        .step('slow', async () => {
          markSlowStepStarted?.()
          await slowStepDone
          return { slow: true }
        })
        .step('after', afterStep)

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('cancel-mid-run', {})
      const tickPromise = engine.tick()

      await slowStepStarted
      expect(await engine.cancel(run.id)).toBe(true)
      releaseSlowStep?.()
      await tickPromise

      const info = expectPresent(await engine.getRunStatus(run.id))
      expect(info.run.status).toBe('cancelled')
      expect(info.steps).toHaveLength(0)
      expect(afterStep).not.toHaveBeenCalled()
    })

    it('aborts the active step signal when a run is cancelled', async () => {
      let capturedSignal: AbortSignal | undefined

      const wf = createWorkflow({
        name: 'cancel-signal',
        input: z.object({}),
      }).step('slow', async ({ signal }) => {
        capturedSignal = signal
        await new Promise((_, reject) => {
          signal.addEventListener('abort', () => reject(signal.reason), { once: true })
        })
      })

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('cancel-signal', {})
      const tickPromise = engine.tick()

      await vi.waitFor(() => {
        expect(capturedSignal).toBeDefined()
      })

      expect(await engine.cancel(run.id)).toBe(true)
      await tickPromise

      expect(capturedSignal?.aborted).toBe(true)
      expect(expectPresent(await engine.getRunStatus(run.id)).run.status).toBe('cancelled')
    })

    it('returns false for a completed run', async () => {
      const wf = createWorkflow({
        name: 'test',
        input: z.object({}),
      }).step('a', async () => ({ done: true }))

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('test', {})
      await engine.tick()

      const cancelled = await engine.cancel(run.id)
      expect(cancelled).toBe(false)
    })

    it('returns false for a nonexistent run', async () => {
      const wf = createWorkflow({
        name: 'test',
        input: z.object({}),
      }).step('a', async () => ({}))

      const engine = createEngine({ storage, workflows: [wf] })
      const cancelled = await engine.cancel('nonexistent')
      expect(cancelled).toBe(false)
    })

    it('prevents a cancelled run from being claimed', async () => {
      const handler = vi.fn().mockResolvedValue({ done: true })
      const wf = createWorkflow({
        name: 'test',
        input: z.object({}),
      }).step('a', handler)

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('test', {})
      await engine.cancel(run.id)
      await engine.tick()

      expect(handler).not.toHaveBeenCalled()
    })
  })

  describe('concurrency', () => {
    it('processes multiple runs per tick when concurrency > 1', async () => {
      const handler = vi.fn().mockResolvedValue({ done: true })
      const wf = createWorkflow({
        name: 'test',
        input: z.object({}),
      }).step('a', handler)

      const engine = createEngine({ storage, workflows: [wf], concurrency: 3 })
      await engine.enqueue('test', {})
      await engine.enqueue('test', {})
      await engine.enqueue('test', {})

      await engine.tick()

      expect(handler).toHaveBeenCalledTimes(3)
    })

    it('handles fewer runs than concurrency limit', async () => {
      const handler = vi.fn().mockResolvedValue({ done: true })
      const wf = createWorkflow({
        name: 'test',
        input: z.object({}),
      }).step('a', handler)

      const engine = createEngine({ storage, workflows: [wf], concurrency: 5 })
      await engine.enqueue('test', {})
      await engine.enqueue('test', {})

      await engine.tick()

      expect(handler).toHaveBeenCalledTimes(2)
    })

    it('defaults to concurrency of 1', async () => {
      const handler = vi.fn().mockResolvedValue({ done: true })
      const wf = createWorkflow({
        name: 'test',
        input: z.object({}),
      }).step('a', handler)

      const engine = createEngine({ storage, workflows: [wf] })
      await engine.enqueue('test', {})
      await engine.enqueue('test', {})

      await engine.tick()

      expect(handler).toHaveBeenCalledTimes(1)
    })
  })

  describe('schedule / unschedule', () => {
    beforeEach(() => {
      vi.useFakeTimers()
    })

    afterEach(() => {
      vi.useRealTimers()
    })

    it('enqueues runs on an interval', async () => {
      const wf = createWorkflow({
        name: 'test',
        input: z.object({ x: z.number() }),
      }).step('a', async ({ input }) => input)

      const engine = createEngine({ storage, workflows: [wf] })
      engine.schedule('test', { x: 1 }, 1000)

      await vi.advanceTimersByTimeAsync(3500)

      // Should have enqueued 3 runs (at 1s, 2s, 3s)
      const info1 = await storage.claimNextRun(['test'])
      const info2 = await storage.claimNextRun(['test'])
      const info3 = await storage.claimNextRun(['test'])
      const info4 = await storage.claimNextRun(['test'])

      expect(info1).not.toBeNull()
      expect(info2).not.toBeNull()
      expect(info3).not.toBeNull()
      expect(info4).toBeNull()

      engine.stop()
    })

    it('returns a schedule id', () => {
      const wf = createWorkflow({
        name: 'test',
        input: z.object({}),
      }).step('a', async () => ({}))

      const engine = createEngine({ storage, workflows: [wf] })
      const id = engine.schedule('test', {}, 5000)

      expect(id).toBeTruthy()
      expect(typeof id).toBe('string')

      engine.stop()
    })

    it('unschedule stops the interval', async () => {
      const wf = createWorkflow({
        name: 'test',
        input: z.object({}),
      }).step('a', async () => ({}))

      const engine = createEngine({ storage, workflows: [wf] })
      const id = engine.schedule('test', {}, 1000)

      await vi.advanceTimersByTimeAsync(1500)
      engine.unschedule(id)
      await vi.advanceTimersByTimeAsync(3000)

      // Only 1 run should have been enqueued (at 1s, then unscheduled at 1.5s)
      const r1 = await storage.claimNextRun(['test'])
      const r2 = await storage.claimNextRun(['test'])
      expect(r1).not.toBeNull()
      expect(r2).toBeNull()

      engine.stop()
    })

    it('unschedule returns false for unknown id', () => {
      const wf = createWorkflow({
        name: 'test',
        input: z.object({}),
      }).step('a', async () => ({}))

      const engine = createEngine({ storage, workflows: [wf] })
      expect(engine.unschedule('nonexistent')).toBe(false)
    })

    it('throws for unknown workflow name', () => {
      const wf = createWorkflow({
        name: 'test',
        input: z.object({}),
      }).step('a', async () => ({}))

      const engine = createEngine({ storage, workflows: [wf] })
      expect(() => engine.schedule('unknown' as any, {}, 1000)).toThrow('Workflow "unknown" not found')
    })

    it('throws for non-positive interval', () => {
      const wf = createWorkflow({
        name: 'test',
        input: z.object({}),
      }).step('a', async () => ({}))

      const engine = createEngine({ storage, workflows: [wf] })
      expect(() => engine.schedule('test', {}, 0)).toThrow(/positive number/)
      expect(() => engine.schedule('test', {}, -1)).toThrow(/positive number/)
    })

    it('schedule swallows enqueue errors instead of crashing the process', async () => {
      const wf = createWorkflow({
        name: 'test',
        input: z.object({}),
      }).step('a', async () => ({}))

      // Use a storage that fails on createRun to trigger an async error inside the interval
      const failStorage = new MemoryStorage()
      await failStorage.initialize()
      failStorage.createRun = async () => { throw new Error('db down') }

      const engine = createEngine({ storage: failStorage, workflows: [wf] })
      engine.schedule('test', {}, 1000)

      // Should not cause an unhandled rejection
      await vi.advanceTimersByTimeAsync(1500)
      engine.stop()
    })

    it('stop() clears all schedules', async () => {
      const wf = createWorkflow({
        name: 'test',
        input: z.object({}),
      }).step('a', async () => ({}))

      const engine = createEngine({ storage, workflows: [wf] })
      engine.schedule('test', {}, 1000)
      engine.schedule('test', {}, 2000)

      engine.stop()
      await vi.advanceTimersByTimeAsync(5000)

      const r = await storage.claimNextRun(['test'])
      expect(r).toBeNull()
    })
  })

  describe('steps context', () => {
    it('step handler receives steps accumulator with previous step results', async () => {
      const captured: Record<string, unknown>[] = []
      const wf = createWorkflow({ name: 'steps-ctx', input: z.object({ x: z.number() }) })
        .step('a', async ({ steps }) => {
          captured.push({ ...steps })
          return { fromA: 1 }
        })
        .step('b', async ({ steps }) => {
          captured.push({ ...steps })
          return { fromB: 2 }
        })
        .step('c', async ({ steps }) => {
          captured.push({ ...steps })
          return { fromC: 3 }
        })

      const engine = createEngine({ storage, workflows: [wf] })
      await engine.enqueue('steps-ctx', { x: 0 })
      await engine.tick()

      expect(captured[0]).toEqual({})
      expect(captured[1]).toEqual({ a: { fromA: 1 } })
      expect(captured[2]).toEqual({ a: { fromA: 1 }, b: { fromB: 2 } })
    })

    it('steps accumulator is populated from persisted results on crash recovery', async () => {
      const captured: Record<string, unknown>[] = []
      let failOnce = true
      const wf = createWorkflow({ name: 'steps-resume', input: z.object({}) })
        .step('a', async () => {
          return { fromA: 'hello' }
        })
        .step('b', {
          retry: { maxAttempts: 2, backoff: 'linear', initialDelayMs: 0 },
          handler: async ({ steps }) => {
            if (failOnce) {
              failOnce = false
              throw new Error('transient failure')
            }
            captured.push({ ...steps })
            return { fromB: 'world' }
          },
        })

      const engine = createEngine({ storage, workflows: [wf] })
      await engine.enqueue('steps-resume', {})
      await engine.tick()

      expect(captured[0]).toEqual({ a: { fromA: 'hello' } })
    })

    it('freezing steps does not freeze prev', async () => {
      let prevFrozen = false
      const wf = createWorkflow({ name: 'steps-prev', input: z.object({}) })
        .step('a', async () => ({ nested: { val: 1 } }))
        .step('b', async ({ prev }) => {
          prevFrozen = Object.isFrozen(prev)
          return {}
        })

      const engine = createEngine({ storage, workflows: [wf] })
      await engine.enqueue('steps-prev', {})
      await engine.tick()

      expect(prevFrozen).toBe(false)
    })

    it('steps object is deeply frozen', async () => {
      let outerFrozen = false
      let innerFrozen = false
      const wf = createWorkflow({ name: 'steps-frozen', input: z.object({}) })
        .step('a', async () => ({ nested: { val: 1 } }))
        .step('b', async ({ steps }) => {
          outerFrozen = Object.isFrozen(steps)
          innerFrozen = Object.isFrozen((steps as Record<string, Record<string, unknown>>).a.nested)
          return {}
        })

      const engine = createEngine({ storage, workflows: [wf] })
      await engine.enqueue('steps-frozen', {})
      await engine.tick()

      expect(outerFrozen).toBe(true)
      expect(innerFrozen).toBe(true)
    })
  })

  describe('early complete', () => {
    it('complete() stops workflow and marks run as completed', async () => {
      const steps: string[] = []
      const wf = createWorkflow({ name: 'early', input: z.object({}) })
        .step('check', async ({ complete }) => {
          steps.push('check')
          return complete()
        })
        .step('never-runs', async () => {
          steps.push('never-runs')
          return {}
        })

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('early', {})
      await engine.tick()

      const info = expectPresent(await engine.getRunStatus(run.id))
      expect(info.run.status).toBe('completed')
      expect(steps).toEqual(['check'])
    })

    it('complete(value) persists the value as step result', async () => {
      const wf = createWorkflow({ name: 'early-val', input: z.object({}) })
        .step('check', async ({ complete }) => {
          return complete({ reason: 'ineligible' })
        })
        .step('never-runs', async () => ({}))

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('early-val', {})
      await engine.tick()

      const info = expectPresent(await engine.getRunStatus(run.id))
      expect(info.run.status).toBe('completed')
      expect(info.steps).toHaveLength(1)
      expect(info.steps[0].output).toEqual({ reason: 'ineligible' })
    })

    it('complete() does not trigger onFailure', async () => {
      const failureCalled = vi.fn()
      const wf = createWorkflow({ name: 'early-nofail', input: z.object({}) })
        .step('check', async ({ complete }) => complete())
        .step('b', async () => ({}))
        .onFailure(async () => { failureCalled() })

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('early-nofail', {})
      await engine.tick()

      expect(failureCalled).not.toHaveBeenCalled()
      const info = expectPresent(await engine.getRunStatus(run.id))
      expect(info.run.status).toBe('completed')
    })

    it('complete() fires onRunComplete hook', async () => {
      const hookCalled = vi.fn()
      const wf = createWorkflow({ name: 'early-hook', input: z.object({}) })
        .step('check', async ({ complete }) => complete())
        .step('b', async () => ({}))

      const engine = createEngine({
        storage, workflows: [wf],
        hooks: { onRunComplete: hookCalled },
      })
      const run = await engine.enqueue('early-hook', {})
      await engine.tick()

      expect(hookCalled).toHaveBeenCalledWith({ runId: run.id, workflow: 'early-hook' })
    })

    it('complete() fires onStepComplete hook', async () => {
      const stepHook = vi.fn()
      const wf = createWorkflow({ name: 'early-step-hook', input: z.object({}) })
        .step('check', async ({ complete }) => complete({ done: true }))
        .step('b', async () => ({}))

      const engine = createEngine({
        storage, workflows: [wf],
        hooks: { onStepComplete: stepHook },
      })
      const run = await engine.enqueue('early-step-hook', {})
      await engine.tick()

      expect(stepHook).toHaveBeenCalledWith({
        runId: run.id,
        stepName: 'check',
        output: { done: true },
        attempts: 1,
      })
    })

    it('steps context is populated when complete() is called from a non-first step', async () => {
      let capturedSteps: Record<string, unknown> | undefined
      const wf = createWorkflow({ name: 'early-steps', input: z.object({}) })
        .step('a', async () => ({ fromA: 42 }))
        .step('b', async ({ steps, complete }) => {
          capturedSteps = { ...steps }
          return complete({ fromB: 99 })
        })
        .step('never', async () => ({}))

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('early-steps', {})
      await engine.tick()

      expect(capturedSteps).toEqual({ a: { fromA: 42 } })
      const info = expectPresent(await engine.getRunStatus(run.id))
      expect(info.run.status).toBe('completed')
      expect(info.steps).toHaveLength(2)
    })

    it('complete() on the last step behaves like normal completion', async () => {
      const wf = createWorkflow({ name: 'early-last', input: z.object({}) })
        .step('a', async () => ({ fromA: 1 }))
        .step('last', async ({ complete }) => complete({ final: true }))

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('early-last', {})
      await engine.tick()

      const info = expectPresent(await engine.getRunStatus(run.id))
      expect(info.run.status).toBe('completed')
      expect(info.steps).toHaveLength(2)
      expect(info.steps[1].output).toEqual({ final: true })
    })

    it('complete() without value persists step with undefined output', async () => {
      const wf = createWorkflow({ name: 'early-noval', input: z.object({}) })
        .step('check', async ({ complete }) => complete())
        .step('b', async () => ({}))

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('early-noval', {})
      await engine.tick()

      const info = expectPresent(await engine.getRunStatus(run.id))
      expect(info.steps).toHaveLength(1)
      expect(info.steps[0].status).toBe('completed-early')
    })

    it('completed-early step is detected on crash recovery', async () => {
      const stepHook = vi.fn()
      const runHook = vi.fn()
      const neverRuns = vi.fn()
      const wf = createWorkflow({ name: 'early-recover', input: z.object({}) })
        .step('check', async ({ complete }) => complete({ done: true }))
        .step('after', async () => {
          neverRuns()
          return {}
        })

      // First engine: enqueue the run and simulate a partial execution
      // where the early-complete step was saved but the run was not marked completed
      const engine1 = createEngine({ storage, workflows: [wf] })
      const run = await engine1.enqueue('early-recover', {})

      // Manually claim and save the early-complete step result (simulating crash after step save)
      const claimed = expectPresent(await storage.claimNextRun(['early-recover']))
      await storage.saveStepResult({
        id: 'step-early',
        runId: run.id,
        name: 'check',
        status: 'completed-early',
        output: { done: true },
        error: null,
        attempts: 1,
        createdAt: Date.now(),
        updatedAt: Date.now(),
      }, claimed.leaseId)

      // Simulate lease expiry so a second engine can reclaim
      // Force the run back to reclaimable by updating its timestamp
      await storage.heartbeatRun(run.id, claimed.leaseId)

      // Second engine: reclaims the stale run and should detect completed-early
      const engine2 = createEngine({
        storage,
        workflows: [wf],
        hooks: { onStepComplete: stepHook, onRunComplete: runHook },
        runLeaseDurationMs: 10,
        heartbeatIntervalMs: 5,
      })

      // Small delay to ensure the lease is stale
      await new Promise((resolve) => setTimeout(resolve, 20))
      await engine2.tick()

      // The run should be completed without executing the 'after' step
      const info = expectPresent(await engine2.getRunStatus(run.id))
      expect(info.run.status).toBe('completed')
      expect(neverRuns).not.toHaveBeenCalled()
      expect(stepHook).toHaveBeenCalledWith({
        runId: run.id,
        stepName: 'check',
        output: { done: true },
        attempts: 1,
      })
      expect(runHook).toHaveBeenCalledWith({ runId: run.id, workflow: 'early-recover' })
    })
  })
})
