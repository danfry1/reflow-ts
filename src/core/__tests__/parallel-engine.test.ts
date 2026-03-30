import { randomUUID } from 'node:crypto'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { z } from 'zod'
import { createWorkflow } from '../workflow'
import { createEngine } from '../engine'
import { MemoryStorage } from '../../storage/memory'
import { ParallelCompleteError } from '../errors'
import type { PersistedValue } from '../types'

function expectPresent<T>(value: T | null | undefined): T {
  expect(value).not.toBeNull()
  expect(value).not.toBeUndefined()

  if (value == null) {
    throw new Error('Expected value to be present')
  }

  return value
}

describe('Parallel engine execution', () => {
  let storage: MemoryStorage

  beforeEach(async () => {
    storage = new MemoryStorage()
    await storage.initialize()
  })

  describe('basic execution', () => {
    it('executes parallel branches and merges results correctly', async () => {
      const wf = createWorkflow({ name: 'parallel-basic', input: z.object({ x: z.number() }) })
        .parallel({
          a: async ({ input }) => ({ doubled: input.x * 2 }),
          b: async ({ input }) => ({ tripled: input.x * 3 }),
        })

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('parallel-basic', { x: 5 })
      await engine.tick()

      const info = expectPresent(await engine.getRunStatus(run.id))
      expect(info.run.status).toBe('completed')
      expect(info.steps).toHaveLength(2)

      const stepA = info.steps.find((s) => s.name === 'a')
      const stepB = info.steps.find((s) => s.name === 'b')
      expect(stepA?.output).toEqual({ doubled: 10 })
      expect(stepB?.output).toEqual({ tripled: 15 })
    })

    it('passes correct prev from preceding step to all branches', async () => {
      const capturedPrevs: PersistedValue[] = []

      const wf = createWorkflow({ name: 'prev-pass', input: z.object({}) })
        .step('setup', async () => ({ value: 42 }))
        .parallel({
          a: async ({ prev }) => {
            capturedPrevs.push(prev)
            return { fromA: (prev as { value: number }).value + 1 }
          },
          b: async ({ prev }) => {
            capturedPrevs.push(prev)
            return { fromB: (prev as { value: number }).value + 2 }
          },
        })

      const engine = createEngine({ storage, workflows: [wf] })
      await engine.enqueue('prev-pass', {})
      await engine.tick()

      expect(capturedPrevs).toHaveLength(2)
      for (const p of capturedPrevs) {
        expect(p).toEqual({ value: 42 })
      }
    })

    it('passes frozen steps snapshot to all branches', async () => {
      let aFrozen = false
      let bFrozen = false

      const wf = createWorkflow({ name: 'frozen-steps', input: z.object({}) })
        .step('setup', async () => ({ nested: { val: 1 } }))
        .parallel({
          a: async ({ steps }) => {
            aFrozen = Object.isFrozen(steps)
            return {}
          },
          b: async ({ steps }) => {
            bFrozen = Object.isFrozen(steps)
            return {}
          },
        })

      const engine = createEngine({ storage, workflows: [wf] })
      await engine.enqueue('frozen-steps', {})
      await engine.tick()

      expect(aFrozen).toBe(true)
      expect(bFrozen).toBe(true)
    })

    it('persists individual step results for each branch', async () => {
      const wf = createWorkflow({ name: 'persist-branches', input: z.object({}) })
        .parallel({
          a: async () => ({ fromA: 1 }),
          b: async () => ({ fromB: 2 }),
        })

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('persist-branches', {})
      await engine.tick()

      const steps = await storage.getStepResults(run.id)
      expect(steps).toHaveLength(2)

      const stepA = steps.find((s) => s.name === 'a')
      const stepB = steps.find((s) => s.name === 'b')
      expect(stepA?.status).toBe('completed')
      expect(stepA?.output).toEqual({ fromA: 1 })
      expect(stepB?.status).toBe('completed')
      expect(stepB?.output).toEqual({ fromB: 2 })
    })

    it('sets prev to merged record for the step after a parallel group', async () => {
      let capturedPrev: PersistedValue = undefined

      const wf = createWorkflow({ name: 'merged-prev', input: z.object({}) })
        .parallel({
          a: async () => ({ fromA: 10 }),
          b: async () => ({ fromB: 20 }),
        })
        .step('after', async ({ prev }) => {
          capturedPrev = prev
          return {}
        })

      const engine = createEngine({ storage, workflows: [wf] })
      await engine.enqueue('merged-prev', {})
      await engine.tick()

      expect(capturedPrev).toEqual({ a: { fromA: 10 }, b: { fromB: 20 } })
    })
  })

  describe('fail-fast', () => {
    it('aborts sibling branches when one fails', async () => {
      let bSignalAborted = false

      const wf = createWorkflow({ name: 'fail-fast', input: z.object({}) })
        .parallel({
          a: async () => {
            throw new Error('branch a failed')
          },
          b: async ({ signal }) => {
            // Wait long enough for a to fail
            await new Promise<void>((resolve, reject) => {
              const timer = setTimeout(resolve, 5000)
              signal.addEventListener('abort', () => {
                clearTimeout(timer)
                bSignalAborted = true
                reject(signal.reason)
              }, { once: true })
            })
            return {}
          },
        })

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('fail-fast', {})
      await engine.tick()

      const info = expectPresent(await engine.getRunStatus(run.id))
      expect(info.run.status).toBe('failed')
      expect(bSignalAborted).toBe(true)
    })

    it('reports the first failing branch name in onRunFailed', async () => {
      const onRunFailed = vi.fn()

      const wf = createWorkflow({ name: 'fail-report', input: z.object({}) })
        .parallel({
          branchX: async () => {
            throw new Error('branchX error')
          },
          branchY: async ({ signal }) => {
            await new Promise<void>((resolve, reject) => {
              const timer = setTimeout(resolve, 5000)
              signal.addEventListener('abort', () => {
                clearTimeout(timer)
                reject(signal.reason)
              }, { once: true })
            })
            return {}
          },
        })

      const engine = createEngine({ storage, workflows: [wf], hooks: { onRunFailed } })
      await engine.enqueue('fail-report', {})
      await engine.tick()

      expect(onRunFailed).toHaveBeenCalledOnce()
      expect(onRunFailed).toHaveBeenCalledWith(
        expect.objectContaining({ stepName: 'branchX' }),
      )
    })

    it('calls onFailure handler with failing branch name', async () => {
      const failHandler = vi.fn(async () => {})

      const wf = createWorkflow({ name: 'fail-handler', input: z.object({ x: z.number() }) })
        .parallel({
          broken: async () => {
            throw new Error('handler error')
          },
          ok: async ({ signal }) => {
            await new Promise<void>((resolve, reject) => {
              const timer = setTimeout(resolve, 5000)
              signal.addEventListener('abort', () => {
                clearTimeout(timer)
                reject(signal.reason)
              }, { once: true })
            })
            return {}
          },
        })
        .onFailure(failHandler)

      const engine = createEngine({ storage, workflows: [wf] })
      await engine.enqueue('fail-handler', { x: 1 })
      await engine.tick()

      expect(failHandler).toHaveBeenCalledOnce()
      expect(failHandler).toHaveBeenCalledWith(
        expect.objectContaining({
          stepName: 'broken',
          error: expect.objectContaining({ message: 'handler error' }),
        }),
      )
    })
  })

  describe('complete() prevention', () => {
    it('throws ParallelCompleteError when complete() is called in a branch', async () => {
      const onRunFailed = vi.fn()

      const wf = createWorkflow({ name: 'no-complete', input: z.object({}) })
        .parallel({
          bad: async ({ complete }) => {
            return complete('done')
          },
          good: async ({ signal }) => {
            await new Promise<void>((resolve, reject) => {
              const timer = setTimeout(resolve, 5000)
              signal.addEventListener('abort', () => {
                clearTimeout(timer)
                reject(signal.reason)
              }, { once: true })
            })
            return {}
          },
        })

      const engine = createEngine({ storage, workflows: [wf], hooks: { onRunFailed } })
      const run = await engine.enqueue('no-complete', {})
      await engine.tick()

      const info = expectPresent(await engine.getRunStatus(run.id))
      expect(info.run.status).toBe('failed')
      expect(onRunFailed).toHaveBeenCalledOnce()
      expect(onRunFailed).toHaveBeenCalledWith(
        expect.objectContaining({
          error: expect.any(ParallelCompleteError),
          stepName: 'bad',
        }),
      )
    })
  })

  describe('per-branch retry', () => {
    it('retries a failing branch independently', async () => {
      let attempts = 0

      const wf = createWorkflow({ name: 'retry-branch', input: z.object({}) })
        .parallel({
          flaky: {
            retry: { maxAttempts: 3, backoff: 'linear', initialDelayMs: 0 },
            handler: async () => {
              attempts++
              if (attempts < 3) {
                throw new Error('transient')
              }
              return { recovered: true }
            },
          },
          stable: async () => ({ stable: true }),
        })

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('retry-branch', {})
      await engine.tick()

      const info = expectPresent(await engine.getRunStatus(run.id))
      expect(info.run.status).toBe('completed')
      expect(attempts).toBe(3)

      const flakyStep = info.steps.find((s) => s.name === 'flaky')
      expect(flakyStep?.output).toEqual({ recovered: true })
      expect(flakyStep?.attempts).toBe(3)
    })
  })

  describe('per-branch timeout', () => {
    it('times out an individual branch without affecting siblings until fail-fast', async () => {
      const onRunFailed = vi.fn()

      const wf = createWorkflow({ name: 'timeout-branch', input: z.object({}) })
        .parallel({
          slow: {
            timeoutMs: 50,
            handler: async ({ signal }) => {
              await new Promise<void>((resolve, reject) => {
                const timer = setTimeout(resolve, 10_000)
                signal.addEventListener('abort', () => {
                  clearTimeout(timer)
                  reject(signal.reason)
                }, { once: true })
              })
              return {}
            },
          },
          fast: async ({ signal }) => {
            await new Promise<void>((resolve, reject) => {
              const timer = setTimeout(resolve, 10_000)
              signal.addEventListener('abort', () => {
                clearTimeout(timer)
                reject(signal.reason)
              }, { once: true })
            })
            return {}
          },
        })

      const engine = createEngine({ storage, workflows: [wf], hooks: { onRunFailed } })
      const run = await engine.enqueue('timeout-branch', {})
      await engine.tick()

      const info = expectPresent(await engine.getRunStatus(run.id))
      expect(info.run.status).toBe('failed')
      expect(onRunFailed).toHaveBeenCalledOnce()
      expect(onRunFailed).toHaveBeenCalledWith(
        expect.objectContaining({ stepName: 'slow' }),
      )
    })
  })

  describe('hooks', () => {
    it('fires onStepStart for each branch', async () => {
      const onStepStart = vi.fn()

      const wf = createWorkflow({ name: 'hooks-start', input: z.object({}) })
        .parallel({
          a: async () => ({ fromA: 1 }),
          b: async () => ({ fromB: 2 }),
        })

      const engine = createEngine({ storage, workflows: [wf], hooks: { onStepStart } })
      await engine.enqueue('hooks-start', {})
      await engine.tick()

      expect(onStepStart).toHaveBeenCalledTimes(2)
      expect(onStepStart).toHaveBeenCalledWith(
        expect.objectContaining({ stepName: 'a' }),
      )
      expect(onStepStart).toHaveBeenCalledWith(
        expect.objectContaining({ stepName: 'b' }),
      )
    })

    it('fires onStepComplete for each branch', async () => {
      const onStepComplete = vi.fn()

      const wf = createWorkflow({ name: 'hooks-complete', input: z.object({}) })
        .parallel({
          a: async () => ({ fromA: 1 }),
          b: async () => ({ fromB: 2 }),
        })

      const engine = createEngine({ storage, workflows: [wf], hooks: { onStepComplete } })
      await engine.enqueue('hooks-complete', {})
      await engine.tick()

      expect(onStepComplete).toHaveBeenCalledTimes(2)
      expect(onStepComplete).toHaveBeenCalledWith(
        expect.objectContaining({ stepName: 'a', output: { fromA: 1 }, attempts: 1 }),
      )
      expect(onStepComplete).toHaveBeenCalledWith(
        expect.objectContaining({ stepName: 'b', output: { fromB: 2 }, attempts: 1 }),
      )
    })
  })

  describe('crash recovery', () => {
    it('re-runs entire parallel group when partially completed', async () => {
      let branchACalls = 0

      const wf = createWorkflow({ name: 'partial-recovery', input: z.object({}) })
        .parallel({
          a: async () => {
            branchACalls++
            return { fromA: 'recovered' }
          },
          b: async () => ({ fromB: 'new' }),
        })

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('partial-recovery', {})

      // Simulate partial completion: only branch 'a' was persisted from a previous run
      const now = Date.now()
      await storage.saveStepResult({
        id: randomUUID(),
        runId: run.id,
        name: 'a',
        status: 'completed',
        output: { fromA: 'old' },
        error: null,
        attempts: 1,
        createdAt: now,
        updatedAt: now,
      })

      await engine.tick()

      // Branch A should have been re-run (since not ALL branches were complete)
      expect(branchACalls).toBe(1)

      const info = expectPresent(await engine.getRunStatus(run.id))
      expect(info.run.status).toBe('completed')

      // The step results should include the re-run results
      const stepA = info.steps.find((s) => s.name === 'a' && s.output !== null)
      const stepB = info.steps.find((s) => s.name === 'b')
      expect(stepA).toBeDefined()
      expect(stepB?.output).toEqual({ fromB: 'new' })
    })

    it('skips parallel group when all branches already completed', async () => {
      let branchACalls = 0
      let branchBCalls = 0

      const wf = createWorkflow({ name: 'skip-group', input: z.object({}) })
        .parallel({
          a: async () => {
            branchACalls++
            return { fromA: 'should-not-run' }
          },
          b: async () => {
            branchBCalls++
            return { fromB: 'should-not-run' }
          },
        })
        .step('after', async ({ prev }) => prev)

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('skip-group', {})

      // Simulate full completion: both branches were persisted
      const now = Date.now()
      await storage.saveStepResult({
        id: randomUUID(),
        runId: run.id,
        name: 'a',
        status: 'completed',
        output: { fromA: 'cached' },
        error: null,
        attempts: 1,
        createdAt: now,
        updatedAt: now,
      })
      await storage.saveStepResult({
        id: randomUUID(),
        runId: run.id,
        name: 'b',
        status: 'completed',
        output: { fromB: 'cached' },
        error: null,
        attempts: 1,
        createdAt: now,
        updatedAt: now,
      })

      await engine.tick()

      // Neither branch should have been called
      expect(branchACalls).toBe(0)
      expect(branchBCalls).toBe(0)

      const info = expectPresent(await engine.getRunStatus(run.id))
      expect(info.run.status).toBe('completed')

      // The step after parallel should have received the cached merged output
      const afterStep = info.steps.find((s) => s.name === 'after')
      expect(afterStep?.output).toEqual({ a: { fromA: 'cached' }, b: { fromB: 'cached' } })
    })
  })
})
