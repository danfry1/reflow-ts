import { randomUUID } from 'node:crypto'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { z } from 'zod'
import { createWorkflow } from '../workflow'
import { createEngine } from '../engine'
import { MemoryStorage } from '../../storage/memory'
import { ParallelCompleteError } from '../errors'
import type { PersistedValue, StorageAdapter } from '../types'

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

    it('reports the branch that caused the abort, not an aborted sibling', async () => {
      // When branch B fails first and aborts branch A, A's failure is an induced
      // side-effect — not the underlying cause. onRunFailed must surface B's name and
      // B's actual error, otherwise debugging a production incident points at the wrong step.
      const onRunFailed = vi.fn()

      const wf = createWorkflow({ name: 'cause-reporting', input: z.object({}) })
        .parallel({
          first: async ({ signal }) => {
            // Cooperative wait — will be aborted by 'second' failing.
            await new Promise<void>((resolve, reject) => {
              const timer = setTimeout(resolve, 5000)
              signal.addEventListener('abort', () => {
                clearTimeout(timer)
                reject(signal.reason)
              }, { once: true })
            })
            return {}
          },
          second: async () => {
            throw new Error('underlying cause')
          },
        })

      const engine = createEngine({ storage, workflows: [wf], hooks: { onRunFailed } })
      await engine.enqueue('cause-reporting', {})
      await engine.tick()

      expect(onRunFailed).toHaveBeenCalledOnce()
      expect(onRunFailed).toHaveBeenCalledWith(
        expect.objectContaining({
          stepName: 'second',
          error: expect.objectContaining({ message: 'underlying cause' }),
        }),
      )
    })

    it('persists only the causing branch as failed, not aborted siblings', async () => {
      const wf = createWorkflow({ name: 'failed-row-cause', input: z.object({}) })
        .parallel({
          aborted: async ({ signal }) => {
            await new Promise<void>((resolve, reject) => {
              const timer = setTimeout(resolve, 5000)
              signal.addEventListener('abort', () => {
                clearTimeout(timer)
                reject(signal.reason)
              }, { once: true })
            })
            return {}
          },
          cause: async () => {
            throw new Error('real failure')
          },
        })

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('failed-row-cause', {})
      await engine.tick()

      const info = expectPresent(await engine.getRunStatus(run.id))
      expect(info.run.status).toBe('failed')

      const failedRows = info.steps.filter((s) => s.status === 'failed')
      expect(failedRows).toHaveLength(1)
      expect(failedRows[0].name).toBe('cause')
      expect(failedRows[0].error).toBe('real failure')
    })

    it('does not surface unhandled rejections when multiple branches fail concurrently', async () => {
      // Promise.all would consume the first rejection and leave siblings' rejections
      // unhandled. Promise.allSettled collects all settlements so no listener-less
      // rejection escapes.
      const rejections: unknown[] = []
      const onUnhandled = (reason: unknown) => rejections.push(reason)
      process.on('unhandledRejection', onUnhandled)

      try {
        const wf = createWorkflow({ name: 'multi-fail', input: z.object({}) })
          .parallel({
            a: async () => {
              throw new Error('a failed')
            },
            b: async () => {
              throw new Error('b failed')
            },
            c: async () => {
              throw new Error('c failed')
            },
          })

        const engine = createEngine({ storage, workflows: [wf] })
        await engine.enqueue('multi-fail', {})
        await engine.tick()

        // Give the microtask queue a chance to drain potential unhandled rejections.
        await new Promise((r) => setTimeout(r, 10))

        expect(rejections).toHaveLength(0)
      } finally {
        process.off('unhandledRejection', onUnhandled)
      }
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
    it('does not exhaust retry attempts on a sibling after the group is aborted', async () => {
      // When branch A fails and aborts the group, branch B (with retries configured)
      // should bail immediately rather than burning through retries that observe the
      // already-aborted signal.
      let bAttempts = 0

      const wf = createWorkflow({ name: 'no-retry-after-abort', input: z.object({}) })
        .parallel({
          a: async () => {
            // small delay so b registers attempts before the abort
            await new Promise((r) => setTimeout(r, 5))
            throw new Error('a failed')
          },
          b: {
            retry: { maxAttempts: 5, backoff: 'linear', initialDelayMs: 0 },
            handler: async ({ signal }) => {
              bAttempts++
              await new Promise<void>((resolve, reject) => {
                const timer = setTimeout(resolve, 1000)
                signal.addEventListener('abort', () => {
                  clearTimeout(timer)
                  reject(signal.reason)
                }, { once: true })
              })
              return {}
            },
          },
        })

      const engine = createEngine({ storage, workflows: [wf] })
      await engine.enqueue('no-retry-after-abort', {})
      await engine.tick()

      // Even though maxAttempts is 5, b should only have run once before the abort
      // propagated and stopped further attempts.
      expect(bAttempts).toBe(1)
    })

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
    it('skips already-completed branches and only re-runs missing ones', async () => {
      let branchACalls = 0
      let branchBCalls = 0

      const wf = createWorkflow({ name: 'partial-recovery', input: z.object({}) })
        .parallel({
          a: async () => {
            branchACalls++
            return { fromA: 'fresh' }
          },
          b: async () => {
            branchBCalls++
            return { fromB: 'fresh' }
          },
        })

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('partial-recovery', {})

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

      await engine.tick()

      expect(branchACalls).toBe(0)
      expect(branchBCalls).toBe(1)

      const info = expectPresent(await engine.getRunStatus(run.id))
      expect(info.run.status).toBe('completed')

      // No duplicate rows — exactly one record per branch
      const rowsA = info.steps.filter((s) => s.name === 'a')
      const rowsB = info.steps.filter((s) => s.name === 'b')
      expect(rowsA).toHaveLength(1)
      expect(rowsB).toHaveLength(1)
      expect(rowsA[0].output).toEqual({ fromA: 'cached' })
      expect(rowsB[0].output).toEqual({ fromB: 'fresh' })
    })

    it('merges cached and fresh branch outputs for the next step', async () => {
      let capturedPrev: PersistedValue = undefined

      const wf = createWorkflow({ name: 'partial-merge', input: z.object({}) })
        .parallel({
          a: async () => ({ fromA: 'fresh' }),
          b: async () => ({ fromB: 'fresh' }),
        })
        .step('after', async ({ prev }) => {
          capturedPrev = prev
          return {}
        })

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('partial-merge', {})

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

      await engine.tick()

      expect(capturedPrev).toEqual({ a: { fromA: 'cached' }, b: { fromB: 'fresh' } })
    })

    it('skips an already-failed branch from a prior run and lets fresh ones complete', async () => {
      // When the prior run persisted a 'failed' record (e.g. mid-run lease loss), the
      // engine should retry that branch fresh — failed records don't count as completed.
      let branchACalls = 0

      const wf = createWorkflow({ name: 'failed-retry', input: z.object({}) })
        .parallel({
          a: async () => {
            branchACalls++
            return { fromA: 'recovered' }
          },
          b: async () => ({ fromB: 'fresh' }),
        })

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('failed-retry', {})

      const now = Date.now()
      await storage.saveStepResult({
        id: randomUUID(),
        runId: run.id,
        name: 'a',
        status: 'failed',
        output: null,
        error: 'prior failure',
        attempts: 1,
        createdAt: now,
        updatedAt: now,
      })

      await engine.tick()

      expect(branchACalls).toBe(1)

      const info = expectPresent(await engine.getRunStatus(run.id))
      expect(info.run.status).toBe('completed')

      const completedA = info.steps.find((s) => s.name === 'a' && s.status === 'completed')
      expect(completedA?.output).toEqual({ fromA: 'recovered' })
    })

    it('does not fire onStepStart or onStepComplete for already-completed branches on resume', async () => {
      const onStepStart = vi.fn()
      const onStepComplete = vi.fn()

      const wf = createWorkflow({ name: 'resume-hooks', input: z.object({}) })
        .parallel({
          a: async () => ({ fromA: 'fresh' }),
          b: async () => ({ fromB: 'fresh' }),
        })

      const engine = createEngine({
        storage,
        workflows: [wf],
        hooks: { onStepStart, onStepComplete },
      })
      const run = await engine.enqueue('resume-hooks', {})

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

      await engine.tick()

      // Only the un-completed branch should fire start/complete
      expect(onStepStart).toHaveBeenCalledTimes(1)
      expect(onStepStart).toHaveBeenCalledWith(expect.objectContaining({ stepName: 'b' }))
      expect(onStepComplete).toHaveBeenCalledTimes(1)
      expect(onStepComplete).toHaveBeenCalledWith(
        expect.objectContaining({ stepName: 'b', output: { fromB: 'fresh' } }),
      )
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

  describe('single branch', () => {
    it('executes a single-branch parallel group with correct prev shape', async () => {
      const wf = createWorkflow({
        name: 'test',
        input: z.object({}),
      })
        .parallel({
          only: async () => ({ value: 42 }),
        })
        .step('after', async ({ prev }) => ({ received: prev.only.value }))

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('test', {})
      await engine.tick()

      const info = expectPresent(await engine.getRunStatus(run.id))
      expect(info.run.status).toBe('completed')

      const afterStep = info.steps.find((s) => s.name === 'after')
      expect(afterStep?.output).toEqual({ received: 42 })
    })
  })

  describe('cancellation', () => {
    it('cancels a run during parallel execution', async () => {
      const aborted: string[] = []
      const wf = createWorkflow({
        name: 'test',
        input: z.object({}),
      }).parallel({
        slow: async ({ signal }) => {
          await new Promise((resolve, reject) => {
            const timer = setTimeout(resolve, 5000)
            signal.addEventListener('abort', () => {
              clearTimeout(timer)
              aborted.push('slow')
              reject(signal.reason)
            })
          })
          return { x: 1 }
        },
        slower: async ({ signal }) => {
          await new Promise((resolve, reject) => {
            const timer = setTimeout(resolve, 5000)
            signal.addEventListener('abort', () => {
              clearTimeout(timer)
              aborted.push('slower')
              reject(signal.reason)
            })
          })
          return { y: 2 }
        },
      })

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('test', {})

      // Start the tick (non-blocking) then cancel immediately
      const tickPromise = engine.tick()
      // Small delay to let branches start
      await new Promise((r) => setTimeout(r, 20))
      await engine.cancel(run.id)
      await tickPromise

      const info = expectPresent(await engine.getRunStatus(run.id))
      expect(info.run.status).toBe('cancelled')
      expect(aborted.sort()).toEqual(['slow', 'slower'])
    })
  })

  describe('lease loss during parallel persistence', () => {
    it('returns a failure when the lease is lost while persisting branch results', async () => {
      // After all branches complete, the engine persists each result via saveStepResult.
      // If the lease is invalidated mid-loop (e.g. heartbeat lost during execution),
      // saveStepResult returns false and the engine surfaces a failure rather than
      // silently corrupting state.
      const delegate = new MemoryStorage()
      await delegate.initialize()

      let saveCalls = 0
      const flakyStorage: StorageAdapter = {
        initialize: () => delegate.initialize(),
        createRun: (run) => delegate.createRun(run),
        claimNextRun: (workflowNames, staleBefore) =>
          delegate.claimNextRun(workflowNames, staleBefore),
        heartbeatRun: (runId, leaseId) => delegate.heartbeatRun(runId, leaseId),
        getRun: (runId) => delegate.getRun(runId),
        getStepResults: (runId) => delegate.getStepResults(runId),
        saveStepResult: async (result, leaseId) => {
          saveCalls++
          // Drop the first parallel-branch save to simulate lease loss mid-persistence.
          if (saveCalls === 1) return false
          return delegate.saveStepResult(result, leaseId)
        },
        updateRunStatus: (runId, status) => delegate.updateRunStatus(runId, status),
        updateClaimedRunStatus: (runId, leaseId, status) =>
          delegate.updateClaimedRunStatus(runId, leaseId, status),
        close: () => delegate.close(),
      }

      const wf = createWorkflow({ name: 'lease-loss', input: z.object({}) })
        .parallel({
          a: async () => ({ ok: true }),
          b: async () => ({ ok: true }),
        })

      const engine = createEngine({ storage: flakyStorage, workflows: [wf] })
      const run = await engine.enqueue('lease-loss', {})
      await engine.tick()

      const info = expectPresent(await engine.getRunStatus(run.id))
      // Either failed (status update succeeded with the same lease) or running (also
      // failed to update). Both are valid responses to lease loss; neither is 'completed'.
      expect(info.run.status).not.toBe('completed')
    })
  })

  describe('lease loss mid-branch', () => {
    it('treats RunControlError from a branch as a skipped-cancelled outcome', async () => {
      // When the heartbeat fails mid-branch, the active-run signal is aborted with a
      // LeaseExpiredError. Branches' executeStep rethrows it (RunControlError) without
      // entering the BranchFailedError path, so causeBranch is never set. The
      // post-allSettled fallback must recognise the RunControlError and bail.
      const delegate = new MemoryStorage()
      await delegate.initialize()

      let heartbeatCalls = 0
      const flakyStorage: StorageAdapter = {
        initialize: () => delegate.initialize(),
        createRun: (run) => delegate.createRun(run),
        claimNextRun: (workflowNames, staleBefore) =>
          delegate.claimNextRun(workflowNames, staleBefore),
        heartbeatRun: async (runId, leaseId) => {
          heartbeatCalls++
          if (heartbeatCalls === 1) throw new Error('heartbeat exploded')
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

      const wf = createWorkflow({ name: 'heartbeat-parallel', input: z.object({}) })
        .parallel({
          slow: async ({ signal }) => {
            await new Promise<void>((resolve, reject) => {
              const timer = setTimeout(resolve, 1000)
              signal.addEventListener('abort', () => {
                clearTimeout(timer)
                reject(signal.reason)
              }, { once: true })
            })
            return {}
          },
        })

      const engine = createEngine({
        storage: flakyStorage,
        workflows: [wf],
        runLeaseDurationMs: 30,
        heartbeatIntervalMs: 5,
      })

      const run = await engine.enqueue('heartbeat-parallel', {})
      await engine.tick()

      const info = expectPresent(await engine.getRunStatus(run.id))
      // The run is not marked completed — lease-loss invalidates the success path.
      expect(info.run.status).not.toBe('completed')
    })
  })

  describe('group entered while already cancelled', () => {
    it('skips parallel group when the run was cancelled before the group started', async () => {
      let parallelEntered = false

      const wf = createWorkflow({ name: 'cancelled-before-group', input: z.object({}) })
        .step('setup', async () => ({ ok: true }))
        .parallel({
          a: async () => {
            parallelEntered = true
            return { ok: true }
          },
        })

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('cancelled-before-group', {})

      // Cancel before tick() — the run is claimed, setup runs, then cancellation
      // is detected on the parallel-group boundary check.
      const tickPromise = engine.tick()
      // Cancel ASAP; the cancellation may land before or during setup.
      await engine.cancel(run.id)
      await tickPromise

      const info = expectPresent(await engine.getRunStatus(run.id))
      expect(info.run.status).toBe('cancelled')
      expect(parallelEntered).toBe(false)
    })
  })

  describe('sequential parallel groups', () => {
    it('chains prev between consecutive parallel groups', async () => {
      const wf = createWorkflow({
        name: 'test',
        input: z.object({}),
      })
        .parallel({
          a: async () => ({ x: 10 }),
          b: async () => ({ y: 20 }),
        })
        .parallel({
          c: async ({ prev }) => ({ sum: prev.a.x + prev.b.y }),
        })
        .step('final', async ({ prev, steps }) => ({
          cResult: prev.c.sum,
          allSteps: {
            a: steps.a.x,
            b: steps.b.y,
            c: steps.c.sum,
          },
        }))

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('test', {})
      await engine.tick()

      const info = expectPresent(await engine.getRunStatus(run.id))
      expect(info.run.status).toBe('completed')

      const finalStep = info.steps.find((s) => s.name === 'final')
      expect(finalStep?.output).toEqual({
        cResult: 30,
        allSteps: { a: 10, b: 20, c: 30 },
      })
    })
  })
})
