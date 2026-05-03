import { describe, it, expect, vi, beforeEach } from 'vitest'
import { z } from 'zod'
import { createWorkflow } from '../workflow'
import { createEngine } from '../engine'
import { MemoryStorage } from '../../storage/memory'

describe('onRunComplete output', () => {
  let storage: MemoryStorage

  beforeEach(async () => {
    storage = new MemoryStorage()
    await storage.initialize()
  })

  it('fires exactly once after a single-step workflow finishes', async () => {
    const onRunComplete = vi.fn()

    const wf = createWorkflow({ name: 'single', input: z.object({}) })
      .step('a', async () => ({ done: true }))

    const engine = createEngine({ storage, workflows: [wf], hooks: { onRunComplete } })
    await engine.enqueue('single', {})
    await engine.tick()

    expect(onRunComplete).toHaveBeenCalledOnce()
  })

  it('fires exactly once after a multi-step workflow (not once per step)', async () => {
    const onRunComplete = vi.fn()

    const wf = createWorkflow({ name: 'multi', input: z.object({}) })
      .step('a', async () => ({ a: 1 }))
      .step('b', async () => ({ b: 2 }))
      .step('c', async () => ({ c: 3 }))

    const engine = createEngine({ storage, workflows: [wf], hooks: { onRunComplete } })
    await engine.enqueue('multi', {})
    await engine.tick()

    expect(onRunComplete).toHaveBeenCalledOnce()
  })

  it('event carries the output of the last step', async () => {
    const onRunComplete = vi.fn()

    const wf = createWorkflow({ name: 'output-test', input: z.object({ x: z.number() }) })
      .step('double', async ({ input }) => ({ result: input.x * 2 }))
      .step('stringify', async ({ prev }) => ({ text: `result=${prev.result}` }))

    const engine = createEngine({ storage, workflows: [wf], hooks: { onRunComplete } })
    await engine.enqueue('output-test', { x: 5 })
    await engine.tick()

    expect(onRunComplete).toHaveBeenCalledWith(
      expect.objectContaining({ output: { text: 'result=10' } }),
    )
  })

  it('does not fire when a run fails (onRunFailed fires instead)', async () => {
    const onRunComplete = vi.fn()
    const onRunFailed = vi.fn()

    const wf = createWorkflow({ name: 'fail-test', input: z.object({}) })
      .step('broken', async () => { throw new Error('boom') })

    const engine = createEngine({ storage, workflows: [wf], hooks: { onRunComplete, onRunFailed } })
    await engine.enqueue('fail-test', {})
    await engine.tick()

    expect(onRunComplete).not.toHaveBeenCalled()
    expect(onRunFailed).toHaveBeenCalledOnce()
  })

  it('fires with output equal to last step when all steps are cache hits', async () => {
    const onRunComplete = vi.fn()
    const handler = vi.fn(async () => ({ cached: true }))

    const wf = createWorkflow({ name: 'cache-run', input: z.object({ k: z.string() }) })
      .step('fetch', {
        cache: true,
        cacheKey: (input) => input.k,
        handler,
      })

    const engine = createEngine({ storage, workflows: [wf], hooks: { onRunComplete } })

    // First run — real execution
    await engine.enqueue('cache-run', { k: 'key' })
    await engine.tick()

    onRunComplete.mockClear()

    // Second run — all steps served from cache
    await engine.enqueue('cache-run', { k: 'key' })
    await engine.tick()

    expect(handler).toHaveBeenCalledOnce()
    expect(onRunComplete).toHaveBeenCalledOnce()
    expect(onRunComplete).toHaveBeenCalledWith(
      expect.objectContaining({ output: { cached: true } }),
    )
  })

  it('fires with output from complete() on early exit', async () => {
    const onRunComplete = vi.fn()

    const wf = createWorkflow({ name: 'early-output', input: z.object({}) })
      .step('check', async ({ complete }) => complete({ reason: 'done early' }))
      .step('never', async () => ({}))

    const engine = createEngine({ storage, workflows: [wf], hooks: { onRunComplete } })
    await engine.enqueue('early-output', {})
    await engine.tick()

    expect(onRunComplete).toHaveBeenCalledWith(
      expect.objectContaining({ output: { reason: 'done early' } }),
    )
  })

  it('output is undefined for a zero-step workflow', async () => {
    const onRunComplete = vi.fn()

    const wf = createWorkflow({ name: 'empty', input: z.object({}) })

    const engine = createEngine({ storage, workflows: [wf], hooks: { onRunComplete } })
    await engine.enqueue('empty', {})
    await engine.tick()

    expect(onRunComplete).toHaveBeenCalledOnce()
    expect(onRunComplete).toHaveBeenCalledWith(
      expect.objectContaining({ output: undefined }),
    )
  })
})
