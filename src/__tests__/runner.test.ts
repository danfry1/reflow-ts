import { describe, it, expect } from 'vitest'
import { z } from 'zod'
import { createWorkflow } from '../core/workflow'
import { createEngineRunner } from '../runner'
import { MemoryStorage } from '../storage/memory'

describe('createEngineRunner', () => {
  it('yields the final output of a completed run', async () => {
    const wf = createWorkflow({ name: 'double', input: z.object({ x: z.number() }) })
      .step('calc', async ({ input }) => ({ result: input.x * 2 }))

    const runner = createEngineRunner<{ result: number }>(wf, { storage: new MemoryStorage() })

    const results: { result: number }[] = []
    const consume = (async () => {
      for await (const item of runner) {
        results.push(item)
        break // consume one item then stop
      }
    })()

    await runner.enqueue({ x: 5 })
    await consume
    await runner.finish()

    expect(results).toEqual([{ result: 10 }])
  })

  it('serialises multiple runs — each enqueue waits for the previous run', async () => {
    const order: number[] = []

    const wf = createWorkflow({ name: 'seq', input: z.object({ n: z.number() }) })
      .step('record', async ({ input }) => {
        order.push(input.n)
        return { n: input.n }
      })

    const storage = new MemoryStorage()
    const runner = createEngineRunner<{ n: number }>(wf, { storage })

    const results: { n: number }[] = []
    const consume = (async () => {
      for await (const item of runner) {
        results.push(item)
        if (results.length === 3) break
      }
    })()

    await runner.enqueue({ n: 1 })
    await runner.enqueue({ n: 2 })
    await runner.enqueue({ n: 3 })

    await consume
    await runner.finish()

    expect(results.map((r) => r.n)).toEqual([1, 2, 3])
  })

  it('accepts a custom storage adapter', async () => {
    const storage = new MemoryStorage()
    const wf = createWorkflow({ name: 'custom-storage', input: z.object({}) })
      .step('a', async () => ({ ok: true }))

    const runner = createEngineRunner<{ ok: boolean }>(wf, { storage })

    const results: { ok: boolean }[] = []
    const consume = (async () => {
      for await (const item of runner) {
        results.push(item)
        break
      }
    })()

    await runner.enqueue({})
    await consume
    await runner.finish()

    expect(results).toEqual([{ ok: true }])
  })

  it('a failing run releases the backpressure permit so subsequent runs can proceed', async () => {
    let callCount = 0
    const wf = createWorkflow({ name: 'fail-then-succeed', input: z.object({ n: z.number() }) })
      .step('step', async ({ input }) => {
        callCount++
        if (input.n === 1) throw new Error('first run fails')
        return { n: input.n }
      })

    const storage = new MemoryStorage()
    const runner = createEngineRunner<{ n: number }>(wf, { storage })

    const results: { n: number }[] = []
    const consume = (async () => {
      for await (const item of runner) {
        results.push(item)
        break
      }
    })()

    // Run 1 fails — the permit must still be released so run 2 can proceed
    await runner.enqueue({ n: 1 })
    // Run 2 succeeds
    await runner.enqueue({ n: 2 })

    await consume
    await runner.finish()

    expect(results).toEqual([{ n: 2 }])
    expect(callCount).toBe(2)
  })

  it('dispose() stops the engine without waiting for in-flight runs', async () => {
    const wf = createWorkflow({ name: 'dispose-test', input: z.object({}) })
      .step('a', async () => ({ done: true }))

    const runner = createEngineRunner(wf, { storage: new MemoryStorage() })

    // Should not hang
    runner.dispose()
  })

  it('[Symbol.asyncDispose] waits for in-flight run then stops', async () => {
    const wf = createWorkflow({ name: 'async-dispose', input: z.object({ x: z.number() }) })
      .step('calc', async ({ input }) => ({ result: input.x * 3 }))

    const runner = createEngineRunner<{ result: number }>(wf, { storage: new MemoryStorage() })

    const results: { result: number }[] = []
    const consume = (async () => {
      for await (const item of runner) {
        results.push(item)
        break
      }
    })()

    await runner.enqueue({ x: 4 })
    await consume
    await runner[Symbol.asyncDispose]()

    expect(results).toEqual([{ result: 12 }])
  })
})
