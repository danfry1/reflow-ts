import { afterEach, describe, expect, it, vi } from 'vitest'
import { z } from 'zod'
import { createEngine } from '../engine'
import { ConfigError, StorageError } from '../errors'
import type { EngineEvent } from '../events'
import { createWorkflow, type AnyWorkflow } from '../workflow'
import { MemoryStorage } from '../../storage/memory'
import { delegatingAdapter } from '../../__tests__/helpers'
import type { PersistedValue, StorageAdapter } from '../types'

const Input = z.object({ userId: z.string(), page: z.number().optional() })

/** Enqueue and run one run of `workflow` to its end, returning what it did. */
async function runOnce(storage: StorageAdapter, workflow: AnyWorkflow, input: PersistedValue, events: EngineEvent[] = []) {
  const engine = createEngine({
    storage,
    workflows: [workflow],
    hooks: { onStepComplete: (event) => void events.push(event) },
  })
  const run = await engine.enqueue(workflow.name, input)
  await engine.tick()
  return { run: await storage.getRun(run.id), steps: await storage.getStepResults(run.id) }
}

/** A workflow whose `fetch` step records each call and is cached with `cache`. */
function counted(
  cache: number | string | { ttl: number | string; key?: (ctx: { input: z.infer<typeof Input> }) => string; version?: string | number },
  calls: string[],
) {
  return createWorkflow({ name: 'profile', input: Input })
    .step('fetch', {
      cache,
      handler: async ({ input }) => {
        calls.push(input.userId)
        return { name: `user ${input.userId}`, call: calls.length }
      },
    })
}

afterEach(() => {
  vi.restoreAllMocks()
})

describe('step caching', () => {
  it('reuses a cached result for the same input instead of running the step again', async () => {
    const storage = new MemoryStorage()
    await storage.initialize()
    const calls: string[] = []
    const events: EngineEvent[] = []
    const wf = counted('1h', calls)

    await runOnce(storage, wf, { userId: 'u1' }, events)
    const second = await runOnce(storage, wf, { userId: 'u1' }, events)

    expect(calls).toStrictEqual(['u1'])
    expect([second.run?.status, second.run?.output]).toStrictEqual(['completed', { name: 'user u1', call: 1 }])
    expect(second.steps.map((step) => [step.name, step.status, step.attempts])).toStrictEqual([['fetch', 'completed', 0]])
    expect(events.map((event) => event.type === 'stepComplete' && event.cached)).toStrictEqual([undefined, true])
  })

  it('runs the step for a different input', async () => {
    const storage = new MemoryStorage()
    const calls: string[] = []
    const wf = counted('1h', calls)

    await runOnce(storage, wf, { userId: 'u1' })
    await runOnce(storage, wf, { userId: 'u2' })

    expect(calls).toStrictEqual(['u1', 'u2'])
  })

  it('keys on the previous step’s output as well as the input by default', async () => {
    const storage = new MemoryStorage()
    const calls: number[] = []
    let version = 0
    const wf = createWorkflow({ name: 'chain', input: z.object({}) })
      .step('read', async () => ({ version: ++version }))
      .step('render', {
        cache: '1h',
        handler: async ({ prev }) => {
          calls.push(prev.version)
          return { html: `v${prev.version}` }
        },
      })

    await runOnce(storage, wf, {})
    await runOnce(storage, wf, {})

    expect(calls).toStrictEqual([1, 2])
  })

  it('keys on earlier steps’ outputs too, not only the previous one', async () => {
    const storage = new MemoryStorage()
    let n = 0
    const wf = createWorkflow({ name: 'deep', input: z.object({}) })
      .step('a', async () => ({ n: ++n }))
      .step('b', async () => ({ same: true }))
      .step('c', { cache: '1h', handler: async ({ steps }) => ({ fromA: steps.a.n }) })

    await runOnce(storage, wf, {})
    const second = await runOnce(storage, wf, {})

    expect(second.run?.output).toStrictEqual({ fromA: 2 })
  })

  it('keys a field passed as undefined the same as one left out', async () => {
    const storage = new MemoryStorage()
    const calls: string[] = []
    const wf = counted('1h', calls)

    await runOnce(storage, wf, { userId: 'u1' })
    await runOnce(storage, wf, { userId: 'u1', page: undefined })

    expect(calls).toStrictEqual(['u1'])
  })

  it('fails the run with an error naming the step when its key is not plain data', async () => {
    const storage = new MemoryStorage()
    const wf = createWorkflow({ name: 'bad-key', input: z.object({}) })
      .step('s', { cache: { ttl: '1h', key: () => ({ score: Number.NaN }) }, handler: async () => ({ ok: true }) })

    const { run } = await runOnce(storage, wf, {})

    expect([run?.status, run?.error?.message]).toStrictEqual(['failed', expect.stringContaining('Step "s" cache key')])
  })

  it('keys on what `key` returns when given', async () => {
    const storage = new MemoryStorage()
    const calls: string[] = []
    const wf = counted({ ttl: '1h', key: ({ input }) => input.userId }, calls)

    await runOnce(storage, wf, { userId: 'u1', page: 1 })
    await runOnce(storage, wf, { userId: 'u1', page: 2 })

    expect(calls).toStrictEqual(['u1'])
  })

  it('does not reuse a result cached under another version', async () => {
    const storage = new MemoryStorage()
    const calls: string[] = []

    await runOnce(storage, counted({ ttl: '1h', version: 1 }, calls), { userId: 'u1' })
    await runOnce(storage, counted({ ttl: '1h', version: 2 }, calls), { userId: 'u1' })
    await runOnce(storage, counted({ ttl: '1h', version: 2 }, calls), { userId: 'u1' })

    expect(calls).toStrictEqual(['u1', 'u1'])
  })

  it('runs the step again once the ttl has passed', async () => {
    const storage = new MemoryStorage()
    const calls: string[] = []
    const wf = counted('1m', calls)
    const start = Date.now()
    const clock = vi.spyOn(Date, 'now').mockReturnValue(start)

    await runOnce(storage, wf, { userId: 'u1' })
    clock.mockReturnValue(start + 59_000)
    await runOnce(storage, wf, { userId: 'u1' })
    clock.mockReturnValue(start + 61_000)
    await runOnce(storage, wf, { userId: 'u1' })

    expect(calls).toStrictEqual(['u1', 'u1'])
  })

  it('applies a shortened ttl to results cached under the longer one', async () => {
    const storage = new MemoryStorage()
    const calls: string[] = []
    const start = Date.now()
    const clock = vi.spyOn(Date, 'now').mockReturnValue(start)

    await runOnce(storage, counted('1h', calls), { userId: 'u1' })
    clock.mockReturnValue(start + 120_000)
    await runOnce(storage, counted('1m', calls), { userId: 'u1' })

    expect(calls).toStrictEqual(['u1', 'u1'])
  })

  it('does not cache a step that failed', async () => {
    const storage = new MemoryStorage()
    let calls = 0
    const wf = createWorkflow({ name: 'flaky', input: z.object({}) })
      .step('call', {
        cache: '1h',
        handler: async () => {
          calls++
          if (calls === 1) throw new Error('down')
          return { ok: true }
        },
      })

    const first = await runOnce(storage, wf, {})
    await runOnce(storage, wf, {})
    await runOnce(storage, wf, {})

    expect([first.run?.status, calls]).toStrictEqual(['failed', 2])
  })

  it('does not cache a step that completed the run early', async () => {
    const storage = new MemoryStorage()
    let calls = 0
    const wf = createWorkflow({ name: 'early', input: z.object({}) })
      .step('check', {
        cache: '1h',
        handler: async ({ complete }) => {
          calls++
          return complete({ done: true })
        },
      })

    await runOnce(storage, wf, {})
    await runOnce(storage, wf, {})

    expect(calls).toBe(2)
  })

  it('runs the step and reports the error when the cache cannot be read', async () => {
    const memory = new MemoryStorage()
    const errors: Error[] = []
    let calls = 0
    const storage = delegatingAdapter(memory, {
      getCachedStep: async () => {
        throw new StorageError('getCachedStep', new Error('disk on fire'))
      },
    })
    const wf = createWorkflow({ name: 'read-fails', input: z.object({}) })
      .step('call', { cache: '1h', handler: async () => ({ call: ++calls }) })
    const engine = createEngine({ storage, workflows: [wf], hooks: { onError: (error) => void errors.push(error) } })

    const run = await engine.enqueue('read-fails', {})
    await engine.tick()
    await new Promise((resolve) => setTimeout(resolve, 0))

    expect([(await memory.getRun(run.id))?.status, calls]).toStrictEqual(['completed', 1])
    expect(errors.map((error) => error.message)).toStrictEqual([new StorageError('getCachedStep', new Error('disk on fire')).message])
  })

  it('prune() deletes expired cached results and keeps live ones', async () => {
    const storage = new MemoryStorage()
    const calls: string[] = []
    const start = Date.now()
    const clock = vi.spyOn(Date, 'now').mockReturnValue(start)
    await runOnce(storage, counted('1m', calls), { userId: 'short' })
    await runOnce(storage, counted('1h', calls), { userId: 'long' })

    clock.mockReturnValue(start + 120_000)
    await createEngine({ storage, workflows: [] }).prune({ olderThan: '30d' })
    await runOnce(storage, counted('1h', calls), { userId: 'long' })

    expect(await storage.deleteExpiredCachedSteps(start + 120_000, 10)).toBe(0)
    expect(calls).toStrictEqual(['short', 'long'])
  })

  it('refuses storage that cannot cache step results', () => {
    const storage = delegatingAdapter(new MemoryStorage(), { getCachedStep: undefined })
    expect(() => createEngine({ storage, workflows: [counted('1h', [])] })).toThrow(
      new ConfigError('Workflow "profile" caches a step, but this storage adapter does not store cached steps'),
    )
  })

  it('refuses a ttl that is not above zero', () => {
    expect(() => counted(0, [])).toThrow(new ConfigError('Step "fetch" in workflow "profile" needs a cache ttl above zero'))
  })

  it('refuses caching on a parallel branch', () => {
    expect(() =>
      createWorkflow({ name: 'fan', input: z.object({}) })
        // @ts-expect-error -- caching is for sequential steps
        .parallel({ a: { cache: '1h', handler: async () => ({ ok: true }) } }),
    ).toThrow(new ConfigError('Step caching is not supported on parallel branch "a" in workflow "fan"'))
  })
})
