import { describe, it, expect, vi, beforeEach } from 'vitest'
import { z } from 'zod'
import { createWorkflow } from '../workflow'
import { _createEngine as createEngine } from '../engine'
import { MemoryStorage } from '../../storage/memory'

describe('step-level caching', () => {
  let storage: MemoryStorage

  beforeEach(async () => {
    storage = new MemoryStorage()
    await storage.initialize()
  })

  it('executes the step on every run when cache and cacheKey are both absent', async () => {
    const handler = vi.fn(async () => ({ v: 1 }))
    const onStepComplete = vi.fn()

    const wf = createWorkflow({ name: 'no-cache', input: z.object({ x: z.number() }) })
      .step('compute', handler)

    const engine = createEngine({ storage, workflows: [wf], hooks: { onStepComplete } })

    await engine.enqueue('no-cache', { x: 1 })
    await engine.tick()
    await engine.enqueue('no-cache', { x: 1 })
    await engine.tick()

    expect(handler).toHaveBeenCalledTimes(2)
    expect(onStepComplete).toHaveBeenCalledTimes(2)
    expect(onStepComplete).toHaveBeenCalledWith(expect.objectContaining({ cacheHit: false }))
  })

  it('silently skips the cache when cache is set but cacheKey is absent', async () => {
    const handler = vi.fn(async () => ({ v: 1 }))

    const wf = createWorkflow({ name: 'cache-no-key', input: z.object({ x: z.number() }) })
      .step('compute', {
        cache: true,
        handler,
      })

    const engine = createEngine({ storage, workflows: [wf] })

    await engine.enqueue('cache-no-key', { x: 1 })
    await engine.tick()
    await engine.enqueue('cache-no-key', { x: 1 })
    await engine.tick()

    // No cacheKey means cache is always skipped — handler runs both times
    expect(handler).toHaveBeenCalledTimes(2)
  })

  it('on a cache miss (first run) the handler executes and result is stored with cache_key', async () => {
    const handler = vi.fn(async () => ({ result: 42 }))
    const onStepComplete = vi.fn()

    const wf = createWorkflow({ name: 'cache-miss', input: z.object({ id: z.string() }) })
      .step('compute', {
        cache: true,
        cacheKey: (input) => input.id,
        handler,
      })

    const engine = createEngine({ storage, workflows: [wf], hooks: { onStepComplete } })

    await engine.enqueue('cache-miss', { id: 'abc' })
    await engine.tick()

    expect(handler).toHaveBeenCalledOnce()
    expect(onStepComplete).toHaveBeenCalledWith(
      expect.objectContaining({ cacheHit: false, output: { result: 42 } }),
    )

    // Verify the result is retrievable via getCachedStepResult
    const cached = await storage.getCachedStepResult('compute', 'abc')
    expect(cached).not.toBeNull()
    expect(cached?.output).toEqual({ result: 42 })
    expect(cached?.attempts).toBeGreaterThan(0)
  })

  it('on a cache hit (second run, same key) the handler does not execute and cacheHit is true', async () => {
    const handler = vi.fn(async () => ({ result: 42 }))
    const onStepComplete = vi.fn()

    const wf = createWorkflow({ name: 'cache-hit', input: z.object({ id: z.string() }) })
      .step('compute', {
        cache: true,
        cacheKey: (input) => input.id,
        handler,
      })

    const engine = createEngine({ storage, workflows: [wf], hooks: { onStepComplete } })

    await engine.enqueue('cache-hit', { id: 'abc' })
    await engine.tick()

    // Second run with same key — should be a cache hit
    await engine.enqueue('cache-hit', { id: 'abc' })
    await engine.tick()

    expect(handler).toHaveBeenCalledOnce()

    const calls = onStepComplete.mock.calls
    const firstCall = calls[0][0]
    const secondCall = calls[1][0]
    expect(firstCall.cacheHit).toBe(false)
    expect(secondCall.cacheHit).toBe(true)
    expect(secondCall.attempts).toBe(0)
    expect(secondCall.output).toEqual({ result: 42 })
  })

  it('sentinel row (attempts: 0) is written into the current run on cache hit', async () => {
    const handler = vi.fn(async () => ({ v: 7 }))

    const wf = createWorkflow({ name: 'sentinel', input: z.object({ k: z.string() }) })
      .step('step', {
        cache: true,
        cacheKey: (input) => input.k,
        handler,
      })

    const engine = createEngine({ storage, workflows: [wf] })

    await engine.enqueue('sentinel', { k: 'key1' })
    await engine.tick()

    const run2 = await engine.enqueue('sentinel', { k: 'key1' })
    await engine.tick()

    const steps = await storage.getStepResults(run2.id)
    expect(steps).toHaveLength(1)
    expect(steps[0].attempts).toBe(0)
    expect(steps[0].status).toBe('completed')
    expect(steps[0].output).toEqual({ v: 7 })
  })

  it('sentinel rows do not enter the cache index (no degenerate hit chain)', async () => {
    const handler = vi.fn(async () => ({ v: 1 }))

    const wf = createWorkflow({ name: 'no-chain', input: z.object({ k: z.string() }) })
      .step('step', {
        cache: true,
        cacheKey: (input) => input.k,
        handler,
      })

    const engine = createEngine({ storage, workflows: [wf] })

    // run 1: real execution, result stored in cache
    await engine.enqueue('no-chain', { k: 'key1' })
    await engine.tick()

    // run 2: cache hit, sentinel row written
    await engine.enqueue('no-chain', { k: 'key1' })
    await engine.tick()

    // The cache index should only contain the original real result (attempts > 0)
    const cached = await storage.getCachedStepResult('step', 'key1')
    expect(cached?.attempts).toBeGreaterThan(0)
  })

  it('TTL expiry: getCachedStepResult returns null when the only entry is older than ttlMs', async () => {
    // Write a result backdated 2 hours into storage directly (bypassing the engine)
    const staleTime = Date.now() - 2 * 3_600_000
    await storage.saveStepResult(
      {
        id: 'step-stale',
        runId: 'run-stale',
        name: 'compute',
        status: 'completed',
        output: { v: 1 },
        error: null,
        attempts: 1,
        createdAt: staleTime,
        updatedAt: staleTime,
      },
      undefined,
      'cache-key',
    )

    // With a 1h TTL the 2h-old entry must not be returned
    const result = await storage.getCachedStepResult('compute', 'cache-key', 3_600_000)
    expect(result).toBeNull()
  })

  it('TTL expiry: engine re-executes when cache lookup returns nothing within TTL', async () => {
    // Seed a stale result directly so the engine sees a miss when using cache: "1h"
    const staleTime = Date.now() - 2 * 3_600_000
    const freshStorage = new MemoryStorage()
    await freshStorage.initialize()
    await freshStorage.saveStepResult(
      {
        id: 'step-stale',
        runId: 'run-old',
        name: 'step',
        status: 'completed',
        output: { v: 0 },
        error: null,
        attempts: 1,
        createdAt: staleTime,
        updatedAt: staleTime,
      },
      undefined,
      'key',
    )

    const handler = vi.fn(async () => ({ v: 1 }))
    const wf = createWorkflow({ name: 'ttl-engine', input: z.object({ k: z.string() }) })
      .step('step', { cache: '1h', cacheKey: (i) => i.k, handler })

    const engine = createEngine({ storage: freshStorage, workflows: [wf] })
    await engine.enqueue('ttl-engine', { k: 'key' })
    await engine.tick()

    // Stale cache — handler must execute
    expect(handler).toHaveBeenCalledOnce()
  })

  it('TTL not expired: cache hit returns within TTL', async () => {
    const handler = vi.fn(async () => ({ v: 99 }))

    const wf = createWorkflow({ name: 'ttl-valid', input: z.object({ k: z.string() }) })
      .step('step', {
        cache: '24h',
        cacheKey: (input) => input.k,
        handler,
      })

    const engine = createEngine({ storage, workflows: [wf] })

    await engine.enqueue('ttl-valid', { k: 'key' })
    await engine.tick()

    // Immediate second run — well within 24h TTL
    await engine.enqueue('ttl-valid', { k: 'key' })
    await engine.tick()

    expect(handler).toHaveBeenCalledOnce()
  })

  it('cacheKey projection: different inputs with same projected key yield a cache hit', async () => {
    const handler = vi.fn(async ({ input }: { input: { id: string; env: string } }) => ({ id: input.id }))

    const wf = createWorkflow({ name: 'projection', input: z.object({ id: z.string(), env: z.string() }) })
      .step('fetch', {
        cache: true,
        // env is intentionally excluded from the key
        cacheKey: (input) => input.id,
        handler,
      })

    const engine = createEngine({ storage, workflows: [wf] })

    await engine.enqueue('projection', { id: 'item-1', env: 'prod' })
    await engine.tick()

    // Different env, same id — should be a cache hit
    await engine.enqueue('projection', { id: 'item-1', env: 'staging' })
    await engine.tick()

    expect(handler).toHaveBeenCalledOnce()
  })

  it('within-run replay wins over cache: already-completed step uses replay path', async () => {
    const handler = vi.fn(async () => ({ v: 1 }))
    const onStepComplete = vi.fn()

    const wf = createWorkflow({ name: 'replay-wins', input: z.object({ k: z.string() }) })
      .step('step', {
        cache: true,
        cacheKey: (input) => input.k,
        handler,
      })

    const engine = createEngine({ storage, workflows: [wf], hooks: { onStepComplete } })

    const run = await engine.enqueue('replay-wins', { k: 'key' })
    // Pre-seed a completed step result for this run (simulates a partial run resuming)
    await storage.saveStepResult({
      id: 'preseeded',
      runId: run.id,
      name: 'step',
      status: 'completed',
      output: { v: 999 },
      error: null,
      attempts: 1,
      createdAt: Date.now(),
      updatedAt: Date.now(),
    })

    await engine.tick()

    // Handler must not run — within-run replay used the preseeded result
    expect(handler).not.toHaveBeenCalled()
    // onStepComplete is NOT fired for within-run replay (engine skips it with continue)
    expect(onStepComplete).not.toHaveBeenCalled()
  })

  it('cache is indefinite when cache: true (no TTL filter)', async () => {
    const handler = vi.fn(async () => ({ v: 1 }))

    const wf = createWorkflow({ name: 'indefinite', input: z.object({ k: z.string() }) })
      .step('step', {
        cache: true,
        cacheKey: (input) => input.k,
        handler,
      })

    const engine = createEngine({ storage, workflows: [wf] })

    await engine.enqueue('indefinite', { k: 'key' })
    await engine.tick()

    await engine.enqueue('indefinite', { k: 'key' })
    await engine.tick()

    await engine.enqueue('indefinite', { k: 'key' })
    await engine.tick()

    expect(handler).toHaveBeenCalledOnce()
  })
})
