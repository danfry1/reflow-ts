import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { z } from 'zod'
import { createWorkflow, createEngine, ConfigError } from '../../index'
import { MemoryStorage } from '../../storage/memory'
import { at, delegatingAdapter } from '../../__tests__/helpers'

const T0 = Date.UTC(2026, 0, 1)

describe('run records', () => {
  let storage: MemoryStorage

  beforeEach(async () => {
    vi.useFakeTimers({ toFake: ['Date'] })
    vi.setSystemTime(T0)
    storage = new MemoryStorage()
    await storage.initialize()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('records the output, start, and finish of a completed run', async () => {
    const wf = createWorkflow({ name: 'ok', input: z.object({}) })
      .step('a', async () => {
        vi.setSystemTime(T0 + 250)
        return { total: 3 }
      })
    const engine = createEngine({ storage, workflows: [wf] })
    const { id } = await engine.enqueue('ok', {})

    await engine.tick()

    const info = await engine.getRunStatus(id)
    expect(info?.run).toMatchObject({
      status: 'completed',
      output: { total: 3 },
      error: null,
      startedAt: T0,
      finishedAt: T0 + 250,
    })
    expect(at(info?.steps ?? [], 0)).toMatchObject({ startedAt: T0, updatedAt: T0 + 250 })
  })

  it('records the complete() value as the output of an early-completed run', async () => {
    const wf = createWorkflow({ name: 'early', input: z.object({}) })
      .step('a', async ({ complete }) => complete({ skipped: true }))
      .step('b', async () => ({ never: true }))
    const engine = createEngine({ storage, workflows: [wf] })
    const { id } = await engine.enqueue('early', {})

    await engine.tick()

    expect((await engine.getRunStatus(id))?.run.output).toStrictEqual({ skipped: true })
  })

  it('records the failing step, message, and code of a failed run', async () => {
    const wf = createWorkflow({ name: 'bad', input: z.object({}) })
      .step('fetch', async () => {
        throw Object.assign(new Error('connect refused'), { code: 'ECONNREFUSED' })
      })
    const engine = createEngine({ storage, workflows: [wf] })
    const { id } = await engine.enqueue('bad', {})

    await engine.tick()

    const run = (await engine.getRunStatus(id))?.run
    expect(run?.status).toBe('failed')
    expect(run?.error).toStrictEqual({ stepName: 'fetch', message: 'connect refused', code: 'ECONNREFUSED' })
    expect(run?.finishedAt).toBe(T0)
    expect(run?.output).toBeUndefined()
  })

  it('records a null code for an error without one', async () => {
    const wf = createWorkflow({ name: 'plain', input: z.object({}) })
      .step('a', async () => {
        throw new Error('nope')
      })
    const engine = createEngine({ storage, workflows: [wf] })
    const { id } = await engine.enqueue('plain', {})

    await engine.tick()

    expect((await engine.getRunStatus(id))?.run.error).toStrictEqual({ stepName: 'a', message: 'nope', code: null })
  })

  it('records the Reflow error code when a step times out', async () => {
    vi.useRealTimers()
    const wf = createWorkflow({ name: 'slow', input: z.object({}) })
      .step('a', { timeoutMs: 5, handler: ({ signal }) => new Promise<void>((_, reject) => {
        signal.addEventListener('abort', () => reject(signal.reason), { once: true })
      }) })
    const engine = createEngine({ storage, workflows: [wf] })
    const { id } = await engine.enqueue('slow', {})

    await engine.tick()

    expect((await engine.getRunStatus(id))?.run.error?.code).toBe('STEP_TIMEOUT')
  })

  it('shows what a waiting run is waiting for, until it resumes', async () => {
    const wf = createWorkflow({ name: 'approval', input: z.object({}) })
      .waitForEvent('approved', { timeoutMs: 60_000 })
    const engine = createEngine({ storage, workflows: [wf] })
    const { id } = await engine.enqueue('approval', {})

    await engine.tick()
    const waiting = (await engine.getRunStatus(id))?.run
    expect([waiting?.status, waiting?.waitingFor, waiting?.wakeAt]).toStrictEqual(['waiting', 'approved', T0 + 60_000])

    await engine.sendEvent(id, 'approved', { by: 'ops' })
    await engine.tick()
    const done = (await engine.getRunStatus(id))?.run
    expect([done?.status, done?.waitingFor, done?.wakeAt]).toStrictEqual(['completed', null, null])
  })

  it('keeps the start time of a sleep that finishes on a later claim', async () => {
    const wf = createWorkflow({ name: 'nap', input: z.object({}) }).sleep('pause', '1m')
    const engine = createEngine({ storage, workflows: [wf] })
    const { id } = await engine.enqueue('nap', {})

    await engine.tick()
    vi.setSystemTime(T0 + 61_000)
    await engine.tick()

    expect(at((await engine.getRunStatus(id))?.steps ?? [], 0)).toMatchObject({
      status: 'completed',
      startedAt: T0,
      updatedAt: T0 + 61_000,
    })
  })

  it('does not cancel a run that has already finished', async () => {
    const wf = createWorkflow({ name: 'ok', input: z.object({}) }).step('a', async () => ({ ok: true }))
    const engine = createEngine({ storage, workflows: [wf] })
    const { id } = await engine.enqueue('ok', {})
    await engine.tick()

    expect(await engine.cancel(id)).toBe(false)
    expect((await engine.getRunStatus(id))?.run.status).toBe('completed')
  })

  it('counts runs by status', async () => {
    const wf = createWorkflow({ name: 'ok', input: z.object({}) }).step('a', async () => ({ ok: true }))
    const engine = createEngine({ storage, workflows: [wf] })
    await engine.enqueue('ok', {})
    await engine.enqueue('ok', {})
    await engine.tick()

    expect(await engine.countRuns()).toStrictEqual({
      pending: 1, running: 0, sleeping: 0, waiting: 0, completed: 1, failed: 0, cancelled: 0,
    })
  })
})

describe('engine.prune', () => {
  let storage: MemoryStorage

  beforeEach(async () => {
    vi.useFakeTimers({ toFake: ['Date'] })
    vi.setSystemTime(T0)
    storage = new MemoryStorage()
    await storage.initialize()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  const wf = createWorkflow({ name: 'job', input: z.object({ fail: z.boolean() }) })
    .step('a', async ({ input }) => {
      if (input.fail) throw new Error('boom')
      return { ok: true }
    })

  it('deletes finished runs older than the cutoff and keeps the rest', async () => {
    const engine = createEngine({ storage, workflows: [wf] })
    const old = await engine.enqueue('job', { fail: false })
    await engine.tick()
    vi.setSystemTime(T0 + 3 * 86_400_000)
    const recent = await engine.enqueue('job', { fail: false })
    await engine.tick()
    const pending = await engine.enqueue('job', { fail: false })

    expect(await engine.prune({ olderThan: '1d' })).toBe(1)

    expect(await engine.getRunStatus(old.id)).toBeNull()
    expect((await engine.getRunStatus(recent.id))?.run.status).toBe('completed')
    expect((await engine.getRunStatus(pending.id))?.run.status).toBe('pending')
  })

  it('deletes only the statuses asked for', async () => {
    const engine = createEngine({ storage, workflows: [wf], concurrency: 2 })
    const succeeded = await engine.enqueue('job', { fail: false })
    const failed = await engine.enqueue('job', { fail: true })
    await engine.tick()
    vi.setSystemTime(T0 + 1_000)

    expect(await engine.prune({ olderThan: 0, statuses: ['completed'] })).toBe(1)

    expect(await engine.getRunStatus(succeeded.id)).toBeNull()
    expect((await engine.getRunStatus(failed.id))?.run.status).toBe('failed')
  })

  it('deletes in batches until nothing older is left', async () => {
    const batches: number[] = []
    const counting = delegatingAdapter(storage, {
      deleteRuns: async (filter) => {
        const deleted = await storage.deleteRuns(filter)
        batches.push(deleted)
        return deleted
      },
    })
    const engine = createEngine({ storage: counting, workflows: [wf], concurrency: 50 })
    for (let index = 0; index < 1_100; index++) {
      await engine.enqueue('job', { fail: false })
    }
    for (let index = 0; index < 22; index++) {
      await engine.tick()
    }
    vi.setSystemTime(T0 + 1_000)

    expect(await engine.prune({ olderThan: 0 })).toBe(1_100)

    expect(batches).toStrictEqual([500, 500, 100])
  })

  it('refuses to delete runs that can still progress', async () => {
    const engine = createEngine({ storage, workflows: [wf] })

    await expect(
      engine.prune({ olderThan: 0, statuses: ['pending' as never] }),
    ).rejects.toBeInstanceOf(ConfigError)
  })
})
