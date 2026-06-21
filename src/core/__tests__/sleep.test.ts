import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { z } from 'zod'
import { createWorkflow, createEngine } from '../../index'
import { DuplicateStepError } from '../errors'
import { MemoryStorage } from '../../storage/memory'

describe('durable sleep', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('suspends the run at the sleep and resumes once the time elapses', async () => {
    const ran: string[] = []

    const wf = createWorkflow({ name: 'sleeper', input: z.object({}) })
      .step('a', async () => {
        ran.push('a')
        return { from: 'a' }
      })
      .sleep('cooldown', '1h')
      .step('b', async ({ prev }) => {
        ran.push('b')
        return { prev }
      })

    const storage = new MemoryStorage()
    const engine = createEngine({ storage, workflows: [wf] })
    const run = await engine.enqueue('sleeper', {})

    // First tick runs `a`, hits the sleep, and suspends.
    await engine.tick()
    expect(ran).toEqual(['a'])
    let info = await engine.getRunStatus(run.id)
    expect(info?.run.status).toBe('sleeping')
    expect(info?.steps.find((s) => s.name === 'cooldown')?.status).toBe('sleeping')

    // Before the wake time, the run is not claimable.
    await engine.tick()
    expect(ran).toEqual(['a'])
    expect((await engine.getRunStatus(run.id))?.run.status).toBe('sleeping')

    // Advance past the wake time; the next tick resumes and completes.
    vi.setSystemTime(Date.now() + 3_600_001)
    await engine.tick()

    expect(ran).toEqual(['a', 'b'])
    info = await engine.getRunStatus(run.id)
    expect(info?.run.status).toBe('completed')
    expect(info?.steps.find((s) => s.name === 'cooldown')?.status).toBe('completed')
    // `prev` passed through the sleep unchanged.
    expect(info?.steps.find((s) => s.name === 'b')?.output).toEqual({ prev: { from: 'a' } })
  })

  it('completes a zero-length sleep in a single tick without suspending', async () => {
    const ran: string[] = []

    const wf = createWorkflow({ name: 'instant', input: z.object({}) })
      .step('a', async () => {
        ran.push('a')
        return undefined
      })
      .sleep('noop', 0)
      .step('b', async () => {
        ran.push('b')
        return undefined
      })

    const storage = new MemoryStorage()
    const engine = createEngine({ storage, workflows: [wf] })
    const run = await engine.enqueue('instant', {})

    await engine.tick()

    expect(ran).toEqual(['a', 'b'])
    expect((await engine.getRunStatus(run.id))?.run.status).toBe('completed')
  })

  it('resumes a sleeping run on a different engine instance (crash recovery)', async () => {
    const ran: string[] = []
    const makeWf = () =>
      createWorkflow({ name: 'durable', input: z.object({}) })
        .step('a', async () => {
          ran.push('a')
          return { ok: true }
        })
        .sleep('wait', '2h')
        .step('b', async () => {
          ran.push('b')
          return { done: true }
        })

    const storage = new MemoryStorage()

    const engine1 = createEngine({ storage, workflows: [makeWf()] })
    const run = await engine1.enqueue('durable', {})
    await engine1.tick()
    expect((await engine1.getRunStatus(run.id))?.run.status).toBe('sleeping')

    // A fresh engine (simulated restart) reclaims and resumes once due.
    vi.setSystemTime(Date.now() + 7_200_001)
    const engine2 = createEngine({ storage, workflows: [makeWf()] })
    await engine2.tick()

    expect(ran).toEqual(['a', 'b'])
    expect((await engine2.getRunStatus(run.id))?.run.status).toBe('completed')
  })

  it('does not re-run earlier steps when it resumes from a sleep', async () => {
    const aCalls = vi.fn()

    const wf = createWorkflow({ name: 'no-rerun', input: z.object({}) })
      .step('a', async () => {
        aCalls()
        return { v: 1 }
      })
      .sleep('wait', '10m')
      .step('b', async () => ({ v: 2 }))

    const storage = new MemoryStorage()
    const engine = createEngine({ storage, workflows: [wf] })
    await engine.enqueue('no-rerun', {})

    await engine.tick()
    vi.setSystemTime(Date.now() + 600_001)
    await engine.tick()

    expect(aCalls).toHaveBeenCalledTimes(1)
  })

  it('can cancel a sleeping run', async () => {
    const ran: string[] = []

    const wf = createWorkflow({ name: 'cancellable', input: z.object({}) })
      .step('a', async () => {
        ran.push('a')
        return undefined
      })
      .sleep('wait', '1h')
      .step('b', async () => {
        ran.push('b')
        return undefined
      })

    const storage = new MemoryStorage()
    const engine = createEngine({ storage, workflows: [wf] })
    const run = await engine.enqueue('cancellable', {})

    await engine.tick()
    expect((await engine.getRunStatus(run.id))?.run.status).toBe('sleeping')

    expect(await engine.cancel(run.id)).toBe(true)
    expect((await engine.getRunStatus(run.id))?.run.status).toBe('cancelled')

    // Even after the wake time, a cancelled run never resumes.
    vi.setSystemTime(Date.now() + 3_600_001)
    await engine.tick()
    expect(ran).toEqual(['a'])
    expect((await engine.getRunStatus(run.id))?.run.status).toBe('cancelled')
  })

  it('rejects a sleep whose name collides with another step', () => {
    expect(() =>
      createWorkflow({ name: 'dup', input: z.object({}) })
        .step('x', async () => undefined)
        .sleep('x', '1s'),
    ).toThrow(DuplicateStepError)
  })
})
