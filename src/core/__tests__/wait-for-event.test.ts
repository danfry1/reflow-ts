import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { z } from 'zod'
import { createWorkflow, createEngine, ConfigError, ValidationError, WaitTimeoutError } from '../../index'
import { DuplicateStepError } from '../errors'
import { MemoryStorage } from '../../storage/memory'

describe('waitForEvent', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('suspends until the event is delivered, then resumes with the payload as prev', async () => {
    const ran: string[] = []

    const wf = createWorkflow({ name: 'wait', input: z.object({}) })
      .step('begin', async () => {
        ran.push('begin')
        return { started: true }
      })
      .waitForEvent('approval', { schema: z.object({ approvedBy: z.string() }) })
      .step('finish', async ({ prev }) => {
        ran.push('finish')
        return { approver: prev.approvedBy }
      })

    const storage = new MemoryStorage()
    const engine = createEngine({ storage, workflows: [wf] })
    const run = await engine.enqueue('wait', {})

    // First tick runs `begin`, reaches the wait, and suspends.
    await engine.tick()
    expect(ran).toEqual(['begin'])
    let info = await engine.getRunStatus(run.id)
    expect(info?.run.status).toBe('waiting')
    expect(info?.steps.find((s) => s.name === 'approval')?.status).toBe('waiting')

    // Another tick changes nothing — there is no event yet.
    await engine.tick()
    expect((await engine.getRunStatus(run.id))?.run.status).toBe('waiting')

    // Deliver the event; the next tick resumes and completes.
    expect(await engine.sendEvent(run.id, 'approval', { approvedBy: 'alice' })).toBe(true)
    await engine.tick()

    expect(ran).toEqual(['begin', 'finish'])
    info = await engine.getRunStatus(run.id)
    expect(info?.run.status).toBe('completed')
    expect(info?.steps.find((s) => s.name === 'approval')?.output).toEqual({ approvedBy: 'alice' })
    expect(info?.steps.find((s) => s.name === 'finish')?.output).toEqual({ approver: 'alice' })
  })

  it('buffers an event delivered before the run reaches the wait', async () => {
    const wf = createWorkflow({ name: 'early', input: z.object({}) })
      .step('a', async () => ({ ok: true }))
      .waitForEvent('go')
      .step('b', async ({ prev }) => ({ payload: prev }))

    const storage = new MemoryStorage()
    const engine = createEngine({ storage, workflows: [wf] })
    const run = await engine.enqueue('early', {})

    // Deliver the event up-front, before the run has even started.
    expect(await engine.sendEvent(run.id, 'go', { value: 42 })).toBe(true)

    // A single tick should run a, consume the buffered event, and finish b.
    await engine.tick()

    const info = await engine.getRunStatus(run.id)
    expect(info?.run.status).toBe('completed')
    expect(info?.steps.find((s) => s.name === 'b')?.output).toEqual({ payload: { value: 42 } })
  })

  it('fails with WaitTimeoutError when the event does not arrive in time', async () => {
    const onFailure = vi.fn()

    const wf = createWorkflow({ name: 'timeout', input: z.object({}) })
      .step('a', async () => ({ ok: true }))
      .waitForEvent('never', { timeoutMs: 60_000 })
      .step('b', async () => ({ ok: true }))
      .onFailure(async ({ error, stepName }) => onFailure({ name: (error as Error).constructor.name, stepName }))

    const storage = new MemoryStorage()
    const engine = createEngine({ storage, workflows: [wf] })
    const run = await engine.enqueue('timeout', {})

    await engine.tick()
    expect((await engine.getRunStatus(run.id))?.run.status).toBe('waiting')

    // Advance past the timeout; the next tick wakes it and fails it.
    vi.setSystemTime(Date.now() + 60_001)
    await engine.tick()

    const info = await engine.getRunStatus(run.id)
    expect(info?.run.status).toBe('failed')
    expect(info?.steps.find((s) => s.name === 'never')?.status).toBe('failed')
    expect(onFailure).toHaveBeenCalledWith({ name: 'WaitTimeoutError', stepName: 'never' })
  })

  it('honors an event that arrives before the timeout', async () => {
    const wf = createWorkflow({ name: 'in-time', input: z.object({}) })
      .waitForEvent('go', { timeoutMs: 60_000 })
      .step('done', async ({ prev }) => ({ got: prev }))

    const storage = new MemoryStorage()
    const engine = createEngine({ storage, workflows: [wf] })
    const run = await engine.enqueue('in-time', {})

    await engine.tick()
    vi.setSystemTime(Date.now() + 30_000) // still within the window
    await engine.sendEvent(run.id, 'go', { ok: true })
    await engine.tick()

    const info = await engine.getRunStatus(run.id)
    expect(info?.run.status).toBe('completed')
    expect(info?.steps.find((s) => s.name === 'done')?.output).toEqual({ got: { ok: true } })
  })

  it('validates the payload against the schema on delivery', async () => {
    const wf = createWorkflow({ name: 'validated', input: z.object({}) })
      .waitForEvent('paid', { schema: z.object({ amount: z.number() }) })
      .step('done', async () => ({ ok: true }))

    const storage = new MemoryStorage()
    const engine = createEngine({ storage, workflows: [wf] })
    const run = await engine.enqueue('validated', {})
    await engine.tick()

    // Invalid payload is rejected at send time; the run keeps waiting.
    await expect(engine.sendEvent(run.id, 'paid', { amount: 'lots' })).rejects.toThrow(ValidationError)
    expect((await engine.getRunStatus(run.id))?.run.status).toBe('waiting')

    // Valid payload goes through.
    expect(await engine.sendEvent(run.id, 'paid', { amount: 100 })).toBe(true)
    await engine.tick()
    expect((await engine.getRunStatus(run.id))?.run.status).toBe('completed')
  })

  it('sendEvent returns false for a missing run and throws for an unknown event', async () => {
    const wf = createWorkflow({ name: 'evt', input: z.object({}) })
      .waitForEvent('known')
      .step('done', async () => ({ ok: true }))

    const storage = new MemoryStorage()
    const engine = createEngine({ storage, workflows: [wf] })
    const run = await engine.enqueue('evt', {})

    expect(await engine.sendEvent('no-such-run', 'known', {})).toBe(false)
    await expect(engine.sendEvent(run.id, 'unknown', {})).rejects.toThrow(ConfigError)
  })

  it('resumes a waiting run on a different engine instance (crash recovery)', async () => {
    const ran: string[] = []
    const makeWf = () =>
      createWorkflow({ name: 'durable-wait', input: z.object({}) })
        .step('a', async () => {
          ran.push('a')
          return { ok: true }
        })
        .waitForEvent('signal')
        .step('b', async () => {
          ran.push('b')
          return { ok: true }
        })

    const storage = new MemoryStorage()
    const engine1 = createEngine({ storage, workflows: [makeWf()] })
    const run = await engine1.enqueue('durable-wait', {})
    await engine1.tick()
    expect((await engine1.getRunStatus(run.id))?.run.status).toBe('waiting')

    // A fresh engine (simulated restart) delivers and resumes.
    const engine2 = createEngine({ storage, workflows: [makeWf()] })
    await engine2.sendEvent(run.id, 'signal', {})
    await engine2.tick()

    expect(ran).toEqual(['a', 'b'])
    expect((await engine2.getRunStatus(run.id))?.run.status).toBe('completed')
  })

  it('handles several sequential waits, each consuming its own event', async () => {
    const wf = createWorkflow({ name: 'multi', input: z.object({}) })
      .waitForEvent('first')
      .step('mid', async ({ prev }) => ({ first: prev }))
      .waitForEvent('second')
      .step('end', async ({ prev, steps }) => ({ first: steps.first, second: prev }))

    const storage = new MemoryStorage()
    const engine = createEngine({ storage, workflows: [wf] })
    const run = await engine.enqueue('multi', {})

    await engine.tick()
    await engine.sendEvent(run.id, 'first', { n: 1 })
    await engine.tick()
    expect((await engine.getRunStatus(run.id))?.run.status).toBe('waiting')

    await engine.sendEvent(run.id, 'second', { n: 2 })
    await engine.tick()

    const info = await engine.getRunStatus(run.id)
    expect(info?.run.status).toBe('completed')
    expect(info?.steps.find((s) => s.name === 'end')?.output).toEqual({
      first: { n: 1 },
      second: { n: 2 },
    })
  })

  it('can cancel a waiting run', async () => {
    const wf = createWorkflow({ name: 'cancellable-wait', input: z.object({}) })
      .waitForEvent('never')
      .step('done', async () => ({ ok: true }))

    const storage = new MemoryStorage()
    const engine = createEngine({ storage, workflows: [wf] })
    const run = await engine.enqueue('cancellable-wait', {})

    await engine.tick()
    expect((await engine.getRunStatus(run.id))?.run.status).toBe('waiting')

    expect(await engine.cancel(run.id)).toBe(true)
    expect((await engine.getRunStatus(run.id))?.run.status).toBe('cancelled')

    // Delivering afterward does not resurrect it.
    await engine.sendEvent(run.id, 'never', {})
    await engine.tick()
    expect((await engine.getRunStatus(run.id))?.run.status).toBe('cancelled')
  })

  it('rejects a waitForEvent whose name collides with another step', () => {
    expect(() =>
      createWorkflow({ name: 'dup', input: z.object({}) })
        .step('x', async () => undefined)
        .waitForEvent('x'),
    ).toThrow(DuplicateStepError)
  })
})
