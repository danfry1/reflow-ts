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
    expect(ran).toStrictEqual(['begin'])
    let info = await engine.getRunStatus(run.id)
    expect(info?.run.status).toBe('waiting')
    expect(info?.steps.find((s) => s.name === 'approval')?.status).toBe('waiting')

    // Another tick changes nothing — there is no event yet.
    await engine.tick()
    expect((await engine.getRunStatus(run.id))?.run.status).toBe('waiting')

    // Deliver the event; the next tick resumes and completes.
    expect(await engine.sendEvent(run.id, 'approval', { approvedBy: 'alice' })).toBe(true)
    await engine.tick()

    expect(ran).toStrictEqual(['begin', 'finish'])
    info = await engine.getRunStatus(run.id)
    expect(info?.run.status).toBe('completed')
    expect(info?.steps.find((s) => s.name === 'approval')?.output).toStrictEqual({ approvedBy: 'alice' })
    expect(info?.steps.find((s) => s.name === 'finish')?.output).toStrictEqual({ approver: 'alice' })
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
    expect(info?.steps.find((s) => s.name === 'b')?.output).toStrictEqual({ payload: { value: 42 } })
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
    expect(info?.steps.find((s) => s.name === 'done')?.output).toStrictEqual({ got: { ok: true } })
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

    expect(ran).toStrictEqual(['a', 'b'])
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
    expect(info?.steps.find((s) => s.name === 'end')?.output).toStrictEqual({
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

    // Delivering afterward is a no-op and reports it was not delivered.
    expect(await engine.sendEvent(run.id, 'never', {})).toBe(false)
    await engine.tick()
    expect((await engine.getRunStatus(run.id))?.run.status).toBe('cancelled')
  })

  it('does not re-validate the payload on consume (supports non-idempotent transforms)', async () => {
    // The schema transforms a string to its length. Validating the stored
    // (already-transformed) number again would fail z.string() — so the engine
    // must trust the value validated at send time.
    const wf = createWorkflow({ name: 'transform', input: z.object({}) })
      .waitForEvent('len', { schema: z.string().transform((s) => s.length) })
      .step('done', async ({ prev }) => ({ length: prev }))

    const storage = new MemoryStorage()
    const engine = createEngine({ storage, workflows: [wf] })
    const run = await engine.enqueue('transform', {})
    await engine.tick()

    expect(await engine.sendEvent(run.id, 'len', 'hello')).toBe(true)
    await engine.tick()

    const info = await engine.getRunStatus(run.id)
    expect(info?.run.status).toBe('completed')
    expect(info?.steps.find((s) => s.name === 'done')?.output).toStrictEqual({ length: 5 })
  })

  it('sendEvent returns false once the run has finished', async () => {
    const wf = createWorkflow({ name: 'done-evt', input: z.object({}) })
      .waitForEvent('x')
      .step('b', async () => ({ ok: true }))

    const storage = new MemoryStorage()
    const engine = createEngine({ storage, workflows: [wf] })
    const run = await engine.enqueue('done-evt', {})

    await engine.tick()
    await engine.sendEvent(run.id, 'x', {})
    await engine.tick()
    expect((await engine.getRunStatus(run.id))?.run.status).toBe('completed')

    // A run that has already finished cannot consume events.
    expect(await engine.sendEvent(run.id, 'x', {})).toBe(false)
  })

  it('restores the consumed event if the lease is lost before the step is saved', async () => {
    // Simulate a lost lease: the first attempt to persist the completed wait
    // step fails (as if another engine reclaimed the run). The event must be put
    // back so it is not lost.
    class FlakyStorage extends MemoryStorage {
      failNextWaitSave = true
      override async saveStepResult(result: Parameters<MemoryStorage['saveStepResult']>[0], leaseId?: string) {
        if (this.failNextWaitSave && result.name === 'e' && result.status === 'completed') {
          this.failNextWaitSave = false
          return false
        }
        return super.saveStepResult(result, leaseId)
      }
    }

    const wf = createWorkflow({ name: 'lease-loss', input: z.object({}) })
      .waitForEvent('e')
      .step('done', async () => ({ ok: true }))

    const storage = new FlakyStorage()
    const engine = createEngine({ storage, workflows: [wf] })
    const run = await engine.enqueue('lease-loss', {})

    await engine.tick() // → waiting
    await engine.sendEvent(run.id, 'e', { value: 7 })
    await engine.tick() // consumes the event, fails to save, restores the event

    // The wait did not complete...
    const info = await engine.getRunStatus(run.id)
    expect(info?.steps.find((s) => s.name === 'e')?.status).not.toBe('completed')
    // ...and the event is back in storage for the reclaiming engine to consume.
    expect(await storage.takeEvent(run.id, 'e')).toStrictEqual({ payload: { value: 7 } })
  })

  it('rejects a waitForEvent whose name collides with another step', () => {
    expect(() =>
      createWorkflow({ name: 'dup', input: z.object({}) })
        .step('x', async () => undefined)
        .waitForEvent('x'),
    ).toThrow(DuplicateStepError)
  })
})
