import { describe, it, expect, expectTypeOf, vi } from 'vitest'
import { z } from 'zod'
import { createWorkflow, createEngine, ConfigError } from '../../index'
import { MemoryStorage } from '../../storage/memory'
import { at } from '../../__tests__/helpers'

describe('conditional steps (when)', () => {
  it('skips the step and passes prev through unchanged when the condition is false', async () => {
    const upgrade = vi.fn()

    const wf = createWorkflow({
      name: 'cond',
      input: z.object({ premium: z.boolean() }),
    })
      .step('base', async () => ({ tier: 'base' }))
      .step('upgrade', {
        when: ({ input }) => input.premium,
        handler: async () => {
          upgrade()
          return { tier: 'premium' }
        },
      })
      .step('finalize', async ({ prev }) => ({ finalTier: prev.tier }))

    const storage = new MemoryStorage()
    const engine = createEngine({ storage, workflows: [wf] })

    const run = await engine.enqueue('cond', { premium: false })
    await engine.tick()

    const info = await engine.getRunStatus(run.id)
    expect(info?.run.status).toBe('completed')
    // `upgrade` never ran...
    expect(upgrade).not.toHaveBeenCalled()
    // ...and `finalize` saw `base`'s output as prev.
    expect(info?.steps.find((s) => s.name === 'finalize')?.output).toStrictEqual({ finalTier: 'base' })
    // The skip is persisted.
    expect(info?.steps.find((s) => s.name === 'upgrade')?.status).toBe('skipped')
  })

  it('runs the step when the condition is true', async () => {
    const wf = createWorkflow({
      name: 'cond-true',
      input: z.object({ premium: z.boolean() }),
    })
      .step('base', async () => ({ tier: 'base' }))
      .step('upgrade', {
        when: ({ input }) => input.premium,
        handler: async () => ({ tier: 'premium' }),
      })
      .step('finalize', async ({ prev }) => ({ finalTier: prev.tier }))

    const storage = new MemoryStorage()
    const engine = createEngine({ storage, workflows: [wf] })

    const run = await engine.enqueue('cond-true', { premium: true })
    await engine.tick()

    const info = await engine.getRunStatus(run.id)
    expect(info?.steps.find((s) => s.name === 'upgrade')?.status).toBe('completed')
    expect(info?.steps.find((s) => s.name === 'finalize')?.output).toStrictEqual({ finalTier: 'premium' })
  })

  it('supports an async condition', async () => {
    const wf = createWorkflow({
      name: 'async-cond',
      input: z.object({ flag: z.boolean() }),
    })
      .step('a', async () => ({ n: 1 }))
      .step('b', {
        when: async ({ input }) => {
          await Promise.resolve()
          return input.flag
        },
        handler: async () => ({ n: 2 }),
      })

    const storage = new MemoryStorage()
    const engine = createEngine({ storage, workflows: [wf] })

    const run = await engine.enqueue('async-cond', { flag: false })
    await engine.tick()

    expect((await engine.getRunStatus(run.id))?.steps.find((s) => s.name === 'b')?.status).toBe('skipped')
  })

  it('leaves a skipped step undefined in the steps map for later steps', async () => {
    const wf = createWorkflow({
      name: 'steps-map',
      input: z.object({}),
    })
      .step('a', async () => ({ v: 1 }))
      .step('maybe', { when: () => false, handler: async () => ({ v: 2 }) })
      .step('c', async ({ steps }) => ({ seenMaybe: steps.maybe === undefined }))

    const storage = new MemoryStorage()
    const engine = createEngine({ storage, workflows: [wf] })

    const run = await engine.enqueue('steps-map', {})
    await engine.tick()

    expect((await engine.getRunStatus(run.id))?.steps.find((s) => s.name === 'c')?.output).toStrictEqual({
      seenMaybe: true,
    })
  })

  it('emits stepSkipped to hooks and streams', async () => {
    const onStepSkipped = vi.fn()

    const wf = createWorkflow({ name: 'evt', input: z.object({}) })
      .step('a', async () => ({ ok: true }))
      .step('b', { when: () => false, handler: async () => ({ ok: true }) })

    const storage = new MemoryStorage()
    const engine = createEngine({ storage, workflows: [wf], hooks: { onStepSkipped } })

    const seen: string[] = []
    const stream = engine.stream()
    const collector = (async () => {
      for await (const event of stream) {
        if (event.type === 'stepSkipped') seen.push(event.stepName)
        if (event.type === 'runComplete') break
      }
    })()

    await engine.enqueue('evt', {})
    await engine.tick()
    await collector

    expect(onStepSkipped).toHaveBeenCalledTimes(1)
    expect(at(at(onStepSkipped.mock.calls, 0), 0)).toMatchObject({ type: 'stepSkipped', stepName: 'b', workflow: 'evt' })
    expect(seen).toStrictEqual(['b'])
  })

  it('evaluates the condition exactly once — the decision is persisted, not recomputed on replay', async () => {
    const when = vi.fn(() => false)

    const wf = createWorkflow({ name: 'once', input: z.object({}) })
      .step('a', async () => ({ ok: true }))
      .step('b', { when, handler: async () => ({ ok: true }) })

    const storage = new MemoryStorage()
    const engine = createEngine({ storage, workflows: [wf] })

    const run = await engine.enqueue('once', {})
    await engine.tick()
    // A second tick finds no pending work; the completed run is never re-evaluated.
    await engine.tick()

    expect(when).toHaveBeenCalledTimes(1)
    expect((await engine.getRunStatus(run.id))?.run.status).toBe('completed')
  })

  it('fails the run at the step when the condition throws', async () => {
    const onFailure = vi.fn()

    const wf = createWorkflow({ name: 'throws', input: z.object({}) })
      .step('a', async () => ({ ok: true }))
      .step('b', {
        when: () => {
          throw new Error('flag service down')
        },
        handler: async () => ({ ok: true }),
      })
      .onFailure(async ({ error, stepName }) => onFailure({ message: error.message, stepName }))

    const storage = new MemoryStorage()
    const engine = createEngine({ storage, workflows: [wf] })

    const run = await engine.enqueue('throws', {})
    await engine.tick()

    expect((await engine.getRunStatus(run.id))?.run.status).toBe('failed')
    expect(onFailure).toHaveBeenCalledWith({ message: 'flag service down', stepName: 'b' })
  })

  it('rejects `when` on a parallel branch', () => {
    expect(() =>
      createWorkflow({ name: 'cond-parallel', input: z.object({}) }).parallel({
        a: { when: () => false, handler: async () => ({ ok: true }) },
        b: async () => ({ ok: true }),
      }),
    ).toThrow(ConfigError)
  })

  it('does not emit stepStart for a skipped step', async () => {
    const wf = createWorkflow({ name: 'no-start', input: z.object({}) })
      .step('a', async () => ({ ok: true }))
      .step('b', { when: () => false, handler: async () => ({ ok: true }) })

    const storage = new MemoryStorage()
    const engine = createEngine({ storage, workflows: [wf] })

    const starts: string[] = []
    const stream = engine.stream()
    const collector = (async () => {
      for await (const event of stream) {
        if (event.type === 'stepStart') starts.push(event.stepName)
        if (event.type === 'runComplete') break
      }
    })()

    await engine.enqueue('no-start', {})
    await engine.tick()
    await collector

    // 'b' was skipped — only 'a' started.
    expect(starts).toStrictEqual(['a'])
  })
})

describe('conditional step types', () => {
  const wf = createWorkflow({ name: 'typed', input: z.object({ premium: z.boolean() }) })
    .step('base', async () => ({ tier: 'base' as const }))
    .step('upgrade', {
      when: ({ input }) => input.premium,
      handler: async () => ({ tier: 'premium' as const, credits: 10 }),
    })

  it('widens prev to include the value that passes through on a skip', () => {
    wf.step('next', async ({ prev }) => {
      // A skipped `upgrade` leaves `base`'s output as `prev`, so the union is
      // what actually arrives at runtime.
      expectTypeOf(prev).toEqualTypeOf<
        { tier: 'premium'; credits: number } | { tier: 'base' }
      >()
      return null
    })
  })

  it('marks the conditional step optional in the steps map', () => {
    wf.step('next', async ({ steps }) => {
      expectTypeOf(steps.upgrade).toEqualTypeOf<{ tier: 'premium'; credits: number } | undefined>()
      // An unconditional step stays required.
      expectTypeOf(steps.base).toEqualTypeOf<{ tier: 'base' }>()
      return null
    })
  })

  it('rejects reading through a conditional step result without a guard', () => {
    wf.step('next', async ({ steps }) => {
      // @ts-expect-error `steps.upgrade` may be undefined when the step was skipped.
      const credits: number = steps.upgrade.credits
      return { credits }
    })
  })
})
