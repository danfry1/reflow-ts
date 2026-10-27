import { afterEach, beforeEach, describe, expect, expectTypeOf, it, vi } from 'vitest'
import { z } from 'zod'
import {
  ConfigError,
  createEngine,
  createWorkflow,
  DuplicateStepError,
  StepFailedError,
  ValidationError,
  RunCancelledError,
  type ChainWorkflow,
  type InferOutput,
  type StorageAdapter,
  type WaitForEventOptions,
  type WorkflowSteps,
} from '../../index'
import { MemoryStorage } from '../../storage/memory'
import { RunSuspendedError } from '../errors'
import { SQLiteStorage } from '../../storage/sqlite-node'
import { testEngine } from '../../test'
import { delegatingAdapter } from '../../__tests__/helpers'

const HOUR = 3_600_000

/** A started engine that never polls on its own, so only the test moves runs forward. */
const NO_POLL = HOUR

describe('workflows written as a function', () => {
  let storage: MemoryStorage

  beforeEach(async () => {
    storage = new MemoryStorage()
    await storage.initialize()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  async function stepsOf(runId: string) {
    return (await storage.getStepResults(runId)).map(({ name, status, output }) => ({ name, status, output }))
  }

  it('runs each step in order and completes with what the function returns', async () => {
    const wf = createWorkflow({
      name: 'order',
      input: z.object({ amount: z.number() }),
    }, async ({ input, step }) => {
      const charge = await step.run('charge', async () => ({ chargeId: `ch_${input.amount}` }))
      const shipment = await step.run('ship', async () => ({ tracking: `${charge.chargeId}-T` }))
      return { tracking: shipment.tracking }
    })

    const engine = createEngine({ storage, workflows: [wf] })
    const run = await engine.enqueue('order', { amount: 50 })
    await engine.tick()

    const info = await engine.getRunStatus(run.id)
    expect(info?.run.status).toBe('completed')
    expect(info?.run.output).toStrictEqual({ tracking: 'ch_50-T' })
    expect(await stepsOf(run.id)).toStrictEqual([
      { name: 'charge', status: 'completed', output: { chargeId: 'ch_50' } },
      { name: 'ship', status: 'completed', output: { tracking: 'ch_50-T' } },
    ])
  })

  it('emits the same lifecycle events as chained steps', async () => {
    const events: string[] = []
    const wf = createWorkflow({ name: 'events', input: z.object({}) }, async ({ step }) => {
      await step.run('a', async () => 1)
    })
    const engine = createEngine({
      storage,
      workflows: [wf],
      hooks: {
        onRunStart: (event) => { events.push(event.type) },
        onStepStart: (event) => { events.push(event.type) },
        onStepComplete: (event) => { events.push(event.type) },
        onRunComplete: (event) => { events.push(event.type) },
      },
    })
    await engine.enqueue('events', {})
    await engine.tick()

    expect(events).toStrictEqual(['runStart', 'stepStart', 'stepComplete', 'runComplete'])
  })

  it('passes each step its run id, attempt, and an idempotency key fixed for the step', async () => {
    const seen: unknown[] = []
    const wf = createWorkflow({ name: 'ctx', input: z.object({}) }, async ({ step, runId }) => {
      await step.run('pay', {
        retry: { maxAttempts: 2, backoff: 'linear', initialDelay: 0 },
        handler: async ({ attempt, idempotencyKey, runId: stepRunId, signal }) => {
          seen.push({ attempt, idempotencyKey, same: stepRunId === runId, aborted: signal.aborted })
          if (attempt === 1) throw new Error('flaky')
        },
      })
    })
    const engine = createEngine({ storage, workflows: [wf] })
    const run = await engine.enqueue('ctx', {})
    await engine.tick()

    expect(seen).toStrictEqual([
      { attempt: 1, idempotencyKey: `${run.id}:pay`, same: true, aborted: false },
      { attempt: 2, idempotencyKey: `${run.id}:pay`, same: true, aborted: false },
    ])
  })

  describe('replay', () => {
    it('resumes after a sleep without running finished steps again', async () => {
      vi.useFakeTimers({ toFake: ['Date'] })
      const ran: string[] = []
      const wf = createWorkflow({ name: 'nap', input: z.object({}) }, async ({ step }) => {
        ran.push('top')
        const a = await step.run('a', async () => {
          ran.push('a')
          return { n: 1 }
        })
        await step.sleep('cooldown', '1h')
        return await step.run('b', async () => {
          ran.push('b')
          return { n: a.n + 1 }
        })
      })

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('nap', {})
      await engine.tick()

      expect(ran).toStrictEqual(['top', 'a'])
      const sleeping = await engine.getRunStatus(run.id)
      expect(sleeping?.run.status).toBe('sleeping')
      expect(sleeping?.run.wakeAt).toBe(Date.now() + HOUR)

      // A different engine picks the run up, as after a deploy or crash.
      vi.setSystemTime(Date.now() + HOUR)
      const next = createEngine({ storage, workflows: [wf] })
      await next.tick()

      expect(ran).toStrictEqual(['top', 'a', 'top', 'b'])
      const done = await next.getRunStatus(run.id)
      expect(done?.run.status).toBe('completed')
      expect(done?.run.output).toStrictEqual({ n: 2 })
    })

    it('keeps the original wake time when a sleeping run is resumed early', async () => {
      vi.useFakeTimers({ toFake: ['Date'] })
      const wf = createWorkflow({ name: 'early', input: z.object({}) }, async ({ step }) => {
        await step.sleep('wait', '1h')
      })
      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('early', {})
      await engine.tick()
      const wakeAt = (await storage.getRun(run.id))?.wakeAt

      // Reclaimed before it is due: the deadline does not move.
      vi.setSystemTime(Date.now() + HOUR / 2)
      await storage.requeueRun(run.id)
      expect(await storage.getRun(run.id)).toMatchObject({ status: 'sleeping' })
      expect((await storage.getRun(run.id))?.wakeAt).toBe(wakeAt)
    })

    it('sleeps until a fixed time with sleepUntil', async () => {
      vi.useFakeTimers({ toFake: ['Date'] })
      const at = Date.now() + 2 * HOUR
      const wf = createWorkflow({ name: 'until', input: z.object({}) }, async ({ step }) => {
        await step.sleepUntil('launch', new Date(at))
      })
      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('until', {})
      await engine.tick()
      expect(await storage.getRun(run.id)).toMatchObject({ status: 'sleeping', wakeAt: at })

      vi.setSystemTime(at)
      await engine.tick()
      expect((await storage.getRun(run.id))?.status).toBe('completed')
    })

    it('replays a failure the function caught, so it takes the same branch again', async () => {
      vi.useFakeTimers({ toFake: ['Date'] })
      let charges = 0
      const path: string[] = []
      const wf = createWorkflow({ name: 'saga', input: z.object({}) }, async ({ step }) => {
        try {
          await step.run('charge', async () => {
            charges++
            if (charges === 1) throw new Error('card declined')
          })
          path.push('charged')
        } catch (error) {
          if (!(error instanceof StepFailedError)) throw error
          path.push(`declined: ${error.message}`)
          await step.run('notify', async () => {})
        }
        await step.sleep('later', '1h')
      })

      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('saga', {})
      await engine.tick()
      vi.setSystemTime(Date.now() + HOUR)
      await engine.tick()

      // The charge would succeed now, but it is not retried on replay.
      expect(charges).toBe(1)
      expect(path).toStrictEqual([
        'declined: Step "charge" failed after 1 attempt(s): card declined',
        'declined: Step "charge" failed after 1 attempt(s): card declined',
      ])
      expect((await storage.getRun(run.id))?.status).toBe('completed')
    })

    it('runs a step that a new branch reaches, rather than handing it another step\'s result', async () => {
      vi.useFakeTimers({ toFake: ['Date'] })
      let express = false
      const wf = createWorkflow({ name: 'branch', input: z.object({}) }, async ({ step }) => {
        await step.sleep('hold', '1h')
        return express
          ? await step.run('courier', async () => 'courier')
          : await step.run('post', async () => 'post')
      })
      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('branch', {})
      await engine.tick()
      express = true
      vi.setSystemTime(Date.now() + HOUR)
      await engine.tick()

      expect((await storage.getRun(run.id))?.output).toBe('courier')
    })

    it('refuses a finished operation\'s name reused for another kind after a deploy', async () => {
      vi.useFakeTimers({ toFake: ['Date'] })
      let deployed = false
      const ran: string[] = []
      const wf = createWorkflow({ name: 'redeployed', input: z.object({}), events: { go: z.object({}) } }, async ({ step }) => {
        if (deployed) {
          await step.run('pause', async () => { ran.push('pause as a step') })
        } else {
          await step.sleep('pause', 10)
        }
        await step.waitForEvent('go')
      })
      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('redeployed', {})
      await engine.tick()
      vi.setSystemTime(Date.now() + 1_000)
      await engine.tick()
      expect((await storage.getRun(run.id))?.status).toBe('waiting')

      deployed = true
      await engine.sendEvent(run.id, 'go', {})
      await engine.tick()

      expect(ran).toStrictEqual([])
      expect((await storage.getRun(run.id))?.error).toMatchObject({ stepName: 'pause', code: 'CONFIG' })
    })

    it('refuses a name reused for a different kind of operation on replay', async () => {
      vi.useFakeTimers({ toFake: ['Date'] })
      let asStep = false
      const wf = createWorkflow({ name: 'renamed', input: z.object({}) }, async ({ step }) => {
        if (asStep) {
          await step.run('pause', async () => 'oops')
        } else {
          await step.sleep('pause', '1h')
        }
      })
      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('renamed', {})
      await engine.tick()
      asStep = true
      vi.setSystemTime(Date.now() + HOUR)
      await engine.tick()

      const failed = await storage.getRun(run.id)
      expect(failed?.status).toBe('failed')
      expect(failed?.error).toMatchObject({ stepName: 'pause', code: 'CONFIG' })
    })
  })

  describe('failures', () => {
    it('fails the run at the step that failed, reporting what its handler threw', async () => {
      const failures: unknown[] = []
      const wf = createWorkflow({
        name: 'fail',
        input: z.object({ id: z.string() }),
        onFailure: async ({ error, stepName, input }) => {
          failures.push({ message: error.message, stepName, input })
        },
      }, async ({ step }) => {
        await step.run('ok', async () => 1)
        await step.run('boom', async () => {
          throw new Error('upstream 500')
        })
      })
      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('fail', { id: 'x' })
      await engine.tick()

      expect((await storage.getRun(run.id))?.error).toStrictEqual({ stepName: 'boom', message: 'upstream 500', code: null })
      expect(failures).toStrictEqual([{ message: 'upstream 500', stepName: 'boom', input: { id: 'x' } }])
    })

    it('rejects step.run with a StepFailedError carrying the handler error as its cause', async () => {
      const cause = new Error('nope')
      let caught: unknown
      const wf = createWorkflow({ name: 'cause', input: z.object({}) }, async ({ step }) => {
        try {
          await step.run('x', { retry: { maxAttempts: 2, backoff: 'linear', initialDelay: 0 }, handler: async () => { throw cause } })
        } catch (error) {
          caught = error
        }
      })
      const engine = createEngine({ storage, workflows: [wf] })
      await engine.enqueue('cause', {})
      await engine.tick()

      expect(caught).toBeInstanceOf(StepFailedError)
      expect(caught).toMatchObject({ code: 'STEP_FAILED', stepName: 'x', attempts: 2, cause })
    })

    it('names no step when the function itself throws', async () => {
      const wf = createWorkflow({ name: 'bug', input: z.object({}) }, async ({ step }) => {
        await step.run('a', async () => 1)
        throw new Error('bug between steps')
      })
      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('bug', {})
      await engine.tick()

      expect((await storage.getRun(run.id))?.error).toStrictEqual({ stepName: '', message: 'bug between steps', code: null })
    })

    it('fails a run whose output cannot be saved', async () => {
      const wf = createWorkflow({ name: 'bad-output', input: z.object({}) }, async () => {
        return { lookup: new Map() } as never
      })
      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('bad-output', {})
      await engine.tick()

      expect((await storage.getRun(run.id))?.error).toMatchObject({ stepName: '', code: 'SERIALIZATION' })
    })

    it('fails the run when two operations share a name', async () => {
      const wf = createWorkflow({ name: 'dupe', input: z.object({}) }, async ({ step }) => {
        await step.run('same', async () => 1)
        await step.run('same', async () => 2)
      })
      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('dupe', {})
      await engine.tick()

      const failed = await storage.getRun(run.id)
      expect(failed?.error).toMatchObject({ stepName: 'same', code: 'DUPLICATE_STEP' })
      expect(new DuplicateStepError('dupe', 'same').message).toBe(failed?.error?.message)
    })

    it('runs only the failed step again when a failed run is resumed', async () => {
      let attempts = 0
      const ran: string[] = []
      const wf = createWorkflow({ name: 'resume', input: z.object({}) }, async ({ step }) => {
        await step.run('first', async () => { ran.push('first') })
        await step.run('flaky', async () => {
          ran.push('flaky')
          if (++attempts === 1) throw new Error('down')
        })
      })
      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('resume', {})
      await engine.tick()
      expect((await storage.getRun(run.id))?.status).toBe('failed')

      expect(await engine.resume(run.id)).toBe(true)
      await engine.tick()

      expect(ran).toStrictEqual(['first', 'flaky', 'flaky'])
      expect((await storage.getRun(run.id))?.status).toBe('completed')
    })

    it('cuts off steps still running when the function fails, without recording them as failed', async () => {
      let sawAbort = false
      const wf = createWorkflow({ name: 'cut', input: z.object({}) }, async ({ step }) => {
        await Promise.all([
          step.run('slow', ({ signal }) => new Promise<string>((resolve) => {
            signal.addEventListener('abort', () => {
              sawAbort = true
              resolve('late')
            })
          })),
          step.run('fast', async () => {
            throw new Error('fast failed')
          }),
        ])
      })
      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('cut', {})
      await engine.tick()

      expect(sawAbort).toBe(true)
      expect((await storage.getRun(run.id))?.error).toMatchObject({ stepName: 'fast', message: 'fast failed' })
      expect((await stepsOf(run.id)).map((step) => [step.name, step.status])).toStrictEqual([['fast', 'failed']])
    })
    it('does not wait out the backoff of a step the function returned without awaiting', async () => {
      const wf = createWorkflow({ name: 'floating', input: z.object({}) }, async ({ step }) => {
        void step.run('forgotten', {
          retry: { maxAttempts: 2, backoff: 'linear', initialDelay: '1h' },
          handler: async () => {
            throw new Error('down')
          },
        })
        return 'done'
      })
      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('floating', {})
      const started = Date.now()
      await engine.tick()

      expect(Date.now() - started).toBeLessThan(1000)
      expect(await storage.getRun(run.id)).toMatchObject({ status: 'completed', output: 'done' })
    })

    it('fails at once when a sibling cut off by the failure has retries with a backoff left', async () => {
      const wf = createWorkflow({ name: 'cut-retry', input: z.object({}) }, async ({ step }) => {
        await Promise.all([
          step.run('fast', async () => {
            throw new Error('fast failed')
          }),
          step.run('slow', {
            retry: { maxAttempts: 3, backoff: 'linear', initialDelay: '1h' },
            handler: ({ signal }) => new Promise<string>((_, reject) => {
              signal.addEventListener('abort', () => reject(signal.reason))
            }),
          }),
        ])
      })
      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('cut-retry', {})
      const started = Date.now()
      await engine.tick()

      expect(Date.now() - started).toBeLessThan(1000)
      expect((await storage.getRun(run.id))?.error).toMatchObject({ stepName: 'fast', message: 'fast failed' })
      expect((await stepsOf(run.id)).map((step) => [step.name, step.status])).toStrictEqual([['fast', 'failed']])
    })
  })

  describe('concurrency', () => {
    it('runs steps started together at the same time, and fans out over dynamic lists', async () => {
      let running = 0
      let peak = 0
      const wf = createWorkflow({
        name: 'fan-out',
        input: z.object({ items: z.array(z.string()) }),
      }, async ({ input, step }) => {
        const results = await Promise.all(input.items.map((item) =>
          step.run(`print-${item}`, async () => {
            running++
            peak = Math.max(peak, running)
            await new Promise((resolve) => setTimeout(resolve, 5))
            running--
            return item.toUpperCase()
          })))
        return results
      })
      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('fan-out', { items: ['a', 'b', 'c'] })
      await engine.tick()

      expect(peak).toBe(3)
      expect((await storage.getRun(run.id))?.output).toStrictEqual(['A', 'B', 'C'])
    })

    it('does not suspend for a sleep while a sibling step is still running', async () => {
      const order: string[] = []
      let release: () => void = () => {}
      const gate = new Promise<void>((resolve) => { release = resolve })
      const wf = createWorkflow({ name: 'siblings', input: z.object({}) }, async ({ step }) => {
        await Promise.all([
          step.sleep('nap', '1h'),
          step.run('work', async () => {
            await gate
            order.push('work done')
          }),
        ])
      })
      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('siblings', {})
      const ticking = engine.tick()

      await vi.waitFor(async () => {
        expect((await stepsOf(run.id)).find((step) => step.name === 'nap')?.status).toBe('sleeping')
      })
      // The sleep is blocked but its sibling is not, so the run keeps its lease.
      await new Promise((resolve) => setImmediate(resolve))
      expect((await storage.getRun(run.id))?.status).toBe('running')

      release()
      await ticking
      expect(order).toStrictEqual(['work done'])
      expect((await storage.getRun(run.id))?.status).toBe('sleeping')
      expect((await stepsOf(run.id)).find((step) => step.name === 'work')?.status).toBe('completed')
    })

    it('wakes a short sleep in process while a sibling keeps the run busy', async () => {
      const done: string[] = []
      const wf = createWorkflow({ name: 'short', input: z.object({}) }, async ({ step }) => {
        await Promise.all([
          step.sleep('blink', 10).then(() => done.push('blink')),
          step.run('busy', () => new Promise<number>((resolve) => setTimeout(() => resolve(done.push('busy')), 60))),
        ])
      })
      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('short', {})
      await engine.tick()

      expect(done).toStrictEqual(['blink', 'busy'])
      expect((await storage.getRun(run.id))?.status).toBe('completed')
    })

    it('stays suspended even when the function swallows the rejection that unwinds it', async () => {
      vi.useFakeTimers({ toFake: ['Date'] })
      const after: string[] = []
      const wf = createWorkflow({ name: 'swallow', input: z.object({}) }, async ({ step }) => {
        try {
          await step.sleep('nap', '1h')
        } catch {
          // Swallowed: the run must stay suspended regardless.
        }
        try {
          await step.run('after', async () => { after.push('ran') })
        } catch {
          // Also refused while suspended.
        }
        return 'finished early'
      })
      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('swallow', {})
      await engine.tick()

      expect(after).toStrictEqual([])
      expect((await storage.getRun(run.id))?.status).toBe('sleeping')
    })
  })

  describe('events', () => {
    const approvals = () => createWorkflow({
      name: 'approve',
      input: z.object({}),
      events: { approved: z.object({ by: z.string() }) },
    }, async ({ step }) => {
      const approval = await step.waitForEvent('approved')
      return await step.run('publish', async () => ({ by: approval.by }))
    })

    it('waits for a declared event and resumes with its payload', async () => {
      const engine = createEngine({ storage, workflows: [approvals()] })
      const run = await engine.enqueue('approve', {})
      await engine.tick()
      expect(await storage.getRun(run.id)).toMatchObject({ status: 'waiting', waitingFor: 'approved' })

      expect(await engine.sendEvent(run.id, 'approved', { by: 'sam' })).toBe(true)
      await engine.tick()

      expect(await storage.getRun(run.id)).toMatchObject({ status: 'completed', output: { by: 'sam' } })
    })

    it('uses an event sent before the run reaches the wait', async () => {
      const engine = createEngine({ storage, workflows: [approvals()] })
      const run = await engine.enqueue('approve', {})
      await engine.sendEvent(run.id, 'approved', { by: 'early' })
      await engine.tick()

      expect(await storage.getRun(run.id)).toMatchObject({ status: 'completed', output: { by: 'early' } })
    })

    it('validates a payload against the declared schema when it is sent', async () => {
      const engine = createEngine({ storage, workflows: [approvals()] })
      const run = await engine.enqueue('approve', {})

      await expect(engine.sendEvent(run.id, 'approved', { by: 7 })).rejects.toBeInstanceOf(ValidationError)
      await expect(engine.sendEvent(run.id, 'rejected', {})).rejects.toBeInstanceOf(ConfigError)
    })

    it('resolves to null when the wait times out, so the function can chase instead of failing', async () => {
      vi.useFakeTimers({ toFake: ['Date'] })
      const wf = createWorkflow({
        name: 'chase',
        input: z.object({}),
        events: { shipped: z.object({ tracking: z.string() }) },
      }, async ({ step }) => {
        const shipped = await step.waitForEvent('shipped', { timeout: '10d' })
        if (!shipped) {
          return await step.run('chase', async () => 'chased')
        }
        return shipped.tracking
      })
      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('chase', {})
      await engine.tick()
      expect((await storage.getRun(run.id))?.wakeAt).toBe(Date.now() + 10 * 24 * HOUR)

      vi.setSystemTime(Date.now() + 10 * 24 * HOUR)
      await engine.tick()

      expect(await storage.getRun(run.id)).toMatchObject({ status: 'completed', output: 'chased' })
      expect(await stepsOf(run.id)).toStrictEqual([
        { name: 'shipped', status: 'skipped', output: null },
        { name: 'chase', status: 'completed', output: 'chased' },
      ])
    })

    it('fails the run when a wait names an event the workflow does not declare', async () => {
      const wf = createWorkflow({ name: 'undeclared', input: z.object({}) }, async ({ step }) => {
        // Only reachable from JavaScript or through a cast: the types refuse it.
        await (step.waitForEvent as (name: string) => Promise<unknown>)('ghost')
      })
      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('undeclared', {})
      await engine.tick()

      expect((await storage.getRun(run.id))?.error).toMatchObject({ stepName: 'ghost', code: 'CONFIG' })
    })

    it('waits for several events at once, on storage that parks a run on several', async () => {
      const wf = createWorkflow({
        name: 'two-waits',
        input: z.object({}),
        events: { a: z.object({ n: z.number() }), b: z.object({ n: z.number() }) },
      }, async ({ step }) => {
        const [a, b] = await Promise.all([step.waitForEvent('a'), step.waitForEvent('b')])
        return a.n + b.n
      })
      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('two-waits', {})
      await engine.tick()
      expect(await storage.getRun(run.id)).toMatchObject({ status: 'waiting', waitingFor: 'a, b' })

      await engine.sendEvent(run.id, 'b', { n: 2 })
      await engine.tick()
      expect((await storage.getRun(run.id))?.status).toBe('waiting')
      await engine.sendEvent(run.id, 'a', { n: 1 })
      await engine.tick()

      expect(await storage.getRun(run.id)).toMatchObject({ status: 'completed', output: 3 })
    })

    it('refuses a second pending wait on storage that parks a run on one event only', async () => {
      const wf = createWorkflow({
        name: 'two-waits-one-name',
        input: z.object({}),
        events: { a: z.object({}), b: z.object({}) },
      }, async ({ step }) => {
        await Promise.all([step.waitForEvent('a'), step.waitForEvent('b')])
      })
      const engine = createEngine({ storage: delegatingAdapter(storage, { supportsEventNames: undefined }), workflows: [wf] })
      const run = await engine.enqueue('two-waits-one-name', {})
      await engine.tick()

      expect((await storage.getRun(run.id))?.error).toMatchObject({ stepName: 'b', code: 'CONFIG' })
    })

    it('waits for the same event more than once, under names of its own', async () => {
      const wf = createWorkflow({
        name: 'rounds',
        input: z.object({}),
        events: { approved: z.object({ by: z.string() }) },
      }, async ({ step }) => {
        const approvers: string[] = []
        for (const round of [1, 2]) {
          const approval = await step.waitForEvent(`approval-${round}`, { event: 'approved' })
          approvers.push(approval.by)
        }
        return approvers
      })
      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('rounds', {})
      await engine.sendEvent(run.id, 'approved', { by: 'ana' })
      await engine.sendEvent(run.id, 'approved', { by: 'ben' })
      await engine.tick()

      expect(await storage.getRun(run.id)).toMatchObject({ status: 'completed', output: ['ana', 'ben'] })
      expect(await stepsOf(run.id)).toStrictEqual([
        { name: 'approval-1', status: 'completed', output: { by: 'ana' } },
        { name: 'approval-2', status: 'completed', output: { by: 'ben' } },
      ])
    })

    it('lets a later wait go ahead after a race left the losing one pending', async () => {
      vi.useFakeTimers({ toFake: ['Date'] })
      const wf = createWorkflow({
        name: 'after-race',
        input: z.object({}),
        events: { a: z.object({}), b: z.object({}) },
      }, async ({ step }) => {
        await Promise.race([step.waitForEvent('a'), step.sleep('s', 10)])
        await step.waitForEvent('b')
        return 'done'
      })
      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue('after-race', {})
      await engine.tick()
      vi.setSystemTime(Date.now() + 1_000)
      await engine.tick()
      expect((await storage.getRun(run.id))?.status).toBe('waiting')
      await engine.sendEvent(run.id, 'b', {})
      await engine.tick()

      expect(await storage.getRun(run.id)).toMatchObject({ status: 'completed', output: 'done' })
    })
  })

  describe('retries', () => {
    it('waits out a backoff durably on a started engine, then continues from the next attempt', async () => {
      vi.useFakeTimers({ toFake: ['Date'] })
      let attempts = 0
      const wf = createWorkflow({ name: 'backoff', input: z.object({}) }, async ({ step }) => {
        return await step.run('flaky', {
          retry: { maxAttempts: 3, backoff: 'linear', initialDelay: '10m' },
          handler: async () => {
            if (++attempts < 3) throw new Error('down')
            return attempts
          },
        })
      })
      const engine = createEngine({ storage, workflows: [wf] })
      await engine.start(NO_POLL)
      const run = await engine.enqueue('backoff', {})

      // Each backoff suspends the run until its retry is due: 10m, then 20m.
      const backoffs: number[] = []
      for (;;) {
        await vi.waitFor(async () => expect(['sleeping', 'completed']).toContain((await storage.getRun(run.id))?.status))
        const current = await storage.getRun(run.id)
        const retrying = (await storage.getStepResults(run.id)).find((step) => step.status === 'retrying')
        if (current?.status !== 'sleeping' || current.wakeAt === null || !retrying) break
        backoffs.push(current.wakeAt - retrying.updatedAt)
        vi.setSystemTime(current.wakeAt)
        await engine.tick()
      }

      expect(backoffs).toStrictEqual([10 * 60_000, 20 * 60_000])
      expect(attempts).toBe(3)
      expect(await storage.getRun(run.id)).toMatchObject({ status: 'completed', output: 3 })
      await engine.stop()
    })
  })

  it('reuses a cached step result across runs', async () => {
    let calls = 0
    const wf = createWorkflow({
      name: 'cached',
      input: z.object({ url: z.string() }),
    }, async ({ input, step }) => {
      return await step.run('summarize', {
        cache: '1h',
        handler: async () => {
          calls++
          return `summary of ${input.url}`
        },
      })
    })
    const engine = createEngine({ storage, workflows: [wf] })
    const first = await engine.enqueue('cached', { url: 'a' })
    await engine.tick()
    const second = await engine.enqueue('cached', { url: 'a' })
    await engine.tick()
    const other = await engine.enqueue('cached', { url: 'b' })
    await engine.tick()

    expect(calls).toBe(2)
    expect((await storage.getRun(first.id))?.output).toBe('summary of a')
    expect((await storage.getRun(second.id))?.output).toBe('summary of a')
    expect((await storage.getRun(other.id))?.output).toBe('summary of b')
  })

  it('fails a cached step on storage that does not store cached steps', async () => {
    const wf = createWorkflow({ name: 'no-cache', input: z.object({}) }, async ({ step }) => {
      await step.run('x', { cache: '1h', handler: async () => 1 })
    })
    const bare = delegatingAdapter(storage, {
      getCachedStep: undefined,
      putCachedStep: undefined,
      deleteExpiredCachedSteps: undefined,
    })
    const engine = createEngine({ storage: bare, workflows: [wf] })
    const run = await engine.enqueue('no-cache', {})
    await engine.tick()

    expect((await storage.getRun(run.id))?.error).toMatchObject({ stepName: 'x', code: 'CONFIG' })
  })

  it('stops a run cancelled mid-step without recording a failure', async () => {
    let engine: ReturnType<typeof createEngine> | undefined
    const wf = createWorkflow({ name: 'cancel', input: z.object({}) }, async ({ step, runId }) => {
      await step.run('long', async ({ signal }) => {
        await engine?.cancel(runId)
        await new Promise((resolve) => signal.addEventListener('abort', resolve))
      })
      await step.run('never', async () => 1)
    })
    engine = createEngine({ storage, workflows: [wf] })
    const run = await engine.enqueue('cancel', {})
    await engine.tick()

    expect((await storage.getRun(run.id))?.status).toBe('cancelled')
    expect((await stepsOf(run.id)).map((step) => step.name)).toStrictEqual([])
  })

  it('replays sleeps and waits that finished on earlier executions', async () => {
    vi.useFakeTimers({ toFake: ['Date'] })
    const wf = createWorkflow({
      name: 'long-haul',
      input: z.object({}),
      events: { approved: z.object({ by: z.string() }) },
    }, async ({ step }) => {
      const approval = await step.waitForEvent('approved')
      await step.sleep('first', '1h')
      await step.sleep('second', '1h')
      return approval.by
    })
    const engine = createEngine({ storage, workflows: [wf] })
    const run = await engine.enqueue('long-haul', {})
    await engine.tick()
    await engine.sendEvent(run.id, 'approved', { by: 'kim' })
    await engine.tick()
    for (let i = 0; i < 2; i++) {
      vi.setSystemTime(Date.now() + HOUR)
      await engine.tick()
    }

    expect(await storage.getRun(run.id)).toMatchObject({ status: 'completed', output: 'kim' })
    expect((await stepsOf(run.id)).map((step) => step.status)).toStrictEqual(['completed', 'completed', 'completed'])
  })

  it('waits out a backoff in process on an engine driven by tick()', async () => {
    let attempts = 0
    const wf = createWorkflow({ name: 'in-process', input: z.object({}) }, async ({ step }) => {
      return await step.run('flaky', {
        retry: { maxAttempts: 3, backoff: 'linear', initialDelay: 5 },
        handler: async () => {
          if (++attempts < 3) throw new Error('down')
          return 'up'
        },
      })
    })
    const engine = createEngine({ storage, workflows: [wf] })
    const run = await engine.enqueue('in-process', {})
    await engine.tick()

    expect(await storage.getRun(run.id)).toMatchObject({ status: 'completed', output: 'up' })
    // Saved as retrying twice, then completed: still one row for the step.
    expect((await storage.getStepResults(run.id)).map(({ name, status, attempts }) => ({ name, status, attempts })))
      .toStrictEqual([{ name: 'flaky', status: 'completed', attempts: 3 }])
  })

  it('stops a run cancelled while one operation sleeps and another runs', async () => {
    let engine: ReturnType<typeof createEngine> | undefined
    const wf = createWorkflow({ name: 'cancel-blocked', input: z.object({}) }, async ({ step, runId }) => {
      await Promise.all([
        step.sleep('nap', '1h'),
        step.run('work', async ({ signal }) => {
          await new Promise((resolve) => setTimeout(resolve, 5))
          await engine?.cancel(runId)
          await new Promise((resolve) => signal.addEventListener('abort', resolve))
        }),
      ])
    })
    engine = createEngine({ storage, workflows: [wf] })
    const run = await engine.enqueue('cancel-blocked', {})
    await engine.tick()

    expect((await storage.getRun(run.id))?.status).toBe('cancelled')
  })

  it('fails the run on unusable arguments, even when the function catches the rejection', async () => {
    const calls: Array<[string, (step: WorkflowSteps<{ ping: Record<string, never> }>) => Promise<unknown>]> = [
      ['empty-name', (step) => step.run('', async () => 1)],
      ['bad-time', (step) => step.sleepUntil('never', new Date(Number.NaN))],
      ['zero-timeout', (step) => step.waitForEvent('ping', { timeout: 0 })],
      ['bad-duration', (step) => step.sleep('nap', 'soon')],
    ]
    for (const [label, call] of calls) {
      const wf = createWorkflow({
        name: `bad-args-${label}`,
        input: z.object({}),
        events: { ping: z.object({}) },
      }, async ({ step }) => {
        await call(step).catch(() => {})
        return 'carried on'
      })
      const engine = createEngine({ storage, workflows: [wf] })
      const run = await engine.enqueue(`bad-args-${label}`, {})
      await engine.tick()

      expect([label, (await storage.getRun(run.id))?.error?.code]).toStrictEqual([label, 'CONFIG'])
    }
  })

  it('runs in testEngine', async () => {
    const wf = createWorkflow({ name: 'tested', input: z.object({ n: z.number() }) }, async ({ input, step }) => {
      return await step.run('double', async () => input.n * 2)
    })
    const result = await testEngine({ workflows: [wf] }).run('tested', { n: 21 })

    expect(result.status).toBe('completed')
    expect(result.steps['double']).toStrictEqual({ status: 'completed', output: 42, error: null })
  })
})

describe('types of workflows written as a function', () => {
  it('infers input, step outputs, and event payloads', () => {
    const wf = createWorkflow({
      name: 'typed',
      input: z.object({ orderId: z.string() }),
      events: { shipped: z.object({ tracking: z.string() }) },
    }, async ({ input, step }) => {
      expectTypeOf(input).toEqualTypeOf<{ orderId: string }>()

      const charge = await step.run('charge', async () => ({ chargeId: 'ch' }))
      expectTypeOf(charge).toEqualTypeOf<{ chargeId: string }>()

      const nothing = await step.run('log', async () => {})
      expectTypeOf(nothing).toEqualTypeOf<undefined>()

      const configured = await step.run('configured', { retry: { maxAttempts: 2 }, handler: async () => 3 })
      expectTypeOf(configured).toEqualTypeOf<number>()

      const shipped = await step.waitForEvent('shipped')
      expectTypeOf(shipped).toEqualTypeOf<{ tracking: string }>()

      const maybe = await step.waitForEvent('shipped', { timeout: '10d' })
      expectTypeOf(maybe).toEqualTypeOf<{ tracking: string } | null>()

      // @ts-expect-error -- only declared events can be waited for
      await step.waitForEvent('delivered')

      return { tracking: shipped.tracking }
    })

    const engine = createEngine({ storage: new MemoryStorage(), workflows: [wf] })
    expectTypeOf(engine.enqueue).toBeCallableWith('typed', { orderId: 'o1' })
    // @ts-expect-error -- input is checked against the schema's type
    expectTypeOf(engine.enqueue).toBeCallableWith('typed', { orderId: 1 })
  })

  it('keeps undefined in results, and widens literals so results can be reassigned', () => {
    createWorkflow({ name: 'widen', input: z.object({ flag: z.boolean() }) }, async ({ input, step }) => {
      expectTypeOf(await step.run('u', async () => undefined)).toEqualTypeOf<undefined>()
      expectTypeOf(await step.run('maybe', async () => (input.flag ? input.toString().length : undefined))).toEqualTypeOf<number | undefined>()

      let count = await step.run('count', async () => 0)
      expectTypeOf(count).toEqualTypeOf<number>()
      count = 2
      const label = await step.run('label', { retry: { maxAttempts: 2 }, handler: async () => 'x' })
      expectTypeOf(label).toEqualTypeOf<string>()
      return count
    })

    // The chained builder shares the fix: a step resolving to undefined is not `never`.
    createWorkflow({ name: 'chain-undefined', input: z.object({}) })
      .step('u', async () => undefined)
      .step('next', async ({ prev }) => {
        expectTypeOf(prev).toEqualTypeOf<undefined>()
      })
  })

  it('checks step options for misspellings', () => {
    createWorkflow({ name: 'options', input: z.object({}) }, async ({ step }) => {
      // @ts-expect-error -- `initialDelayMs` is the chained form; here it is `initialDelay`
      await step.run('a', { retry: { maxAttempts: 2, initialDelayMs: 10 }, handler: async () => 1 })
      // @ts-expect-error -- `timeoutMs` is the chained form; here it is `timeout`
      await step.run('b', { timeoutMs: 10, handler: async () => 1 })
      await step.run('c', { retry: { maxAttempts: 2, initialDelay: '1s' }, timeout: '30s', handler: async () => 1 })
    })
  })

  it('adds null to a wait only when a timeout may be set', () => {
    createWorkflow({
      name: 'maybe-timeout',
      input: z.object({ slow: z.boolean() }),
      events: { done: z.object({ ok: z.boolean() }) },
    }, async ({ input, step }) => {
      expectTypeOf(await step.waitForEvent('done', {})).toEqualTypeOf<{ ok: boolean }>()
      expectTypeOf(await step.waitForEvent('done', { timeout: input.slow ? '1d' : undefined }))
        .toEqualTypeOf<{ ok: boolean } | null>()
    })
  })

  it('types the run output, for testEngine and InferOutput', async () => {
    const wf = createWorkflow({ name: 'out', input: z.object({}) }, async ({ step }) => {
      const n = await step.run('n', async () => 2)
      return { doubled: n * 2 }
    })
    expectTypeOf<InferOutput<typeof wf>>().toEqualTypeOf<{ doubled: number }>()

    const result = await testEngine({ workflows: [wf] }).run('out', {})
    expectTypeOf(result.output).toEqualTypeOf<{ doubled: number } | undefined>()
    expect(result).toMatchObject({ status: 'completed', output: { doubled: 4 } })
  })

  it('refuses waitForEvent on a workflow that declares no events', () => {
    createWorkflow({ name: 'no-events', input: z.object({}) }, async ({ step }) => {
      // @ts-expect-error -- nothing to wait for
      await step.waitForEvent('anything')
    })
  })
})

describe('workflows written as a function on SQLite', () => {
  afterEach(() => {
    vi.useRealTimers()
  })

  it('survives a sleep, an event, and a caught failure across engines', async () => {
    vi.useFakeTimers({ toFake: ['Date'] })
    const storage = new SQLiteStorage(':memory:')
    await storage.initialize()
    let refunds = 0
    const wf = createWorkflow({
      name: 'durable',
      input: z.object({ orderId: z.string() }),
      events: { shipped: z.object({ tracking: z.string() }) },
    }, async ({ input, step }) => {
      const printed = await Promise.all(['a', 'b'].map((item) =>
        step.run(`print-${item}`, async () => `${input.orderId}-${item}`)))
      await step.sleep('settle', '1h')
      const shipped = await step.waitForEvent('shipped', { timeout: '1d' })
      try {
        await step.run('invoice', async () => {
          throw new Error('invoicing down')
        })
      } catch {
        await step.run('refund', async () => { refunds++ })
      }
      return { printed, tracking: shipped?.tracking ?? null }
    })

    const first = createEngine({ storage, workflows: [wf] })
    const run = await first.enqueue('durable', { orderId: 'o1' })
    await first.tick()
    expect((await storage.getRun(run.id))?.status).toBe('sleeping')

    vi.setSystemTime(Date.now() + HOUR)
    const second = createEngine({ storage, workflows: [wf] })
    await second.tick()
    expect((await storage.getRun(run.id))?.status).toBe('waiting')

    await second.sendEvent(run.id, 'shipped', { tracking: 'TRK1' })
    await second.tick()

    expect(await storage.getRun(run.id)).toMatchObject({
      status: 'completed',
      output: { printed: ['o1-a', 'o1-b'], tracking: 'TRK1' },
    })
    expect(refunds).toBe(1)
    expect((await storage.getStepResults(run.id)).map((step) => `${step.name}:${step.status}`)).toStrictEqual([
      'print-a:completed',
      'print-b:completed',
      'settle:completed',
      'shipped:completed',
      'invoice:failed',
      'refund:completed',
    ])
    await storage.close()
  })
})

describe('workflows written as a function, under adversarial conditions', () => {
  let storage: MemoryStorage

  beforeEach(async () => {
    storage = new MemoryStorage()
    await storage.initialize()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))
  const rowsOf = async (runId: string) =>
    (await storage.getStepResults(runId)).map((step) => `${step.name}:${step.status}`)

  it('fails the run on a storage error, even when the function catches it, rather than taking the failure branch', async () => {
    let charges = 0
    let failedOnce = false
    const flaky = delegatingAdapter(storage, {
      saveStepResult: async (row, leaseId) => {
        if (row.name === 'charge' && row.status === 'completed' && !failedOnce) {
          failedOnce = true
          throw new Error('SQLITE_BUSY')
        }
        return storage.saveStepResult(row, leaseId)
      },
    })
    const wf = createWorkflow({ name: 'pay', input: z.object({}) }, async ({ step }) => {
      try {
        await step.run('charge', async () => {
          charges++
          return 'ch_1'
        })
        return 'paid'
      } catch {
        await step.run('email-failure', async () => {})
        return 'failed-to-pay'
      }
    })
    const engine = createEngine({ storage: flaky, workflows: [wf] })
    const run = await engine.enqueue('pay', {})
    await engine.tick()

    const failed = await storage.getRun(run.id)
    expect(failed?.status).toBe('failed')
    expect(failed?.error).toMatchObject({ stepName: 'charge', code: 'STORAGE' })
    expect(charges).toBe(1)
    expect(await rowsOf(run.id)).toStrictEqual([])
  })

  it('replays Promise.race between operations with the same winner as the first execution', async () => {
    const branches: string[] = []
    const wf = createWorkflow({
      name: 'race',
      input: z.object({}),
      events: { resume: z.object({}) },
    }, async ({ step }) => {
      const winner = await Promise.race([
        step.run('slow', async () => {
          await delay(150)
          return 'done'
        }),
        step.sleep('deadline', 20).then(() => 'timeout'),
      ])
      branches.push(winner)
      await step.waitForEvent('resume')
      return winner
    })
    const engine = createEngine({ storage, workflows: [wf] })
    const run = await engine.enqueue('race', {})
    await engine.tick()
    await engine.sendEvent(run.id, 'resume', {})
    await engine.tick()

    expect(branches).toStrictEqual(['timeout', 'timeout'])
    expect(await storage.getRun(run.id)).toMatchObject({ status: 'completed', output: 'timeout' })
  })

  it('wakes a wait blocked in process as soon as its event is sent, beside a busy sibling', async () => {
    let arrivedAfter: number | null = null
    const wf = createWorkflow({
      name: 'prompt',
      input: z.object({}),
      events: { go: z.object({}) },
    }, async ({ step }) => {
      const started = Date.now()
      return await Promise.race([
        step.waitForEvent('go').then(() => {
          arrivedAfter = Date.now() - started
          return 'event'
        }),
        step.run('long', async () => {
          await delay(1_000)
          return 'long'
        }),
      ])
    })
    const engine = createEngine({ storage, workflows: [wf] })
    const run = await engine.enqueue('prompt', {})
    const ticking = engine.tick()
    await delay(50)
    await engine.sendEvent(run.id, 'go', {})
    await ticking

    expect((await storage.getRun(run.id))?.output).toBe('event')
    // Under the 1s event poll and the 1s sibling: only an in-process wake gets here.
    expect(arrivedAfter).toBeLessThan(900)
  })

  it('lets a branch await other work between steps beside a sleep without being cut off', async () => {
    const wf = createWorkflow({ name: 'io', input: z.object({}) }, async ({ step }) => {
      await Promise.all([
        step.sleep('cooldown', '1h'),
        (async () => {
          await step.run('a', async () => 1)
          await delay(5)
          await step.run('b', async () => 2)
        })(),
      ])
    })
    const engine = createEngine({ storage, workflows: [wf] })
    const run = await engine.enqueue('io', {})
    await engine.tick()

    expect((await storage.getRun(run.id))?.status).toBe('sleeping')
    expect(await rowsOf(run.id)).toStrictEqual(['cooldown:sleeping', 'a:completed', 'b:completed'])
  })

  it('fails the run when the heartbeat cannot reach storage, rather than recording later steps as failed', async () => {
    let heartbeats = 0
    const ran: string[] = []
    const blip = delegatingAdapter(storage, {
      heartbeatRun: async (runId, leaseId) => {
        if (heartbeats++ === 0) throw new Error('db blip')
        return storage.heartbeatRun(runId, leaseId)
      },
    })
    const wf = createWorkflow({ name: 'blip', input: z.object({}) }, async ({ step }) => {
      for (const name of ['a', 'b', 'c']) {
        try {
          await step.run(name, async () => {
            ran.push(name)
            if (name === 'a') await delay(150)
          })
        } catch {
          // Carry on, as a function that handles its steps' failures would.
        }
      }
      return 'finished'
    })
    const engine = createEngine({ storage: blip, workflows: [wf], heartbeatIntervalMs: 50 })
    const run = await engine.enqueue('blip', {})
    await engine.tick()

    const failed = await storage.getRun(run.id)
    expect(failed?.status).toBe('failed')
    expect(failed?.error?.stepName).toBe('a')
    expect(ran).toStrictEqual(['a'])
    expect(await rowsOf(run.id)).toStrictEqual([])
  })

  it('waits out a backoff longer than a timer can hold on an engine driven by tick()', async () => {
    vi.useFakeTimers()
    let attempts = 0
    const wf = createWorkflow({ name: 'month', input: z.object({}) }, async ({ step }) => {
      return await step.run('flaky', {
        retry: { maxAttempts: 2, initialDelay: '30d' },
        handler: async () => {
          if (++attempts === 1) throw new Error('down')
          return 'up'
        },
      })
    })
    // A long heartbeat, so advancing a month of fake time does not fire a heartbeat every few seconds.
    const engine = createEngine({ storage, workflows: [wf], runLeaseDurationMs: 30 * 24 * HOUR, heartbeatIntervalMs: 12 * HOUR })
    const run = await engine.enqueue('month', {})
    let done = false
    const ticking = engine.tick().then(() => { done = true })

    await vi.advanceTimersByTimeAsync(25 * 24 * HOUR)
    expect([done, attempts]).toStrictEqual([false, 1])
    await vi.advanceTimersByTimeAsync(5 * 24 * HOUR + 1_000)
    await ticking

    expect(await storage.getRun(run.id)).toMatchObject({ status: 'completed', output: 'up' })
  }, 15_000)

  it('does not let stop() return before a suspend under way has been written', async () => {
    let releaseSleep: () => void = () => {}
    const landed: string[] = []
    const gated = delegatingAdapter(storage, {
      sleepRun: async (runId, leaseId, wakeAt) => {
        await new Promise<void>((resolve) => { releaseSleep = resolve })
        const parked = await storage.sleepRun(runId, leaseId, wakeAt)
        landed.push('sleepRun')
        return parked
      },
    })
    const wf = createWorkflow({ name: 'gated', input: z.object({}) }, async ({ step }) => {
      await step.sleep('nap', '1h')
    })
    const engine = createEngine({ storage: gated, workflows: [wf] })
    await engine.start(NO_POLL)
    const run = await engine.enqueue('gated', {})
    await vi.waitFor(() => expect(releaseSleep).not.toBe(undefined))
    await vi.waitFor(async () => expect((await storage.getStepResults(run.id)).length).toBe(1))
    await delay(60)

    const stopping = engine.stop().then(() => landed.push('stop'))
    await delay(20)
    releaseSleep()
    await stopping

    expect(landed).toStrictEqual(['sleepRun', 'stop'])
  })

  it('keys a cached step on failures the function caught, not just completed outputs', async () => {
    let reason = ''
    const wf = createWorkflow({ name: 'decide', input: z.object({}) }, async ({ step }) => {
      let why = 'ok'
      try {
        await step.run('lookup', async () => {
          throw new Error(reason)
        })
      } catch (error) {
        if (!(error instanceof StepFailedError)) throw error
        why = error.message
      }
      return await step.run('decide', { cache: '1h', handler: async () => `decided on: ${why}` })
    })
    const engine = createEngine({ storage, workflows: [wf] })
    reason = 'rate limited'
    const first = await engine.enqueue('decide', {})
    await engine.tick()
    reason = 'account closed'
    const second = await engine.enqueue('decide', {})
    await engine.tick()

    expect((await storage.getRun(first.id))?.output).toContain('rate limited')
    expect((await storage.getRun(second.id))?.output).toContain('account closed')
  })

  it('records an operation the function never awaited as skipped once the run completes', async () => {
    const wf = createWorkflow({ name: 'stray', input: z.object({}) }, async ({ step }) => {
      void step.sleep('forgotten', '1h')
      return 1
    })
    const result = await testEngine({ workflows: [wf] }).run('stray', {})

    expect(result).toMatchObject({ status: 'completed', output: 1 })
    expect(result.steps['forgotten']).toStrictEqual({ status: 'skipped', output: null, error: null })
  })

  it('reports the handler\'s error when a caught-up crash replays the failure that ended the run', async () => {
    vi.useFakeTimers({ toFake: ['Date'] })
    let crashed = false
    const crashing = delegatingAdapter(storage, {
      failRun: async (runId, leaseId, error) => {
        if (!crashed) {
          crashed = true
          throw new Error('process died')
        }
        return storage.failRun(runId, leaseId, error)
      },
    })
    const wf = createWorkflow({ name: 'crash', input: z.object({}) }, async ({ step }) => {
      await step.run('boom', async () => {
        throw new Error('card declined')
      })
    })
    const first = createEngine({ storage: crashing, workflows: [wf], runLeaseDurationMs: 1_000 })
    const run = await first.enqueue('crash', {})
    await first.tick().catch(() => {})

    vi.setSystemTime(Date.now() + 5_000)
    const second = createEngine({ storage, workflows: [wf], runLeaseDurationMs: 1_000 })
    await second.tick()

    expect((await storage.getRun(run.id))?.error).toMatchObject({ stepName: 'boom', message: 'card declined' })
  })

  it('keeps failures the function caught when a failed run is resumed, rerunning only the one that ended it', async () => {
    let charges = 0
    let ships = 0
    const path: string[] = []
    const wf = createWorkflow({ name: 'resume-caught', input: z.object({}) }, async ({ step }) => {
      try {
        await step.run('charge', async () => {
          if (++charges === 1) throw new Error('declined')
        })
        path.push('charged')
      } catch (error) {
        if (!(error instanceof StepFailedError)) throw error
        path.push('declined')
        await step.run('notify', async () => {})
      }
      await step.run('ship', async () => {
        if (++ships === 1) throw new Error('warehouse down')
      })
    })
    const engine = createEngine({ storage, workflows: [wf] })
    const run = await engine.enqueue('resume-caught', {})
    await engine.tick()
    expect((await storage.getRun(run.id))?.error?.stepName).toBe('ship')

    await engine.resume(run.id)
    await engine.tick()

    expect([charges, ships]).toStrictEqual([1, 2])
    expect(path).toStrictEqual(['declined', 'declined'])
    expect((await storage.getRun(run.id))?.status).toBe('completed')
  })
})

describe('workflows written as a function, second adversarial round', () => {
  let storage: MemoryStorage

  beforeEach(async () => {
    storage = new MemoryStorage()
    await storage.initialize()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  const delay = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))

  /** A race between `a` (instant) and `b` (10ms), then a wait, run twice; returns the winner each time. */
  async function raceTwice(options: { slowHookFor?: string; adapter?: StorageAdapter; cacheA?: boolean }): Promise<string[]> {
    const winners: string[] = []
    let firstExecution = true
    const wf = createWorkflow({ name: 'race-gap', input: z.object({}), events: { go: z.object({}) } }, async ({ step }) => {
      const winner = await Promise.race([
        step.run('a', options.cacheA ? { cache: '1h', handler: async () => 'a' } : async () => 'a'),
        step.run('b', async () => {
          await delay(10)
          return 'b'
        }),
      ])
      winners.push(winner)
      await step.waitForEvent('go')
      return winner
    })
    const engine = createEngine({
      storage: options.adapter ?? storage,
      workflows: [wf],
      hooks: {
        onStepComplete: async (event) => {
          if (firstExecution && event.stepName === options.slowHookFor) await delay(60)
        },
      },
    })
    const run = await engine.enqueue('race-gap', {})
    await engine.tick()
    firstExecution = false
    await engine.sendEvent(run.id, 'go', {})
    await engine.tick()
    return winners
  }

  it('replays a race with the same winner when a hook delays the first finisher', async () => {
    const [first, replayed] = await raceTwice({ slowHookFor: 'a' })
    expect(replayed).toBe(first)
  })

  it('replays a race with the same winner when caching the first finisher is slow', async () => {
    let slow = true
    const adapter = delegatingAdapter(storage, {
      putCachedStep: async (entry) => {
        if (slow) {
          slow = false
          await delay(60)
        }
        await storage.putCachedStep(entry)
      },
    })
    const [first, replayed] = await raceTwice({ adapter, cacheA: true })
    expect(replayed).toBe(first)
  })

  it('sees an event sent between the wait looking for it and blocking', async () => {
    let engine: ReturnType<typeof createEngine> | undefined
    let runId = ''
    const sending = delegatingAdapter(storage, {
      saveStepResult: async (row, leaseId) => {
        const saved = await storage.saveStepResult(row, leaseId)
        if (row.name === 'go' && row.status === 'waiting') await engine?.sendEvent(runId, 'go', {})
        return saved
      },
    })
    const wf = createWorkflow({ name: 'gap', input: z.object({}), events: { go: z.object({}) } }, async ({ step }) => {
      return await Promise.race([
        step.waitForEvent('go').then(() => 'event'),
        step.run('long', async () => {
          await delay(800)
          return 'long'
        }),
      ])
    })
    engine = createEngine({ storage: sending, workflows: [wf] })
    runId = (await engine.enqueue('gap', {})).id
    await engine.tick()

    expect((await storage.getRun(runId))?.output).toBe('event')
  })

  it('sees an event sent through another engine while the wait sits beside busy work', async () => {
    let heardAfter = Infinity
    const wf = createWorkflow({ name: 'cross', input: z.object({}), events: { go: z.object({}) } }, async ({ step }) => {
      return await Promise.race([
        step.waitForEvent('go').then(() => {
          heardAfter = Date.now() - started
          return 'event'
        }),
        step.run('long', async () => {
          await delay(3_000)
          return 'long'
        }),
      ])
    })
    const worker = createEngine({ storage, workflows: [wf] })
    const api = createEngine({ storage, workflows: [wf] })
    const run = await worker.enqueue('cross', {})
    const started = Date.now()
    const ticking = worker.tick()
    await delay(100)
    await api.sendEvent(run.id, 'go', {})
    await ticking

    // The losing step runs on to its end, so the run completes after it; the
    // wait itself heard the event within a poll.
    expect((await storage.getRun(run.id))?.output).toBe('event')
    expect(heardAfter).toBeLessThan(2_000)
  }, 10_000)

  it('completes a run whose completion was cut off after it recorded unawaited operations', async () => {
    vi.useFakeTimers({ toFake: ['Date'] })
    let died = false
    const crashing = delegatingAdapter(storage, {
      completeRun: async (runId, leaseId, output) => {
        if (!died) {
          died = true
          throw new Error('process died')
        }
        return storage.completeRun(runId, leaseId, output)
      },
    })
    const wf = createWorkflow({ name: 'stray-crash', input: z.object({}) }, async ({ step }) => {
      void step.sleep('forgotten', '1h')
      return 1
    })
    const first = createEngine({ storage: crashing, workflows: [wf], runLeaseDurationMs: 1_000 })
    const run = await first.enqueue('stray-crash', {})
    await first.tick().catch(() => {})
    vi.setSystemTime(Date.now() + 5_000)
    await createEngine({ storage, workflows: [wf], runLeaseDurationMs: 1_000 }).tick()

    expect(await storage.getRun(run.id)).toMatchObject({ status: 'completed', output: 1 })
  })

  it('reruns the step behind a failure the function wrapped in its own error, on resume', async () => {
    let charges = 0
    const wf = createWorkflow({ name: 'wrapped', input: z.object({}) }, async ({ step }) => {
      try {
        await step.run('charge', async () => {
          if (++charges === 1) throw new Error('declined')
        })
      } catch (error) {
        throw new Error('payment failed', { cause: error })
      }
      return 'paid'
    })
    const engine = createEngine({ storage, workflows: [wf] })
    const run = await engine.enqueue('wrapped', {})
    await engine.tick()
    expect((await storage.getRun(run.id))?.error).toMatchObject({ stepName: 'charge', message: 'payment failed' })

    await engine.resume(run.id)
    await engine.tick()

    expect(charges).toBe(2)
    expect(await storage.getRun(run.id)).toMatchObject({ status: 'completed', output: 'paid' })
  })

  it('replays a caught failure with the same error name and code, and keeps the code after a crash', async () => {
    vi.useFakeTimers({ toFake: ['Date'] })
    class CardDeclined extends Error {
      readonly code = 'card_declined'
      constructor() {
        super('Card declined')
        this.name = 'CardDeclined'
      }
    }
    const seen: unknown[] = []
    const wf = createWorkflow({ name: 'declined', input: z.object({}), events: { go: z.object({}) } }, async ({ step }) => {
      try {
        await step.run('charge', async () => {
          throw new CardDeclined()
        })
      } catch (error) {
        if (!(error instanceof StepFailedError) || !(error.cause instanceof Error)) throw error
        seen.push([error.cause.name, 'code' in error.cause ? error.cause.code : undefined, error.cause.message])
      }
      await step.waitForEvent('go')
      await step.run('timed-out', { timeout: 20, handler: () => delay(1_000) })
    })
    let died = false
    const crashing = delegatingAdapter(storage, {
      failRun: async (runId, leaseId, error) => {
        if (!died) {
          died = true
          throw new Error('process died')
        }
        return storage.failRun(runId, leaseId, error)
      },
    })
    const engine = createEngine({ storage: crashing, workflows: [wf], runLeaseDurationMs: 1_000 })
    const run = await engine.enqueue('declined', {})
    await engine.tick()
    await engine.sendEvent(run.id, 'go', {})
    await engine.tick().catch(() => {})
    vi.setSystemTime(Date.now() + 5_000)
    await createEngine({ storage, workflows: [wf], runLeaseDurationMs: 1_000 }).tick()

    expect(seen).toStrictEqual([
      ['CardDeclined', 'card_declined', 'Card declined'],
      ['CardDeclined', 'card_declined', 'Card declined'],
      ['CardDeclined', 'card_declined', 'Card declined'],
    ])
    expect((await storage.getRun(run.id))?.error).toMatchObject({ stepName: 'timed-out', code: 'STEP_TIMEOUT' })
  })

  it('treats a control-flow error thrown by user code as a failure, not a reason to leave the run running', async () => {
    const inStep = createWorkflow({ name: 'throws-in-step', input: z.object({}) }, async ({ step, runId }) => {
      await step.run('liar', async () => {
        throw new RunSuspendedError(runId)
      })
    })
    const inBody = createWorkflow({ name: 'throws-in-body', input: z.object({}) }, async ({ runId }) => {
      throw new RunCancelledError(runId)
    })
    const engine = createEngine({ storage, workflows: [inStep, inBody] })
    const a = await engine.enqueue('throws-in-step', {})
    const b = await engine.enqueue('throws-in-body', {})
    await engine.tick()
    await engine.tick()

    expect(await storage.getRun(a.id)).toMatchObject({ status: 'failed', error: { stepName: 'liar', code: 'RUN_SUSPENDED' } })
    expect(await storage.getRun(b.id)).toMatchObject({ status: 'failed', error: { stepName: '', code: 'RUN_CANCELLED' } })
  })

  it('fails the run when a step handler starts another operation', async () => {
    const wf = createWorkflow({ name: 'nested', input: z.object({}) }, async ({ step }) => {
      await step.run('outer', async () => {
        await step.run('inner', async () => 1)
      })
    })
    const engine = createEngine({ storage, workflows: [wf] })
    const run = await engine.enqueue('nested', {})
    await engine.tick()

    expect((await storage.getRun(run.id))?.error).toMatchObject({ code: 'CONFIG' })
    expect((await storage.getStepResults(run.id)).map((step) => step.name)).not.toContain('inner')
  })

  it('runs under default fake timers when the test advances them', async () => {
    vi.useFakeTimers()
    const wf = createWorkflow({ name: 'faked', input: z.object({}) }, async ({ step }) => {
      const a = await step.run('a', () => new Promise<number>((resolve) => setTimeout(() => resolve(1), 100)))
      const b = await step.run('b', () => new Promise<number>((resolve) => setTimeout(() => resolve(2), 100)))
      return a + b
    })
    const engine = createEngine({ storage, workflows: [wf] })
    const run = await engine.enqueue('faked', {})
    const ticking = engine.tick()
    await vi.advanceTimersByTimeAsync(500)
    await ticking

    expect(await storage.getRun(run.id)).toMatchObject({ status: 'completed', output: 3 })
  })

  it('does not rewrite a sleeping row each time it wakes early in process', async () => {
    vi.useFakeTimers({ toFake: ['Date'] })
    let sleepingSaves = 0
    const counting = delegatingAdapter(storage, {
      saveStepResult: async (row, leaseId) => {
        if (row.status === 'sleeping') sleepingSaves++
        return storage.saveStepResult(row, leaseId)
      },
    })
    const wf = createWorkflow({ name: 'frozen', input: z.object({}) }, async ({ step }) => {
      await Promise.all([step.sleep('blink', 10), step.run('busy', () => delay(200))])
    })
    const engine = createEngine({ storage: counting, workflows: [wf] })
    await engine.enqueue('frozen', {})
    await engine.tick()

    expect(sleepingSaves).toBe(1)
  })
})

describe('types of waits, second adversarial round', () => {
  it('adds null whenever a timeout may be set, and types a wait named apart from its event', () => {
    createWorkflow({
      name: 'wait-types',
      input: z.object({}),
      events: { approved: z.object({ by: z.string() }) },
    }, async ({ step }) => {
      const optional: { timeout?: string } = { timeout: '1s' }
      expectTypeOf(await step.waitForEvent('approved', optional)).toEqualTypeOf<{ by: string } | null>()

      const helper = (options?: Pick<WaitForEventOptions, 'timeout'>) => step.waitForEvent('approved', options)
      expectTypeOf(await helper()).toEqualTypeOf<{ by: string } | null>()

      expectTypeOf(await step.waitForEvent('round-2', { event: 'approved' })).toEqualTypeOf<{ by: string }>()
      // @ts-expect-error -- a wait named apart from its event still needs a declared event
      await step.waitForEvent('round-3', { event: 'rejected' })
    })
  })
})

describe('compatibility for chained workflows', () => {
  it('keeps ReturnType<typeof createWorkflow> the chained builder', () => {
    type Built = ReturnType<typeof createWorkflow>
    expectTypeOf<Built>().toHaveProperty('step')
    expectTypeOf<ChainWorkflow>().toHaveProperty('executionUnits')
  })

  it('resumes a chained run with the same storage call as before', async () => {
    const storage = new MemoryStorage()
    await storage.initialize()
    const calls: unknown[][] = []
    const spying = delegatingAdapter(storage, {
      requeueRun: (...args) => {
        calls.push(args)
        return storage.requeueRun(...args)
      },
    })
    const wf = createWorkflow({ name: 'chain', input: z.object({}) }).step('boom', async () => {
      throw new Error('boom')
    })
    const engine = createEngine({ storage: spying, workflows: [wf] })
    const run = await engine.enqueue('chain', {})
    await engine.tick()
    await engine.resume(run.id)

    expect(calls).toStrictEqual([[run.id]])
  })
})

describe('types of step results that can be saved', () => {
  interface Charge {
    id: string
    amount: number
    created: Date
    metadata: { tags: string[] }
    refunded?: boolean
  }
  const stripe = { charge: async (): Promise<Charge> => ({ id: 'ch', amount: 1, created: new Date(), metadata: { tags: [] } }) }

  it('accepts what an SDK returns, typed as an interface, and widens literals', () => {
    createWorkflow({ name: 'sdk', input: z.object({}) }, async ({ step }) => {
      const charge = await step.run('charge', () => stripe.charge())
      expectTypeOf(charge).toEqualTypeOf<Charge>()
      const configured = await step.run('again', { retry: { maxAttempts: 2 }, handler: () => stripe.charge() })
      expectTypeOf(configured).toEqualTypeOf<Charge>()
      expectTypeOf(await step.run('flag', async () => ({ ok: true }))).toEqualTypeOf<{ ok: boolean }>()
      return charge
    })
  })

  it('refuses a result, or a run output, that cannot be saved', () => {
    createWorkflow({ name: 'unsaveable', input: z.object({}) }, async ({ step }) => {
      // @ts-expect-error -- a function cannot be saved
      await step.run('fn', async () => () => 1)
      // @ts-expect-error -- nor can a Map, even inside an object
      await step.run('map', async () => ({ index: new Map<string, number>() }))
      // @ts-expect-error -- nor in the config form
      await step.run('config', { timeout: '1s', handler: async () => new Set<number>() })
    })
    // @ts-expect-error -- the run's output must be data too
    createWorkflow({ name: 'bad-output-type', input: z.object({}) }, async () => ({ later: Promise.resolve(1) }))
  })

  it('accepts recursive interfaces and fields typed unknown, and still finds a Map deep inside', () => {
    interface TreeNode {
      value: number
      children: TreeNode[]
      parent?: TreeNode
    }
    interface LinkedList<T> {
      head: T
      tail: LinkedList<T> | null
    }
    interface ToolUse {
      id: string
      input: unknown
    }
    interface Deep {
      a: { b: { c: { index: Map<string, number> } } }
    }
    const tree = async (): Promise<TreeNode> => ({ value: 1, children: [] })
    const list = async (): Promise<LinkedList<string>> => ({ head: 'a', tail: null })
    const tool = async (): Promise<ToolUse> => ({ id: 't', input: {} })
    const deep = async (): Promise<Deep> => ({ a: { b: { c: { index: new Map() } } } })
    createWorkflow({ name: 'shapes', input: z.object({}) }, async ({ step }) => {
      expectTypeOf(await step.run('tree', tree)).toEqualTypeOf<TreeNode>()
      expectTypeOf(await step.run('list', list)).toEqualTypeOf<LinkedList<string>>()
      expectTypeOf(await step.run('tool', tool)).toEqualTypeOf<ToolUse>()
      expectTypeOf(await step.run('unknown', async (): Promise<unknown> => 1)).toEqualTypeOf<unknown>()
      // @ts-expect-error -- a Map three levels down still cannot be saved
      await step.run('deep', deep)
    })
  })

  it('accepts a result that is sometimes nothing', () => {
    const notify = async (): Promise<void> => {}
    createWorkflow({ name: 'maybe-nothing', input: z.object({ flag: z.boolean() }) }, async ({ input, step }) => {
      expectTypeOf(await step.run('b', async () => (input.flag ? notify() : 1))).toEqualTypeOf<number | undefined>()
      return Promise.race([step.sleep('wait', '1m'), step.run('c', async () => 2)])
    })
  })

  it('types a run output typed as an interface', () => {
    const wf = createWorkflow({ name: 'iface-out', input: z.object({}) }, async ({ step }) => step.run('c', () => stripe.charge()))
    expectTypeOf<InferOutput<typeof wf>>().toEqualTypeOf<Charge>()
  })
})

describe('workflows written as a function, PR review', () => {
  let storage: MemoryStorage

  beforeEach(async () => {
    storage = new MemoryStorage()
    await storage.initialize()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  const delay = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))

  it('lets a step handler send an event another run is waiting for', async () => {
    let engine: ReturnType<typeof createEngine> | undefined
    let waiterId = ''
    const waiter = createWorkflow({ name: 'waiter', input: z.object({}), events: { go: z.object({}) } }, async ({ step }) => {
      await step.waitForEvent('go')
      return await step.run('after', async () => 'woken')
    })
    const sender = createWorkflow({ name: 'sender', input: z.object({}) }, async ({ step }) => {
      await step.run('send', async () => {
        await engine?.sendEvent(waiterId, 'go', {})
        await engine?.enqueue('child', {})
      })
    })
    const child = createWorkflow({ name: 'child', input: z.object({}) }, async ({ step }) => step.run('only', async () => 'child ran'))
    engine = createEngine({ storage, workflows: [waiter, sender, child], concurrency: 4 })
    waiterId = (await engine.enqueue('waiter', {})).id
    await engine.tick()
    await engine.enqueue('sender', {})
    await engine.start(NO_POLL)
    await vi.waitFor(async () => {
      const runs = await storage.listRuns({})
      expect(runs.map((run) => run.status).sort()).toStrictEqual(['completed', 'completed', 'completed'])
    })
    await engine.stop()

    expect((await storage.getRun(waiterId))?.output).toBe('woken')
  })

  it('blames the function, not a step it cut off, when its own code throws', async () => {
    const wf = createWorkflow({ name: 'own-error', input: z.object({}) }, async ({ step }) => {
      await Promise.all([
        step.run('slow', () => delay(200)),
        (async () => {
          await delay(10)
          throw new Error('boom')
        })(),
      ])
    })
    const engine = createEngine({ storage, workflows: [wf] })
    const run = await engine.enqueue('own-error', {})
    await engine.tick()

    expect((await storage.getRun(run.id))?.error).toStrictEqual({ stepName: '', message: 'boom', code: null })
  })

  it('replays a race the same way when the winner is started later, after other work', async () => {
    vi.useFakeTimers({ toFake: ['Date'] })
    const winners: string[] = []
    // `a` finishes after `b` by construction, not by a margin a slow machine could close.
    let bFinished: () => void = () => {}
    const afterB = new Promise<void>((resolve) => { bFinished = resolve })
    const wf = createWorkflow({ name: 'late-start', input: z.object({}) }, async ({ step }) => {
      const a = step.run('a', async () => {
        await afterB
        await delay(10)
        return 'a'
      })
      await delay(10)
      const b = step.run('b', async () => {
        await delay(10)
        bFinished()
        return 'b'
      })
      const winner = await Promise.race([a, b])
      winners.push(winner)
      await step.sleep('later', '1h')
      return winner
    })
    const engine = createEngine({ storage, workflows: [wf] })
    const run = await engine.enqueue('late-start', {})
    await engine.tick()
    vi.setSystemTime(Date.now() + HOUR)
    await engine.tick()

    expect(winners).toStrictEqual(['b', 'b'])
    expect((await storage.getRun(run.id))?.output).toBe('b')
  })

  it('completes when the only blocked operation is one the function never waited for', async () => {
    const wf = createWorkflow({ name: 'ignored-sleep', input: z.object({}) }, async ({ step }) => {
      void step.sleep('nap', '1h')
      await delay(60)
      return 'done'
    })
    const engine = createEngine({ storage, workflows: [wf] })
    const run = await engine.enqueue('ignored-sleep', {})
    await engine.tick()

    expect(await storage.getRun(run.id)).toMatchObject({ status: 'completed', output: 'done' })
    expect((await storage.getStepResults(run.id)).map((step) => `${step.name}:${step.status}`)).toStrictEqual(['nap:skipped'])
  })

  it('finishes a replay whose code no longer reaches a finished operation', async () => {
    vi.useFakeTimers({ toFake: ['Date'] })
    let deployed = false
    const wf = createWorkflow({ name: 'removed', input: z.object({}) }, async ({ step }) => {
      const early = deployed ? 'gone' : await step.run('early', async () => 'early')
      const late = step.run('late', async () => 'late')
      await step.sleep('nap', '1h')
      return [early, await late]
    })
    const engine = createEngine({ storage, workflows: [wf] })
    const run = await engine.enqueue('removed', {})
    await engine.tick()
    deployed = true
    vi.setSystemTime(Date.now() + HOUR)
    await engine.tick()

    expect(await storage.getRun(run.id)).toMatchObject({ status: 'completed', output: ['gone', 'late'] })
  })
})

describe('the operation limit of a workflow written as a function', () => {
  it('fails a run that starts more operations than its limit, and accepts a raised one', async () => {
    const storage = new MemoryStorage()
    await storage.initialize()
    const loop = (name: string, maxOperations?: number) => createWorkflow({ name, input: z.object({ n: z.number() }), maxOperations }, async ({ input, step }) => {
      for (let index = 0; index < input.n; index++) await step.run(`item-${index}`, async () => index)
      return input.n
    })
    const capped = loop('capped', 3)
    const plenty = loop('plenty', 10)
    const engine = createEngine({ storage, workflows: [capped, plenty] })
    const over = await engine.enqueue('capped', { n: 4 })
    const within = await engine.enqueue('plenty', { n: 4 })
    await engine.tick()
    await engine.tick()

    expect((await storage.getRun(over.id))?.error).toMatchObject({ stepName: 'item-3', code: 'CONFIG' })
    expect((await storage.getRun(within.id))?.output).toBe(4)
    expect(() => loop('bad', 0)).toThrow(ConfigError)
  })
})

describe('workflows written as a function, final review', () => {
  let storage: MemoryStorage

  beforeEach(async () => {
    storage = new MemoryStorage()
    await storage.initialize()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  const delay = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))

  it('leaves no promise the function derived rejecting unhandled when the run suspends, is cancelled, or stops', async () => {
    const unhandled: unknown[] = []
    const onUnhandled = (reason: unknown) => { unhandled.push(reason) }
    process.on('unhandledRejection', onUnhandled)
    try {
      let engine: ReturnType<typeof createEngine> | undefined
      const helpers = createWorkflow({ name: 'helpers', input: z.object({ mode: z.string() }) }, async ({ input, step, runId }) => {
        const get = async (id: string) => await step.run(`user-${id}`, {
          retry: { maxAttempts: 3, initialDelay: '1h' },
          handler: async () => {
            if (input.mode === 'cancel') await engine?.cancel(runId)
            throw new Error('flaky')
          },
        })
        void step.sleep('aside', '1h').finally(() => {})
        const first = get('1')
        const second = get('2')
        return [await first, await second]
      })
      engine = createEngine({ storage, workflows: [helpers] })
      await engine.start(NO_POLL)
      const suspended = await engine.enqueue('helpers', { mode: 'suspend' })
      const cancelled = await engine.enqueue('helpers', { mode: 'cancel' })
      await vi.waitFor(async () => {
        expect((await storage.getRun(suspended.id))?.status).toBe('sleeping')
        expect((await storage.getRun(cancelled.id))?.status).toBe('cancelled')
      })
      await engine.stop()
      await delay(50)
    } finally {
      process.off('unhandledRejection', onUnhandled)
    }

    expect(unhandled).toStrictEqual([])
  })

  it('carries on past a race whose loser is still pending, through other work outside a step', async () => {
    const wf = createWorkflow({ name: 'race-then-work', input: z.object({}), events: { cancel: z.object({}) } }, async ({ step }) => {
      const result = await Promise.race([
        step.run('work', async () => 'done'),
        step.waitForEvent('cancel').then(() => 'cancelled'),
      ])
      await delay(100)
      return await step.run('next', async () => `${result} then next`)
    })
    const engine = createEngine({ storage, workflows: [wf] })
    const run = await engine.enqueue('race-then-work', {})
    await engine.tick()

    expect(await storage.getRun(run.id)).toMatchObject({ status: 'completed', output: 'done then next' })
  })
})
