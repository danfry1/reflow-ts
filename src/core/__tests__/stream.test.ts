import { describe, it, expect, vi, beforeEach } from 'vitest'
import { z } from 'zod'
import { createWorkflow } from '../workflow'
import { createEngine } from '../engine'
import type { EngineEvent } from '../engine'
import type { StorageAdapter } from '../types'
import { MemoryStorage } from '../../storage/memory'

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function deferred(): {
  promise: Promise<void>
  resolve: () => void
} {
  let resolve!: () => void
  const promise = new Promise<void>((innerResolve) => {
    resolve = innerResolve
  })
  return { promise, resolve }
}

async function settlesWithin<T>(promise: Promise<T>, timeoutMs = 500): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new Error(`Promise did not settle within ${timeoutMs}ms`))
    }, timeoutMs)
    promise.then(
      (value) => {
        clearTimeout(timer)
        resolve(value)
      },
      (error) => {
        clearTimeout(timer)
        reject(error)
      },
    )
  })
}

function delaySecondClaim(
  delegate: MemoryStorage,
  secondClaimStarted: { resolve: () => void },
  releaseSecondClaim: Promise<void>,
): StorageAdapter {
  let claimCalls = 0
  return {
    initialize: () => delegate.initialize(),
    createRun: (run) => delegate.createRun(run),
    claimNextRun: async (workflowNames, staleBefore) => {
      claimCalls++
      if (claimCalls === 2) {
        secondClaimStarted.resolve()
        await releaseSecondClaim
      }
      return delegate.claimNextRun(workflowNames, staleBefore)
    },
    heartbeatRun: (runId, leaseId) => delegate.heartbeatRun(runId, leaseId),
    sleepRun: (runId, leaseId, wakeAt) => delegate.sleepRun(runId, leaseId, wakeAt),
    getRun: (runId) => delegate.getRun(runId),
    getStepResults: (runId) => delegate.getStepResults(runId),
    saveStepResult: (result, leaseId) => delegate.saveStepResult(result, leaseId),
    updateRunStatus: (runId, status) => delegate.updateRunStatus(runId, status),
    updateClaimedRunStatus: (runId, leaseId, status) =>
      delegate.updateClaimedRunStatus(runId, leaseId, status),
    close: () => delegate.close(),
  }
}

describe('async hooks', () => {
  let storage: MemoryStorage

  beforeEach(async () => {
    storage = new MemoryStorage()
    await storage.initialize()
  })

  it('awaits an async hook before running the next step', async () => {
    const order: string[] = []

    const wf = createWorkflow({ name: 'async-hook', input: z.object({}) })
      .step('a', async () => {
        order.push('a-run')
        return { x: 1 }
      })
      .step('b', async () => {
        order.push('b-run')
        return { y: 2 }
      })

    const engine = createEngine({
      storage,
      workflows: [wf],
      hooks: {
        onStepComplete: async (event) => {
          await delay(5)
          order.push(`hook-${event.stepName}`)
        },
      },
    })

    await engine.enqueue('async-hook', {})
    await engine.tick()

    // The async onStepComplete for 'a' must settle before 'b' begins.
    expect(order).toEqual(['a-run', 'hook-a', 'b-run', 'hook-b'])
  })

  it('a rejecting async hook does not fail the run', async () => {
    const wf = createWorkflow({ name: 'reject-hook', input: z.object({}) })
      .step('a', async () => ({ ok: true }))

    const engine = createEngine({
      storage,
      workflows: [wf],
      hooks: {
        onStepComplete: async () => {
          await delay(1)
          throw new Error('async hook boom')
        },
      },
    })

    const run = await engine.enqueue('reject-hook', {})
    await expect(engine.tick()).resolves.toBeUndefined()

    const info = await engine.getRunStatus(run.id)
    expect(info?.run.status).toBe('completed')
  })

  it('awaits async onRunComplete with the final run output', async () => {
    let captured: EngineEvent | undefined

    const wf = createWorkflow({ name: 'final-output', input: z.object({}) })
      .step('a', async () => ({ step: 'a' }))
      .step('b', async () => ({ result: 42 }))

    const engine = createEngine({
      storage,
      workflows: [wf],
      hooks: {
        onRunComplete: async (event) => {
          await delay(1)
          captured = event
        },
      },
    })

    await engine.enqueue('final-output', {})
    await engine.tick()

    expect(captured).toEqual({
      type: 'runComplete',
      runId: expect.any(String),
      workflow: 'final-output',
      output: { result: 42 },
    })
  })

  it('awaits async start and failure hooks in lifecycle order', async () => {
    const order: string[] = []
    const wf = createWorkflow({ name: 'hook-order', input: z.object({}) })
      .step('broken', async () => {
        order.push('handler')
        throw new Error('boom')
      })

    const engine = createEngine({
      storage,
      workflows: [wf],
      hooks: {
        onRunStart: async () => {
          await delay(1)
          order.push('run-start')
        },
        onStepStart: async () => {
          await delay(1)
          order.push('step-start')
        },
        onRunFailed: async () => {
          await delay(1)
          order.push('run-failed')
        },
      },
    })

    await engine.enqueue('hook-order', {})
    await engine.tick()

    expect(order).toEqual(['run-start', 'step-start', 'handler', 'run-failed'])
  })

  it('cancelling a run interrupts a pending async hook', async () => {
    const hookStarted = deferred()
    const wf = createWorkflow({ name: 'cancel-hook', input: z.object({}) })
      .step('a', async () => ({ ok: true }))

    const engine = createEngine({
      storage,
      workflows: [wf],
      hooks: {
        onRunStart: () => {
          hookStarted.resolve()
          return new Promise<void>(() => {})
        },
      },
    })

    const run = await engine.enqueue('cancel-hook', {})
    const tickPromise = engine.tick()
    await hookStarted.promise

    expect(await engine.cancel(run.id)).toBe(true)
    await settlesWithin(tickPromise)

    expect((await engine.getRunStatus(run.id))?.run.status).toBe('cancelled')
  })

  it('stop() interrupts a pending async hook and waits for the tick to settle', async () => {
    const hookStarted = deferred()
    const wf = createWorkflow({ name: 'stop-hook', input: z.object({}) })
      .step('a', async () => ({ ok: true }))

    const engine = createEngine({
      storage,
      workflows: [wf],
      hooks: {
        onRunStart: () => {
          hookStarted.resolve()
          return new Promise<void>(() => {})
        },
      },
    })

    const run = await engine.enqueue('stop-hook', {})
    const tickPromise = engine.tick()
    await hookStarted.promise

    await settlesWithin(engine.stop())
    await settlesWithin(tickPromise)

    expect((await engine.getRunStatus(run.id))?.run.status).toBe('running')
  })

  it('stop() interrupts a failure hook after a heartbeat error aborted execution', async () => {
    const delegate = new MemoryStorage()
    await delegate.initialize()
    let heartbeatCalls = 0
    const flakyStorage: StorageAdapter = {
      initialize: () => delegate.initialize(),
      createRun: (run) => delegate.createRun(run),
      claimNextRun: (workflowNames, staleBefore) =>
        delegate.claimNextRun(workflowNames, staleBefore),
      heartbeatRun: async (runId, leaseId) => {
        heartbeatCalls++
        if (heartbeatCalls === 1) throw new Error('heartbeat failed')
        return delegate.heartbeatRun(runId, leaseId)
      },
      sleepRun: (runId, leaseId, wakeAt) => delegate.sleepRun(runId, leaseId, wakeAt),
      getRun: (runId) => delegate.getRun(runId),
      getStepResults: (runId) => delegate.getStepResults(runId),
      saveStepResult: (result, leaseId) => delegate.saveStepResult(result, leaseId),
      updateRunStatus: (runId, status) => delegate.updateRunStatus(runId, status),
      updateClaimedRunStatus: (runId, leaseId, status) =>
        delegate.updateClaimedRunStatus(runId, leaseId, status),
      close: () => delegate.close(),
    }
    const failureHookStarted = deferred()
    const wf = createWorkflow({ name: 'heartbeat-hook', input: z.object({}) })
      .step('slow', async ({ signal }) => {
        await new Promise<void>((resolve, reject) => {
          const timer = setTimeout(resolve, 1000)
          signal.addEventListener('abort', () => {
            clearTimeout(timer)
            reject(signal.reason)
          }, { once: true })
        })
        return { ok: true }
      })

    const engine = createEngine({
      storage: flakyStorage,
      workflows: [wf],
      runLeaseDurationMs: 30,
      heartbeatIntervalMs: 5,
      hooks: {
        onRunFailed: () => {
          failureHookStarted.resolve()
          return new Promise<void>(() => {})
        },
      },
    })

    await engine.enqueue('heartbeat-hook', {})
    const tickPromise = engine.tick()
    await failureHookStarted.promise

    await settlesWithin(engine.stop())
    await settlesWithin(tickPromise)
  })
})

describe('engine control boundaries', () => {
  it('does not execute a run cancelled after claim but before active registration', async () => {
    const delegate = new MemoryStorage()
    await delegate.initialize()
    const secondClaimStarted = deferred()
    const releaseSecondClaim = deferred()
    const storage = delaySecondClaim(
      delegate,
      secondClaimStarted,
      releaseSecondClaim.promise,
    )
    const handler = vi.fn(async () => ({ ok: true }))
    const wf = createWorkflow({ name: 'cancel-before-register', input: z.object({}) })
      .step('a', handler)
    const engine = createEngine({ storage, workflows: [wf], concurrency: 2 })

    const run = await engine.enqueue('cancel-before-register', {})
    const tickPromise = engine.tick()
    await secondClaimStarted.promise

    expect(await engine.cancel(run.id)).toBe(true)
    releaseSecondClaim.resolve()
    await tickPromise

    expect(handler).not.toHaveBeenCalled()
    expect((await engine.getRunStatus(run.id))?.run.status).toBe('cancelled')
  })

  it('does not start claimed runs when stop() lands before active registration', async () => {
    const delegate = new MemoryStorage()
    await delegate.initialize()
    const secondClaimStarted = deferred()
    const releaseSecondClaim = deferred()
    const storage = delaySecondClaim(
      delegate,
      secondClaimStarted,
      releaseSecondClaim.promise,
    )
    const handler = vi.fn(async () => ({ ok: true }))
    const wf = createWorkflow({ name: 'stop-before-register', input: z.object({}) })
      .step('a', handler)
    const engine = createEngine({ storage, workflows: [wf], concurrency: 2 })

    const run = await engine.enqueue('stop-before-register', {})
    const tickPromise = engine.tick()
    await secondClaimStarted.promise

    const stopPromise = engine.stop()
    releaseSecondClaim.resolve()
    await Promise.all([stopPromise, tickPromise])

    expect(handler).not.toHaveBeenCalled()
    expect((await engine.getRunStatus(run.id))?.run.status).toBe('running')
  })
})

describe('engine.stream()', () => {
  let storage: MemoryStorage

  beforeEach(async () => {
    storage = new MemoryStorage()
    await storage.initialize()
  })

  it.each([
    ['negative', -1],
    ['fractional', 1.5],
    ['NaN', Number.NaN],
    ['negative infinity', Number.NEGATIVE_INFINITY],
  ])('rejects an invalid %s buffer size', (_label, bufferSize) => {
    const wf = createWorkflow({ name: 'invalid-buffer', input: z.object({}) })
      .step('a', async () => ({ ok: true }))
    const engine = createEngine({ storage, workflows: [wf] })

    expect(() => engine.stream({ bufferSize })).toThrow(
      'Stream bufferSize must be a non-negative integer or Infinity',
    )
  })

  it('accepts zero-capacity and unbounded streams', async () => {
    const wf = createWorkflow({ name: 'valid-buffer', input: z.object({}) })
      .step('a', async () => ({ ok: true }))
    const engine = createEngine({ storage, workflows: [wf] })
    const rendezvous = engine.stream({ bufferSize: 0 })
    const unbounded = engine.stream({ bufferSize: Number.POSITIVE_INFINITY })

    await rendezvous.return?.()
    await unbounded.return?.()
  })

  it('supports zero-capacity rendezvous without starting work before each pull', async () => {
    const handler = vi.fn(async () => ({ ok: true }))
    const wf = createWorkflow({ name: 'rendezvous', input: z.object({}) })
      .step('a', handler)
    const engine = createEngine({ storage, workflows: [wf] })
    const stream = engine.stream({ bufferSize: 0 })

    await engine.enqueue('rendezvous', {})
    const tickPromise = engine.tick()
    await delay(10)
    expect(handler).not.toHaveBeenCalled()

    expect((await stream.next()).value.type).toBe('runStart')
    await delay(10)
    expect(handler).not.toHaveBeenCalled()

    expect((await stream.next()).value.type).toBe('stepStart')
    await vi.waitFor(() => {
      expect(handler).toHaveBeenCalledOnce()
    })

    const remaining: string[] = []
    for await (const event of stream) {
      remaining.push(event.type)
      if (event.type === 'runComplete') break
    }

    await tickPromise
    expect(remaining).toEqual(['stepComplete', 'runComplete'])
  })

  it('never buffers more than the configured capacity', async () => {
    const stepStartHook = deferred()
    const wf = createWorkflow({ name: 'strict-capacity', input: z.object({}) })
      .step('a', async () => ({ ok: true }))
    const engine = createEngine({
      storage,
      workflows: [wf],
      hooks: {
        onStepStart: () => {
          stepStartHook.resolve()
        },
      },
    })
    const stream = engine.stream({ bufferSize: 1 })

    await engine.enqueue('strict-capacity', {})
    const tickPromise = engine.tick()
    await stepStartHook.promise
    await delay(0)

    await engine.stop()
    await tickPromise

    const buffered: string[] = []
    while (true) {
      const result = await stream.next()
      if (result.done) break
      buffered.push(result.value.type)
    }

    expect(buffered).toEqual(['runStart'])
  })

  it('yields lifecycle events in order for a single run', async () => {
    const wf = createWorkflow({ name: 'stream-order', input: z.object({}) })
      .step('a', async () => ({ x: 1 }))
      .step('b', async () => ({ y: 2 }))

    const engine = createEngine({ storage, workflows: [wf] })
    const stream = engine.stream()

    await engine.enqueue('stream-order', {})
    await engine.tick()

    const types: string[] = []
    for await (const event of stream) {
      types.push(event.type)
      if (event.type === 'runComplete') break
    }

    expect(types).toEqual([
      'runStart',
      'stepStart',
      'stepComplete',
      'stepStart',
      'stepComplete',
      'runComplete',
    ])
  })

  it('delivers the final output on runComplete', async () => {
    const wf = createWorkflow({ name: 'stream-output', input: z.object({}) })
      .step('a', async () => ({ done: true, value: 7 }))

    const engine = createEngine({ storage, workflows: [wf] })
    const stream = engine.stream()

    await engine.enqueue('stream-output', {})
    await engine.tick()

    let output: unknown
    for await (const event of stream) {
      if (event.type === 'runComplete') {
        output = event.output
        break
      }
    }

    expect(output).toEqual({ done: true, value: 7 })
  })

  it('tags every event with its workflow name across multiple workflows', async () => {
    const wfA = createWorkflow({ name: 'wf-a', input: z.object({}) }).step('s', async () => ({ a: 1 }))
    const wfB = createWorkflow({ name: 'wf-b', input: z.object({}) }).step('s', async () => ({ b: 1 }))

    const engine = createEngine({ storage, workflows: [wfA, wfB], concurrency: 2 })
    const stream = engine.stream()

    await engine.enqueue('wf-a', {})
    await engine.enqueue('wf-b', {})
    await engine.tick()

    const completions = new Set<string>()
    for await (const event of stream) {
      if (event.type === 'runComplete') {
        completions.add(event.workflow)
        if (completions.size === 2) break
      }
    }

    expect(completions).toEqual(new Set(['wf-a', 'wf-b']))
  })

  it('preserves per-run ordering with concurrent producers and a bounded buffer', async () => {
    const wf = createWorkflow({ name: 'concurrent-stream', input: z.object({ value: z.number() }) })
      .step('a', async ({ input }) => ({ value: input.value }))
    const engine = createEngine({ storage, workflows: [wf], concurrency: 3 })
    const stream = engine.stream({ bufferSize: 1 })
    const runs = await Promise.all([
      engine.enqueue('concurrent-stream', { value: 1 }),
      engine.enqueue('concurrent-stream', { value: 2 }),
      engine.enqueue('concurrent-stream', { value: 3 }),
    ])

    const eventsByRun = new Map<string, string[]>()
    const consumer = (async () => {
      let completions = 0
      for await (const event of stream) {
        const events = eventsByRun.get(event.runId) ?? []
        events.push(event.type)
        eventsByRun.set(event.runId, events)
        if (event.type === 'runComplete') {
          completions++
          if (completions === runs.length) break
        }
      }
    })()

    await Promise.all([engine.tick(), consumer])

    for (const run of runs) {
      expect(eventsByRun.get(run.id)).toEqual([
        'runStart',
        'stepStart',
        'stepComplete',
        'runComplete',
      ])
    }
  })

  it('applies backpressure: the engine pauses until the consumer pulls', async () => {
    const wf = createWorkflow({ name: 'backpressure', input: z.object({}) })
      .step('a', async () => ({ x: 1 }))
      .step('b', async () => ({ y: 2 }))
      .step('c', async () => ({ z: 3 }))

    const engine = createEngine({ storage, workflows: [wf] })
    const stream = engine.stream({ bufferSize: 1 })

    const run = await engine.enqueue('backpressure', {})

    let tickSettled = false
    const tickPromise = engine.tick().then(() => {
      tickSettled = true
    })

    // With a 1-event buffer and no consumer, the engine blocks early.
    await delay(30)
    expect(tickSettled).toBe(false)
    const midInfo = await engine.getRunStatus(run.id)
    expect(midInfo?.run.status).not.toBe('completed')

    // Draining the stream unblocks the producer and lets the run finish.
    const types: string[] = []
    for await (const event of stream) {
      types.push(event.type)
      if (event.type === 'runComplete') break
    }

    await tickPromise
    expect(tickSettled).toBe(true)
    expect(types.at(-1)).toBe('runComplete')

    const info = await engine.getRunStatus(run.id)
    expect(info?.run.status).toBe('completed')
  })

  it('cancelling a backpressured run releases tick() without stopping the engine', async () => {
    const wf = createWorkflow({ name: 'cancel-backpressure', input: z.object({}) })
      .step('a', async () => ({ ok: true }))
    const engine = createEngine({ storage, workflows: [wf] })
    const stream = engine.stream({ bufferSize: 1 })

    const run = await engine.enqueue('cancel-backpressure', {})
    const tickPromise = engine.tick()
    await vi.waitFor(async () => {
      expect((await engine.getRunStatus(run.id))?.run.status).toBe('running')
    })

    expect(await engine.cancel(run.id)).toBe(true)
    await settlesWithin(tickPromise)
    expect((await engine.getRunStatus(run.id))?.run.status).toBe('cancelled')

    await stream.return?.()
    const nextRun = await engine.enqueue('cancel-backpressure', {})
    await settlesWithin(engine.tick())
    expect((await engine.getRunStatus(nextRun.id))?.run.status).toBe('completed')
  })

  it('lease loss releases a producer paused on backpressure', async () => {
    const delegate = new MemoryStorage()
    await delegate.initialize()
    const leaseLosingStorage: StorageAdapter = {
      initialize: () => delegate.initialize(),
      createRun: (run) => delegate.createRun(run),
      claimNextRun: (workflowNames, staleBefore) =>
        delegate.claimNextRun(workflowNames, staleBefore),
      heartbeatRun: async () => false,
      sleepRun: (runId, leaseId, wakeAt) => delegate.sleepRun(runId, leaseId, wakeAt),
      getRun: (runId) => delegate.getRun(runId),
      getStepResults: (runId) => delegate.getStepResults(runId),
      saveStepResult: (result, leaseId) => delegate.saveStepResult(result, leaseId),
      updateRunStatus: (runId, status) => delegate.updateRunStatus(runId, status),
      updateClaimedRunStatus: (runId, leaseId, status) =>
        delegate.updateClaimedRunStatus(runId, leaseId, status),
      close: () => delegate.close(),
    }
    const wf = createWorkflow({ name: 'lease-backpressure', input: z.object({}) })
      .step('a', async () => ({ ok: true }))
    const engine = createEngine({
      storage: leaseLosingStorage,
      workflows: [wf],
      runLeaseDurationMs: 30,
      heartbeatIntervalMs: 5,
    })
    const stream = engine.stream({ bufferSize: 1 })

    const run = await engine.enqueue('lease-backpressure', {})
    await settlesWithin(engine.tick())

    expect((await engine.getRunStatus(run.id))?.run.status).toBe('running')
    await stream.return?.()
  })

  it('lets an independent stream continue after a backpressured stream is disposed', async () => {
    const wf = createWorkflow({ name: 'independent-streams', input: z.object({}) })
      .step('a', async () => ({ ok: true }))
    const engine = createEngine({ storage, workflows: [wf] })
    const blocked = engine.stream({ bufferSize: 0 })
    const observing = engine.stream()

    await engine.enqueue('independent-streams', {})
    const tickPromise = engine.tick()

    expect((await observing.next()).value.type).toBe('runStart')
    await blocked.return?.()

    const observed: string[] = ['runStart']
    for await (const event of observing) {
      observed.push(event.type)
      if (event.type === 'runComplete') break
    }

    await tickPromise
    expect(observed).toEqual(['runStart', 'stepStart', 'stepComplete', 'runComplete'])
  })

  it('unsubscribes when the consumer breaks out of the loop', async () => {
    const wf = createWorkflow({ name: 'unsub', input: z.object({}) })
      .step('a', async () => ({ x: 1 }))

    const engine = createEngine({ storage, workflows: [wf] })
    const stream = engine.stream({ bufferSize: 1 })

    // Consume one event then break — `for await` calls return(), disposing the stream.
    await engine.enqueue('unsub', {})
    const firstTick = engine.tick()
    for await (const event of stream) {
      void event
      break
    }
    await firstTick

    // A dead stream must not hold backpressure on future runs.
    const run = await engine.enqueue('unsub', {})
    await settlesWithin(engine.tick())

    const info = await engine.getRunStatus(run.id)
    expect(info?.run.status).toBe('completed')
  })

  it('hooks and streams both observe the same run', async () => {
    const wf = createWorkflow({ name: 'both', input: z.object({}) })
      .step('a', async () => ({ x: 1 }))

    const hookEvents: string[] = []
    const engine = createEngine({
      storage,
      workflows: [wf],
      hooks: { onRunComplete: () => hookEvents.push('hook') },
    })
    const stream = engine.stream()

    await engine.enqueue('both', {})
    await engine.tick()

    let streamSawComplete = false
    for await (const event of stream) {
      if (event.type === 'runComplete') {
        streamSawComplete = true
        break
      }
    }

    expect(hookEvents).toEqual(['hook'])
    expect(streamSawComplete).toBe(true)
  })

  it('isolates hook output mutations from workflow state and stream events', async () => {
    let downstreamValue: number | undefined
    const wf = createWorkflow({ name: 'hook-isolation', input: z.object({}) })
      .step('a', async () => ({ nested: { value: 1 } }))
      .step('b', async ({ prev }) => {
        downstreamValue = prev.nested.value
        return { observed: downstreamValue }
      })
    const engine = createEngine({
      storage,
      workflows: [wf],
      hooks: {
        onStepComplete: (event) => {
          if (event.stepName === 'a') {
            const output = event.output as { nested: { value: number } }
            output.nested.value = 999
          }
        },
      },
    })
    const stream = engine.stream()

    await engine.enqueue('hook-isolation', {})
    await engine.tick()

    let streamedValue: number | undefined
    for await (const event of stream) {
      if (event.type === 'stepComplete' && event.stepName === 'a') {
        streamedValue = (event.output as { nested: { value: number } }).nested.value
      }
      if (event.type === 'runComplete') break
    }

    expect(downstreamValue).toBe(1)
    expect(streamedValue).toBe(1)
  })

  it('gives each stream an isolated output snapshot', async () => {
    const wf = createWorkflow({ name: 'stream-isolation', input: z.object({}) })
      .step('a', async () => ({ nested: { value: 1 } }))
    const engine = createEngine({ storage, workflows: [wf] })
    const firstStream = engine.stream()
    const secondStream = engine.stream()

    await engine.enqueue('stream-isolation', {})
    await engine.tick()

    for await (const event of firstStream) {
      if (event.type === 'stepComplete') {
        const output = event.output as { nested: { value: number } }
        output.nested.value = 999
        break
      }
    }

    let secondValue: number | undefined
    for await (const event of secondStream) {
      if (event.type === 'stepComplete') {
        secondValue = (event.output as { nested: { value: number } }).nested.value
        break
      }
    }

    expect(secondValue).toBe(1)
  })

  it('isolates hook error mutations from failure handlers and streams', async () => {
    let failureHandlerMessage: string | undefined
    const wf = createWorkflow({ name: 'error-isolation', input: z.object({}) })
      .step('broken', async () => {
        throw new Error('original failure')
      })
      .onFailure(async ({ error }) => {
        failureHandlerMessage = error.message
      })
    const engine = createEngine({
      storage,
      workflows: [wf],
      hooks: {
        onRunFailed: (event) => {
          event.error.message = 'mutated by hook'
        },
      },
    })
    const stream = engine.stream()

    await engine.enqueue('error-isolation', {})
    await engine.tick()

    let streamMessage: string | undefined
    for await (const event of stream) {
      if (event.type === 'runFailed') {
        streamMessage = event.error.message
        break
      }
    }

    expect(failureHandlerMessage).toBe('original failure')
    expect(streamMessage).toBe('original failure')
  })

  it('emits runFailed with the original error and never emits runComplete', async () => {
    const failure = new Error('stream failure')
    const wf = createWorkflow({ name: 'stream-failure', input: z.object({}) })
      .step('broken', async () => {
        throw failure
      })
    const engine = createEngine({ storage, workflows: [wf] })
    const stream = engine.stream()

    const run = await engine.enqueue('stream-failure', {})
    await engine.tick()

    const types: string[] = []
    let observedError: Error | undefined
    for await (const event of stream) {
      types.push(event.type)
      if (event.type === 'runFailed') {
        observedError = event.error
        break
      }
    }

    expect(types).toEqual(['runStart', 'stepStart', 'runFailed'])
    expect(observedError).toBeInstanceOf(Error)
    expect(observedError).not.toBe(failure)
    expect(observedError?.message).toBe(failure.message)
    expect((await engine.getRunStatus(run.id))?.run.status).toBe('failed')
  })

  it('emits stepComplete and runComplete with the early completion output', async () => {
    const neverRuns = vi.fn(async () => ({ unexpected: true }))
    const wf = createWorkflow({ name: 'stream-early-complete', input: z.object({}) })
      .step('check', async ({ complete }) => complete({ done: true }))
      .step('after', neverRuns)
    const engine = createEngine({ storage, workflows: [wf] })
    const stream = engine.stream()

    await engine.enqueue('stream-early-complete', {})
    await engine.tick()

    const events: EngineEvent[] = []
    for await (const event of stream) {
      events.push(event)
      if (event.type === 'runComplete') break
    }

    expect(events.map((event) => event.type)).toEqual([
      'runStart',
      'stepStart',
      'stepComplete',
      'runComplete',
    ])
    expect(events.find((event) => event.type === 'stepComplete')?.output).toEqual({ done: true })
    expect(events.find((event) => event.type === 'runComplete')?.output).toEqual({ done: true })
    expect(neverRuns).not.toHaveBeenCalled()
  })

  it('emits all parallel starts before completions and returns the merged output', async () => {
    const wf = createWorkflow({ name: 'stream-parallel', input: z.object({}) })
      .parallel({
        a: async () => ({ value: 'a' }),
        b: async () => ({ value: 'b' }),
      })
    const engine = createEngine({ storage, workflows: [wf] })
    const stream = engine.stream()

    await engine.enqueue('stream-parallel', {})
    await engine.tick()

    const events: EngineEvent[] = []
    for await (const event of stream) {
      events.push(event)
      if (event.type === 'runComplete') break
    }

    const eventTypes = events.map((event) => event.type)
    const firstCompletion = eventTypes.indexOf('stepComplete')
    const lastStart = eventTypes.lastIndexOf('stepStart')
    expect(lastStart).toBeLessThan(firstCompletion)
    expect(events.filter((event) => event.type === 'stepStart')).toHaveLength(2)
    expect(events.filter((event) => event.type === 'stepComplete')).toHaveLength(2)
    expect(events.find((event) => event.type === 'runComplete')?.output).toEqual({
      a: { value: 'a' },
      b: { value: 'b' },
    })
  })

  it('leaves a run reclaimable (not failed) when stopped while paused on backpressure', async () => {
    const wf = createWorkflow({ name: 'stop-bp', input: z.object({}) })
      .step('a', async () => ({ x: 1 }))
      .step('b', async () => ({ y: 2 }))

    const engine = createEngine({ storage, workflows: [wf] })
    // A 1-event buffer with no consumer pauses the engine mid-run on backpressure.
    const stream = engine.stream({ bufferSize: 1 })
    void stream

    const run = await engine.enqueue('stop-bp', {})
    await engine.start(10_000)
    await delay(50)
    await engine.stop()

    // A graceful stop must not mark the paused run failed — it stays 'running'
    // so a future engine can reclaim it via stale-lease recovery.
    const info = await engine.getRunStatus(run.id)
    expect(info?.run.status).toBe('running')
  })

  it('cancels (not fails) a run paused on backpressure', async () => {
    const wf = createWorkflow({ name: 'cancel-bp', input: z.object({}) })
      .step('a', async () => ({ x: 1 }))
      .step('b', async () => ({ y: 2 }))

    const engine = createEngine({ storage, workflows: [wf] })
    const stream = engine.stream({ bufferSize: 1 })
    void stream

    const run = await engine.enqueue('cancel-bp', {})
    await engine.start(10_000)
    await delay(50)

    expect(await engine.cancel(run.id)).toBe(true)
    await engine.stop() // closes the stream, unblocking the paused producer

    const info = await engine.getRunStatus(run.id)
    expect(info?.run.status).toBe('cancelled')
  })

  it('ends open streams when the engine stops', async () => {
    const wf = createWorkflow({ name: 'stop-stream', input: z.object({}) })
      .step('a', async () => ({ x: 1 }))

    const engine = createEngine({ storage, workflows: [wf] })
    await engine.start(10_000)
    const stream = engine.stream()

    const drained: EngineEvent[] = []
    const consumer = (async () => {
      for await (const event of stream) {
        drained.push(event)
      }
    })()

    await engine.stop()
    // The consumer loop must terminate once the engine stops.
    await settlesWithin(consumer)
  })

  it('resolves a pending next() as done when the stream is disposed', async () => {
    const wf = createWorkflow({ name: 'pending-next', input: z.object({}) })
      .step('a', async () => ({ x: 1 }))

    const engine = createEngine({ storage, workflows: [wf] })
    const stream = engine.stream()

    // next() on an empty buffer parks a consumer waiter; disposing must release it.
    const pending = stream.next()
    await stream.return?.()

    await expect(pending).resolves.toEqual({ value: undefined, done: true })
  })

  it('resolves concurrent next() calls in event order', async () => {
    const wf = createWorkflow({ name: 'concurrent-pulls', input: z.object({}) })
      .step('a', async () => ({ ok: true }))
    const engine = createEngine({ storage, workflows: [wf] })
    const stream = engine.stream()

    const first = stream.next()
    const second = stream.next()
    const third = stream.next()
    await engine.enqueue('concurrent-pulls', {})
    const tickPromise = engine.tick()

    expect((await first).value.type).toBe('runStart')
    expect((await second).value.type).toBe('stepStart')
    expect((await third).value.type).toBe('stepComplete')

    await stream.return?.()
    await tickPromise
  })

  it('return() is terminal and discards buffered events', async () => {
    const wf = createWorkflow({ name: 'terminal-return', input: z.object({}) })
      .step('a', async () => ({ ok: true }))
    const engine = createEngine({ storage, workflows: [wf] })
    const stream = engine.stream()

    await engine.enqueue('terminal-return', {})
    await engine.tick()
    await stream.return?.()

    await expect(stream.next()).resolves.toEqual({ value: undefined, done: true })
    await expect(stream.next()).resolves.toEqual({ value: undefined, done: true })
  })

  it('throw() is terminal, preserves the thrown value, and unblocks producers', async () => {
    const wf = createWorkflow({ name: 'terminal-throw', input: z.object({}) })
      .step('a', async () => ({ ok: true }))
    const engine = createEngine({ storage, workflows: [wf] })
    const stream = engine.stream({ bufferSize: 1 })
    const thrown = { reason: 'consumer failed' }

    await engine.enqueue('terminal-throw', {})
    const tickPromise = engine.tick()
    await delay(10)

    await expect(stream.throw?.(thrown)).rejects.toBe(thrown)
    await settlesWithin(tickPromise)
    await expect(stream.next()).resolves.toEqual({ value: undefined, done: true })
  })

  it('disposal is idempotent', async () => {
    const wf = createWorkflow({ name: 'idempotent-dispose', input: z.object({}) })
      .step('a', async () => ({ ok: true }))
    const engine = createEngine({ storage, workflows: [wf] })
    const stream = engine.stream()

    await stream.return?.()
    await stream.return?.()
    await stream[Symbol.asyncDispose]()

    await expect(stream.next()).resolves.toEqual({ value: undefined, done: true })
  })

  it('allows a new stream after stop() closed previous subscriptions', async () => {
    const wf = createWorkflow({ name: 'stream-after-stop', input: z.object({}) })
      .step('a', async () => ({ ok: true }))
    const engine = createEngine({ storage, workflows: [wf] })
    const oldStream = engine.stream()

    await engine.stop()
    await expect(oldStream.next()).resolves.toEqual({ value: undefined, done: true })

    const newStream = engine.stream()
    await engine.enqueue('stream-after-stop', {})
    await engine.tick()

    const types: string[] = []
    for await (const event of newStream) {
      types.push(event.type)
      if (event.type === 'runComplete') break
    }
    expect(types).toEqual(['runStart', 'stepStart', 'stepComplete', 'runComplete'])
  })

  it('supports await using for disposal', async () => {
    const wf = createWorkflow({ name: 'dispose', input: z.object({}) })
      .step('a', async () => ({ x: 1 }))

    const engine = createEngine({ storage, workflows: [wf] })

    let observed: string | undefined
    {
      await using stream = engine.stream()
      await engine.enqueue('dispose', {})
      await engine.tick()
      for await (const event of stream) {
        if (event.type === 'runComplete') {
          observed = event.workflow
          break
        }
      }
    }

    expect(observed).toBe('dispose')

    // After disposal, a new run completes without the old stream blocking it.
    const run = await engine.enqueue('dispose', {})
    await engine.tick()
    const info = await engine.getRunStatus(run.id)
    expect(info?.run.status).toBe('completed')
  })
})
