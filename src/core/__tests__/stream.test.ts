import { describe, it, expect, beforeEach } from 'vitest'
import { z } from 'zod'
import { createWorkflow } from '../workflow'
import { createEngine } from '../engine'
import type { EngineEvent } from '../engine'
import { MemoryStorage } from '../../storage/memory'

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
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
})

describe('engine.stream()', () => {
  let storage: MemoryStorage

  beforeEach(async () => {
    storage = new MemoryStorage()
    await storage.initialize()
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
    await Promise.race([
      engine.tick(),
      delay(500).then(() => {
        throw new Error('tick() hung — disposed stream still applied backpressure')
      }),
    ])

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
    await Promise.race([
      consumer,
      delay(500).then(() => {
        throw new Error('stream did not close on engine stop')
      }),
    ])
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
