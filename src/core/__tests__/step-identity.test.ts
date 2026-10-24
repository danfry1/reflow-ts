import { describe, it, expect, beforeEach } from 'vitest'
import { z } from 'zod'
import { createWorkflow } from '../workflow'
import { createEngine } from '../engine'
import { MemoryStorage } from '../../storage/memory'

interface Seen {
  step: string
  runId: string
  attempt: number
  idempotencyKey: string
}

describe('step identity: runId, attempt, idempotencyKey', () => {
  let storage: MemoryStorage

  beforeEach(() => {
    storage = new MemoryStorage()
  })

  it('gives each step its run id and a key unique to the run and step', async () => {
    const seen: Seen[] = []
    const wf = createWorkflow({ name: 'order', input: z.object({}) })
      .step('charge', async ({ runId, attempt, idempotencyKey }) => {
        seen.push({ step: 'charge', runId, attempt, idempotencyKey })
      })
      .step('ship', async ({ runId, attempt, idempotencyKey }) => {
        seen.push({ step: 'ship', runId, attempt, idempotencyKey })
      })

    const engine = createEngine({ storage, workflows: [wf] })
    const first = await engine.enqueue('order', {})
    const second = await engine.enqueue('order', {})
    await engine.tick()
    await engine.tick()

    expect([...seen].sort((a, b) => (a.runId + a.step).localeCompare(b.runId + b.step))).toStrictEqual(
      [first.id, second.id].sort().flatMap((runId) => [
        { step: 'charge', runId, attempt: 1, idempotencyKey: `${runId}:charge` },
        { step: 'ship', runId, attempt: 1, idempotencyKey: `${runId}:ship` },
      ]),
    )
  })

  it('keeps the key and counts the attempt across retries', async () => {
    const seen: Array<Pick<Seen, 'attempt' | 'idempotencyKey'>> = []
    const wf = createWorkflow({ name: 'flaky', input: z.object({}) })
      .step('charge', {
        retry: { maxAttempts: 3, backoff: 'linear', initialDelayMs: 0 },
        handler: async ({ attempt, idempotencyKey }) => {
          seen.push({ attempt, idempotencyKey })
          if (attempt < 3) throw new Error('declined')
        },
      })

    const engine = createEngine({ storage, workflows: [wf] })
    const run = await engine.enqueue('flaky', {})
    await engine.tick()

    const key = `${run.id}:charge`
    expect(seen).toStrictEqual([
      { attempt: 1, idempotencyKey: key },
      { attempt: 2, idempotencyKey: key },
      { attempt: 3, idempotencyKey: key },
    ])
  })

  it('keeps the attempt count across a durable backoff', async () => {
    const attempts: number[] = []
    const wf = createWorkflow({ name: 'backoff', input: z.object({}) })
      .step('charge', {
        retry: { maxAttempts: 2, backoff: 'linear', initialDelayMs: 20 },
        handler: async ({ attempt }) => {
          attempts.push(attempt)
          if (attempt === 1) throw new Error('declined')
        },
      })

    const engine = createEngine({ storage, workflows: [wf] })
    const run = await engine.enqueue('backoff', {})
    await engine.start(10)
    const result = await engine.waitForRun(run.id)
    await engine.stop()

    expect([result.status, attempts]).toStrictEqual(['completed', [1, 2]])
  })

  it('gives the same key to a step repeated by another process after a crash', async () => {
    const keys: string[] = []
    let release = (): void => {}
    const wf = createWorkflow({ name: 'crashy', input: z.object({}) })
      .step('charge', async ({ idempotencyKey, signal }) => {
        keys.push(idempotencyKey)
        if (keys.length === 1) {
          // The first process dies mid-step: this attempt never finishes.
          await new Promise<void>((resolve) => {
            release = resolve
            signal.addEventListener('abort', () => resolve(), { once: true })
          })
          throw new Error('process died')
        }
      })

    const dying = createEngine({ storage, workflows: [wf], runLeaseDurationMs: 60, heartbeatIntervalMs: 20 })
    const run = await dying.enqueue('crashy', {})
    await dying.start(10)
    while (keys.length === 0) await new Promise((resolve) => setTimeout(resolve, 5))
    await dying.stop()
    release()

    const next = createEngine({ storage, workflows: [wf], runLeaseDurationMs: 60, heartbeatIntervalMs: 20 })
    await next.start(10)
    const result = await next.waitForRun(run.id)
    await next.stop()

    expect([result.status, keys]).toStrictEqual(['completed', [`${run.id}:charge`, `${run.id}:charge`]])
  })

  it('gives each parallel branch its own key', async () => {
    const keys: string[] = []
    const wf = createWorkflow({ name: 'fanout', input: z.object({}) })
      .parallel({
        email: async ({ idempotencyKey }) => {
          keys.push(idempotencyKey)
        },
        sms: async ({ idempotencyKey }) => {
          keys.push(idempotencyKey)
        },
      })

    const engine = createEngine({ storage, workflows: [wf] })
    const run = await engine.enqueue('fanout', {})
    await engine.tick()

    expect([...keys].sort()).toStrictEqual([`${run.id}:email`, `${run.id}:sms`])
  })
})
