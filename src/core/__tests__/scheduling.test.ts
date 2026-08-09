import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { z } from 'zod'
import { createWorkflow, createEngine } from '../../index'
import { MemoryStorage } from '../../storage/memory'
import { canonicalizePersistedValue } from '../../storage/codec'
import type { WorkflowRun } from '../../index'

/**
 * Record the runs a storage actually creates, ignoring idempotent hits.
 *
 * Counting creations rather than enqueue calls is the point: deduplication
 * happens inside `createRun`, so this measures the real work produced.
 */
function trackCreatedRuns(storage: MemoryStorage): WorkflowRun[] {
  const created: WorkflowRun[] = []
  const original = storage.createRun.bind(storage)

  vi.spyOn(storage, 'createRun').mockImplementation(async (run) => {
    const result = await original(run)
    if (result.created) created.push(result.run)
    return result
  })

  return created
}

const cleanup = createWorkflow({ name: 'cleanup', input: z.object({ olderThanDays: z.number() }) })
  .step('purge', async () => ({ purged: true }))

describe('scheduling', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('produces one run per interval when the same schedule runs on several engines', async () => {
    // The library documents running multiple engine instances against one
    // store, so a schedule registered on each must not multiply the work.
    const storage = new MemoryStorage()
    await storage.initialize()

    const created = trackCreatedRuns(storage)
    const engines = [1, 2, 3].map(() => createEngine({ storage, workflows: [cleanup] }))
    for (const engine of engines) {
      engine.schedule('cleanup', { olderThanDays: 30 }, 60_000)
    }

    await vi.advanceTimersByTimeAsync(60_000)

    expect(created).toHaveLength(1)

    for (const engine of engines) {
      await engine.stop()
    }
  })

  it('produces a distinct run for each successive interval', async () => {
    const storage = new MemoryStorage()
    await storage.initialize()

    const created = trackCreatedRuns(storage)
    const engine = createEngine({ storage, workflows: [cleanup] })
    engine.schedule('cleanup', { olderThanDays: 30 }, 60_000)

    await vi.advanceTimersByTimeAsync(180_000)

    expect(created).toHaveLength(3)

    await engine.stop()
  })

  it('keeps schedules with different inputs independent', async () => {
    const storage = new MemoryStorage()
    await storage.initialize()

    const created = trackCreatedRuns(storage)
    const engine = createEngine({ storage, workflows: [cleanup] })
    engine.schedule('cleanup', { olderThanDays: 30 }, 60_000)
    engine.schedule('cleanup', { olderThanDays: 90 }, 60_000)

    await vi.advanceTimersByTimeAsync(60_000)

    expect(created).toHaveLength(2)
    expect(created.map((run) => run.input)).toStrictEqual(
      expect.arrayContaining([{ olderThanDays: 30 }, { olderThanDays: 90 }]),
    )

    await engine.stop()
  })

  it('treats a shared explicit key as one schedule across engines', async () => {
    const storage = new MemoryStorage()
    await storage.initialize()

    const created = trackCreatedRuns(storage)
    const a = createEngine({ storage, workflows: [cleanup] })
    const b = createEngine({ storage, workflows: [cleanup] })

    a.schedule('cleanup', { olderThanDays: 30 }, 60_000, { key: 'nightly-cleanup' })
    b.schedule('cleanup', { olderThanDays: 30 }, 60_000, { key: 'nightly-cleanup' })

    await vi.advanceTimersByTimeAsync(60_000)

    expect(created).toHaveLength(1)

    await a.stop()
    await b.stop()
  })

  it('splits one workflow into separate schedules when given distinct keys', async () => {
    const storage = new MemoryStorage()
    await storage.initialize()

    const created = trackCreatedRuns(storage)
    const engine = createEngine({ storage, workflows: [cleanup] })
    engine.schedule('cleanup', { olderThanDays: 30 }, 60_000, { key: 'eu' })
    engine.schedule('cleanup', { olderThanDays: 30 }, 60_000, { key: 'us' })

    await vi.advanceTimersByTimeAsync(60_000)

    expect(created).toHaveLength(2)

    await engine.stop()
  })

  it('stops enqueuing once unscheduled', async () => {
    const storage = new MemoryStorage()
    await storage.initialize()

    const created = trackCreatedRuns(storage)
    const engine = createEngine({ storage, workflows: [cleanup] })
    const scheduleId = engine.schedule('cleanup', { olderThanDays: 30 }, 60_000)

    await vi.advanceTimersByTimeAsync(60_000)
    expect(engine.unschedule(scheduleId)).toBe(true)
    await vi.advanceTimersByTimeAsync(180_000)

    expect(created).toHaveLength(1)

    await engine.stop()
  })
})

describe('canonicalizePersistedValue', () => {
  it('is independent of object key order', () => {
    const left = canonicalizePersistedValue({ a: 1, b: { c: 2, d: 3 } }, 'test')
    const right = canonicalizePersistedValue({ b: { d: 3, c: 2 }, a: 1 }, 'test')

    expect(left).toBe(right)
  })

  it('still distinguishes different values', () => {
    const left = canonicalizePersistedValue({ a: 1 }, 'test')
    const right = canonicalizePersistedValue({ a: 2 }, 'test')

    expect(left).not.toBe(right)
  })

  it('does not conflate a key order swap with a value swap', () => {
    const left = canonicalizePersistedValue({ a: 1, b: 2 }, 'test')
    const right = canonicalizePersistedValue({ a: 2, b: 1 }, 'test')

    expect(left).not.toBe(right)
  })

  it('preserves array order, which is significant', () => {
    const left = canonicalizePersistedValue([1, 2], 'test')
    const right = canonicalizePersistedValue([2, 1], 'test')

    expect(left).not.toBe(right)
  })
})
