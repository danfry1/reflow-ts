import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { z } from 'zod'
import {
  createWorkflow,
  createEngine,
  ConfigError,
  ValidationError,
  WorkflowNotFoundError,
} from '../../index'
import type { WorkflowRun } from '../../index'
import { MemoryStorage } from '../../storage/memory'
import { canonicalizePersistedValue } from '../../storage/codec'
import { nextOccurrence } from '../schedule-timing'
import { at, delegatingAdapter } from '../../__tests__/helpers'

const HOUR = 60 * 60 * 1000

const cleanup = createWorkflow({ name: 'cleanup', input: z.object({ olderThanDays: z.number() }) })
  .step('purge', async () => ({ purged: true }))

const report = createWorkflow({ name: 'report', input: z.object({}) })
  .step('build', async () => ({ built: true }))

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

describe('durable schedules', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('does not fire before the first occurrence is due', async () => {
    const storage = new MemoryStorage()
    await storage.initialize()
    const created = trackCreatedRuns(storage)

    const engine = createEngine({ storage, workflows: [cleanup] })
    await engine.schedule('cleanup', { olderThanDays: 30 }, HOUR)

    await engine.tick()

    expect(created).toHaveLength(0)
  })

  it('fires once the occurrence comes due', async () => {
    const storage = new MemoryStorage()
    await storage.initialize()
    const created = trackCreatedRuns(storage)

    const engine = createEngine({ storage, workflows: [cleanup] })
    await engine.schedule('cleanup', { olderThanDays: 30 }, HOUR)

    vi.advanceTimersByTime(HOUR)
    await engine.tick()

    expect(created).toHaveLength(1)
    expect(at(created, 0).input).toStrictEqual({ olderThanDays: 30 })
  })

  it('survives a restart — a fresh engine picks up the registered schedule', async () => {
    // The whole point of making schedules durable: the process that registered
    // the schedule is gone, and the work still happens.
    const storage = new MemoryStorage()
    await storage.initialize()
    const created = trackCreatedRuns(storage)

    const before = createEngine({ storage, workflows: [cleanup] })
    await before.schedule('cleanup', { olderThanDays: 30 }, HOUR)
    await before.stop()

    vi.advanceTimersByTime(HOUR)

    // A new process, with no memory of the registration.
    const after = createEngine({ storage, workflows: [cleanup] })
    await after.tick()

    expect(created).toHaveLength(1)
  })

  it('fires an occurrence once even when several engines tick together', async () => {
    const storage = new MemoryStorage()
    await storage.initialize()
    const created = trackCreatedRuns(storage)

    const engines = [1, 2, 3].map(() => createEngine({ storage, workflows: [cleanup] }))
    await at(engines, 0).schedule('cleanup', { olderThanDays: 30 }, HOUR)

    vi.advanceTimersByTime(HOUR)
    await Promise.all(engines.map((engine) => engine.tick()))

    expect(created).toHaveLength(1)
  })

  it('skips missed occurrences instead of backfilling them', async () => {
    // A three-hour outage on an hourly schedule must not enqueue three runs at
    // once — that turns an outage into a thundering herd.
    const storage = new MemoryStorage()
    await storage.initialize()
    const created = trackCreatedRuns(storage)

    const engine = createEngine({ storage, workflows: [cleanup] })
    await engine.schedule('cleanup', { olderThanDays: 30 }, HOUR)

    vi.advanceTimersByTime(5 * HOUR)
    await engine.tick()

    expect(created).toHaveLength(1)

    // ...and the cadence resumes rather than firing again immediately.
    await engine.tick()
    expect(created).toHaveLength(1)

    vi.advanceTimersByTime(HOUR)
    await engine.tick()
    expect(created).toHaveLength(2)
  })

  it('keeps the existing cadence when a schedule is re-registered unchanged', async () => {
    // Every instance registers at startup, and deploys are frequent. If
    // re-registering reset the clock, a schedule could be starved indefinitely.
    const storage = new MemoryStorage()
    await storage.initialize()
    const created = trackCreatedRuns(storage)

    const engine = createEngine({ storage, workflows: [cleanup] })
    await engine.schedule('cleanup', { olderThanDays: 30 }, HOUR)

    // Redeploy repeatedly, each time just before the schedule would fire.
    for (let restart = 0; restart < 4; restart++) {
      vi.advanceTimersByTime(HOUR / 5)
      await createEngine({ storage, workflows: [cleanup] }).schedule(
        'cleanup',
        { olderThanDays: 30 },
        HOUR,
      )
    }

    vi.advanceTimersByTime(HOUR / 5)
    await engine.tick()

    expect(created).toHaveLength(1)
  })

  it('resets the next firing when the interval changes', async () => {
    const storage = new MemoryStorage()
    await storage.initialize()

    const engine = createEngine({ storage, workflows: [cleanup] })
    const key = await engine.schedule('cleanup', { olderThanDays: 30 }, HOUR, { key: 'nightly' })

    vi.advanceTimersByTime(HOUR / 2)
    await engine.schedule('cleanup', { olderThanDays: 30 }, 2 * HOUR, { key: 'nightly' })

    const schedules = await engine.listSchedules()
    expect(schedules).toHaveLength(1)
    expect(at(schedules, 0).key).toBe(key)
    expect(at(schedules, 0).recurrence).toStrictEqual({ kind: 'interval', intervalMs: 2 * HOUR })
    expect(at(schedules, 0).nextRunAt).toBe(Date.now() + 2 * HOUR)
  })

  it('keeps schedules with different inputs independent', async () => {
    const storage = new MemoryStorage()
    await storage.initialize()
    const created = trackCreatedRuns(storage)

    const engine = createEngine({ storage, workflows: [cleanup] })
    await engine.schedule('cleanup', { olderThanDays: 30 }, HOUR)
    await engine.schedule('cleanup', { olderThanDays: 90 }, HOUR)

    vi.advanceTimersByTime(HOUR)
    await engine.tick()

    expect(created).toHaveLength(2)
    expect(created.map((run) => run.input)).toStrictEqual(
      expect.arrayContaining([{ olderThanDays: 30 }, { olderThanDays: 90 }]),
    )
  })

  it('treats a shared explicit key as one schedule', async () => {
    const storage = new MemoryStorage()
    await storage.initialize()

    const a = createEngine({ storage, workflows: [cleanup] })
    const b = createEngine({ storage, workflows: [cleanup] })

    await a.schedule('cleanup', { olderThanDays: 30 }, HOUR, { key: 'nightly-cleanup' })
    await b.schedule('cleanup', { olderThanDays: 90 }, HOUR, { key: 'nightly-cleanup' })

    // Same key means one schedule; the later registration wins on input.
    const schedules = await a.listSchedules()
    expect(schedules).toHaveLength(1)
    expect(at(schedules, 0).input).toStrictEqual({ olderThanDays: 90 })
  })

  it('leaves schedules for workflows this engine does not run alone', async () => {
    // A fleet where workers register different workflows: claiming advances the
    // schedule, so claiming one you cannot run would swallow that firing.
    const storage = new MemoryStorage()
    await storage.initialize()
    const created = trackCreatedRuns(storage)

    const both = createEngine({ storage, workflows: [cleanup, report] })
    await both.schedule('report', {}, HOUR)

    const cleanupOnly = createEngine({ storage, workflows: [cleanup] })

    vi.advanceTimersByTime(HOUR)
    await cleanupOnly.tick()
    expect(created).toHaveLength(0)

    // The occurrence is still pending for a worker that can serve it.
    await both.tick()
    expect(created).toHaveLength(1)
    expect(at(created, 0).workflow).toBe('report')
  })

  it('stops firing once unscheduled, for every instance', async () => {
    const storage = new MemoryStorage()
    await storage.initialize()
    const created = trackCreatedRuns(storage)

    const a = createEngine({ storage, workflows: [cleanup] })
    const b = createEngine({ storage, workflows: [cleanup] })
    const key = await a.schedule('cleanup', { olderThanDays: 30 }, HOUR)

    expect(await a.unschedule(key)).toBe(true)
    expect(await a.unschedule(key)).toBe(false)

    vi.advanceTimersByTime(3 * HOUR)
    await a.tick()
    await b.tick()

    expect(created).toHaveLength(0)
    expect(await b.listSchedules()).toStrictEqual([])
  })

  it('keeps schedules registered across engine.stop()', async () => {
    // Schedules belong to the storage, not the instance: stopping one worker
    // must not cancel a schedule the rest of the fleet is still serving.
    const storage = new MemoryStorage()
    await storage.initialize()

    const engine = createEngine({ storage, workflows: [cleanup] })
    await engine.schedule('cleanup', { olderThanDays: 30 }, HOUR)
    await engine.stop()

    expect(await engine.listSchedules()).toHaveLength(1)
  })

  it('reports a failing schedule without stopping the others', async () => {
    const onError = vi.fn()
    const delegate = new MemoryStorage()
    await delegate.initialize()
    const created: WorkflowRun[] = []

    // Injected through the adapter rather than a spy: `trackCreatedRuns`
    // already replaces `createRun`, and stacking a second spy on the same
    // method makes the two wrappers fight over which one delegates.
    const storage = delegatingAdapter(delegate, {
      createRun: async (run) => {
        if (run.workflow === 'cleanup') {
          throw new Error('storage exploded')
        }
        const result = await delegate.createRun(run)
        if (result.created) created.push(result.run)
        return result
      },
    })

    const engine = createEngine({ storage, workflows: [cleanup, report], hooks: { onError } })
    await engine.schedule('cleanup', { olderThanDays: 30 }, HOUR, { key: 'a-broken' })
    await engine.schedule('report', {}, HOUR, { key: 'b-fine' })

    vi.advanceTimersByTime(HOUR)
    await engine.tick()

    expect(onError).toHaveBeenCalled()
    expect(created.map((run) => run.workflow)).toStrictEqual(['report'])
  })

  it('keeps executing runs when the scheduler itself is failing', async () => {
    // Schedule processing is the first await in every tick. It is an auxiliary
    // responsibility, so a storage failure there must not stop the engine
    // claiming and running work, which is its primary job.
    const onError = vi.fn()
    const delegate = new MemoryStorage()
    await delegate.initialize()

    const storage = delegatingAdapter(delegate, {
      claimDueSchedule: () => Promise.reject(new Error('schedule table locked')),
    })

    const engine = createEngine({ storage, workflows: [cleanup], hooks: { onError } })
    const run = await engine.enqueue('cleanup', { olderThanDays: 30 })

    await engine.tick()

    expect((await engine.getRunStatus(run.id))?.run.status).toBe('completed')
    expect(onError).toHaveBeenCalled()
  })

  it('accepts a duration string as the interval', async () => {
    const storage = new MemoryStorage()
    await storage.initialize()
    const created = trackCreatedRuns(storage)

    const engine = createEngine({ storage, workflows: [cleanup] })
    await engine.schedule('cleanup', { olderThanDays: 30 }, { every: '1h' })

    vi.advanceTimersByTime(HOUR)
    await engine.tick()

    expect(created).toHaveLength(1)
  })

  it('fires a cron schedule at its next occurrence', async () => {
    vi.setSystemTime(Date.parse('2026-03-10T08:00:00Z'))
    const storage = new MemoryStorage()
    await storage.initialize()
    const created = trackCreatedRuns(storage)

    const engine = createEngine({ storage, workflows: [cleanup] })
    await engine.schedule('cleanup', { olderThanDays: 30 }, { cron: '0 9 * * *' })

    // 08:30 — not yet due.
    vi.setSystemTime(Date.parse('2026-03-10T08:30:00Z'))
    await engine.tick()
    expect(created).toHaveLength(0)

    // 09:00 — due.
    vi.setSystemTime(Date.parse('2026-03-10T09:00:00Z'))
    await engine.tick()
    expect(created).toHaveLength(1)
  })

  it('advances a cron schedule to its next occurrence, not by a fixed gap', async () => {
    // Weekdays at 09:00: from Friday the next firing is Monday, three days on.
    vi.setSystemTime(Date.parse('2026-03-13T08:00:00Z'))
    const storage = new MemoryStorage()
    await storage.initialize()

    const engine = createEngine({ storage, workflows: [cleanup] })
    await engine.schedule('cleanup', { olderThanDays: 30 }, { cron: '0 9 * * 1-5' })

    vi.setSystemTime(Date.parse('2026-03-13T09:00:00Z'))
    await engine.tick()

    expect(at(await engine.listSchedules(), 0).nextRunAt).toBe(Date.parse('2026-03-16T09:00:00Z'))
  })

  it('stores the cron expression as its recurrence', async () => {
    const storage = new MemoryStorage()
    await storage.initialize()

    const engine = createEngine({ storage, workflows: [cleanup] })
    await engine.schedule('cleanup', { olderThanDays: 30 }, { cron: '@daily' })

    expect(at(await engine.listSchedules(), 0).recurrence)
      .toStrictEqual({ kind: 'cron', expression: '@daily' })
  })

  it('gives interval and cron schedules of the same workflow distinct keys', async () => {
    const storage = new MemoryStorage()
    await storage.initialize()

    const engine = createEngine({ storage, workflows: [cleanup] })
    const a = await engine.schedule('cleanup', { olderThanDays: 30 }, HOUR)
    const b = await engine.schedule('cleanup', { olderThanDays: 30 }, { cron: '0 * * * *' })

    expect(a).not.toBe(b)
    expect(await engine.listSchedules()).toHaveLength(2)
  })

  it('rejects a malformed cron expression at registration', async () => {
    // Registration is the last point this can reach a caller; after it the
    // schedule fires unattended.
    const engine = createEngine({ storage: new MemoryStorage(), workflows: [cleanup] })

    await expect(engine.schedule('cleanup', { olderThanDays: 1 }, { cron: 'not a cron' }))
      .rejects.toThrow(ConfigError)
    await expect(engine.schedule('cleanup', { olderThanDays: 1 }, { cron: '99 * * * *' }))
      .rejects.toThrow(/out of range/)
  })

  it('rejects a cron expression that can never occur', async () => {
    const engine = createEngine({ storage: new MemoryStorage(), workflows: [cleanup] })

    // 30 February. Caught at registration rather than on the first tick.
    await expect(engine.schedule('cleanup', { olderThanDays: 1 }, { cron: '0 0 30 2 *' }))
      .rejects.toThrow(ConfigError)
  })

  it('rejects a non-positive interval', async () => {
    const engine = createEngine({ storage: new MemoryStorage(), workflows: [cleanup] })

    await expect(engine.schedule('cleanup', { olderThanDays: 1 }, 0)).rejects.toThrow(ConfigError)
    await expect(engine.schedule('cleanup', { olderThanDays: 1 }, -1)).rejects.toThrow(
      /intervalMs must be a positive number/,
    )
  })

  it('rejects an unregistered workflow name', async () => {
    const engine = createEngine({ storage: new MemoryStorage(), workflows: [cleanup] })

    await expect(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (engine as any).schedule('unknown', {}, HOUR),
    ).rejects.toThrow(WorkflowNotFoundError)
  })

  it('validates the input against the workflow schema at registration', async () => {
    // Registering is the last point a bad input can be reported to a caller —
    // after this the schedule fires unattended.
    const engine = createEngine({ storage: new MemoryStorage(), workflows: [cleanup] })

    await expect(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      engine.schedule('cleanup', { olderThanDays: 'soon' } as any, HOUR),
    ).rejects.toThrow(ValidationError)
  })
})

describe('nextOccurrence', () => {
  it('leaves a future occurrence untouched', () => {
    expect(nextOccurrence(1_000, { kind: 'interval', intervalMs: 100 }, 500)).toBe(1_000)
  })

  it('advances past now by whole intervals', () => {
    expect(nextOccurrence(1_000, { kind: 'interval', intervalMs: 100 }, 1_000)).toBe(1_100)
    expect(nextOccurrence(1_000, { kind: 'interval', intervalMs: 100 }, 1_050)).toBe(1_100)
    expect(nextOccurrence(1_000, { kind: 'interval', intervalMs: 100 }, 1_100)).toBe(1_200)
  })

  it('skips a long outage in one step rather than stepping through it', () => {
    // A year of missed one-second firings must not cost a loop of 31 million.
    expect(nextOccurrence(0, { kind: 'interval', intervalMs: 1_000 }, 365 * 24 * 60 * 60 * 1_000)).toBe(31_536_001_000)
  })

  it('rejects an interval that would produce NaN rather than writing it', () => {
    // A NaN next_run_at compares false against every clock, so the schedule
    // would stop firing permanently and silently.
    for (const bad of [0, -1, Number.NaN, Number.POSITIVE_INFINITY]) {
      expect(() => nextOccurrence(0, { kind: 'interval', intervalMs: bad }, 1_000)).toThrow(ConfigError)
    }
  })

  it('always returns a time strictly after now', () => {
    for (const now of [0, 1, 99, 100, 101, 12_345]) {
      expect(nextOccurrence(0, { kind: 'interval', intervalMs: 100 }, now)).toBeGreaterThan(now)
    }
  })
})

describe('canonicalizePersistedValue', () => {
  it('is independent of object key order', () => {
    expect(canonicalizePersistedValue({ a: 1, b: { c: 2, d: 3 } }, 'test'))
      .toBe(canonicalizePersistedValue({ b: { d: 3, c: 2 }, a: 1 }, 'test'))
  })

  it('still distinguishes different values', () => {
    expect(canonicalizePersistedValue({ a: 1 }, 'test'))
      .not.toBe(canonicalizePersistedValue({ a: 2 }, 'test'))
  })

  it('does not conflate a key order swap with a value swap', () => {
    expect(canonicalizePersistedValue({ a: 1, b: 2 }, 'test'))
      .not.toBe(canonicalizePersistedValue({ a: 2, b: 1 }, 'test'))
  })

  it('preserves array order, which is significant', () => {
    expect(canonicalizePersistedValue([1, 2], 'test'))
      .not.toBe(canonicalizePersistedValue([2, 1], 'test'))
  })
})
