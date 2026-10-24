import { randomUUID } from 'node:crypto'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { rmSync } from 'node:fs'
import { describe, it, expect, afterEach } from 'vitest'
import { z } from 'zod'
import { createWorkflow } from '../workflow'
import { createEngine } from '../engine'
import { ConfigError } from '../errors'
import { MemoryStorage } from '../../storage/memory'
import { SQLiteStorage } from '../../storage/sqlite-node'
import type { StorageAdapter } from '../types'

const files: string[] = []
afterEach(() => {
  for (const file of files.splice(0)) rmSync(file, { force: true })
})

const adapters: Array<[string, () => StorageAdapter]> = [
  ['memory', () => new MemoryStorage()],
  ['sqlite', () => {
    const file = join(tmpdir(), `reflow-delay-${randomUUID()}.db`)
    files.push(file)
    return new SQLiteStorage(file)
  }],
]

function workflow(started: number[]) {
  return createWorkflow({ name: 'reminder', input: z.object({ to: z.string() }) })
    .step('send', async () => {
      started.push(Date.now())
      return { sent: true }
    })
}

describe.each(adapters)('delayed start (%s)', (_, makeStorage) => {
  it('holds a run enqueued with a delay until it is due, then runs it', async () => {
    const started: number[] = []
    const storage = makeStorage()
    await storage.initialize()
    const engine = createEngine({ storage, workflows: [workflow(started)] })
    await engine.start(1000)

    const enqueuedAt = Date.now()
    const run = await engine.enqueue('reminder', { to: 'ada' }, { delay: 80 })
    expect([run.status, run.wakeAt !== null && run.wakeAt >= enqueuedAt + 80, run.startedAt]).toStrictEqual(['sleeping', true, null])

    await new Promise((resolve) => setTimeout(resolve, 30))
    expect(started).toStrictEqual([])

    // The poll interval is a second: the engine's own wake-up starts it on time.
    const result = await engine.waitForRun(run.id)
    await engine.stop()
    expect(result.status).toBe('completed')
    expect(started.length).toBe(1)
    expect((started[0] ?? 0) - enqueuedAt).toBeGreaterThanOrEqual(80)
    expect((started[0] ?? 0) - enqueuedAt).toBeLessThan(800)
  })

  it('accepts a duration string and a start time', async () => {
    const storage = makeStorage()
    await storage.initialize()
    const engine = createEngine({ storage, workflows: [workflow([])] })
    const now = Date.now()

    const inTen = await engine.enqueue('reminder', { to: 'a' }, { delay: '10m' })
    const atNoon = await engine.enqueue('reminder', { to: 'b' }, { startAt: new Date(now + 3_600_000) })
    const atMs = await engine.enqueue('reminder', { to: 'c' }, { startAt: now + 5_000 })

    expect([inTen.status, (inTen.wakeAt ?? 0) - now >= 600_000 && (inTen.wakeAt ?? 0) - now < 601_000]).toStrictEqual(['sleeping', true])
    expect([atNoon.status, atNoon.wakeAt]).toStrictEqual(['sleeping', now + 3_600_000])
    expect([atMs.status, atMs.wakeAt]).toStrictEqual(['sleeping', now + 5_000])
    // Not due, so a tick starts none of them.
    await engine.tick()
    const statuses = await Promise.all([inTen, atNoon, atMs].map(async (run) => (await storage.getRun(run.id))?.status))
    expect(statuses).toStrictEqual(['sleeping', 'sleeping', 'sleeping'])
  })

  it('starts a run straight away when its start time has passed or its delay is zero', async () => {
    const storage = makeStorage()
    await storage.initialize()
    const engine = createEngine({ storage, workflows: [workflow([])] })

    const past = await engine.enqueue('reminder', { to: 'a' }, { startAt: Date.now() - 1000 })
    const zero = await engine.enqueue('reminder', { to: 'b' }, { delay: 0 })

    expect([past.status, past.wakeAt, zero.status, zero.wakeAt]).toStrictEqual(['pending', null, 'pending', null])
  })

  it('returns the existing delayed run for a repeated idempotency key', async () => {
    const storage = makeStorage()
    await storage.initialize()
    const engine = createEngine({ storage, workflows: [workflow([])] })

    const first = await engine.enqueue('reminder', { to: 'a' }, { delay: '1h', idempotencyKey: 'remind-a' })
    const again = await engine.enqueue('reminder', { to: 'a' }, { delay: '2h', idempotencyKey: 'remind-a' })

    expect([again.id, again.wakeAt]).toStrictEqual([first.id, first.wakeAt])
  })
})

describe('delayed start options', () => {
  const engine = createEngine({ storage: new MemoryStorage(), workflows: [workflow([])] })

  it.each([
    ['both delay and startAt', { delay: 10, startAt: Date.now() }, 'enqueue: pass delay or startAt, not both'],
    ['an invalid Date', { startAt: new Date('nope') }, 'enqueue: startAt must be a valid Date or epoch milliseconds'],
    ['a non-finite startAt', { startAt: Number.NaN }, 'enqueue: startAt must be a valid Date or epoch milliseconds'],
    ['a negative delay', { delay: -1 }, 'Invalid duration: -1ms must be a non-negative, finite number'],
  ])('rejects %s', async (_, options, message) => {
    await expect(engine.enqueue('reminder', { to: 'a' }, options)).rejects.toThrow(new ConfigError(message))
    await expect(engine.enqueue('reminder', { to: 'a' }, options)).rejects.toBeInstanceOf(ConfigError)
  })
})
