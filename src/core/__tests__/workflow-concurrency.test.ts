import { randomUUID } from 'node:crypto'
import { rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { describe, it, expect, afterEach } from 'vitest'
import { z } from 'zod'
import { createWorkflow } from '../workflow'
import { createEngine } from '../engine'
import { ConfigError } from '../errors'
import { MemoryStorage } from '../../storage/memory'
import { SQLiteStorage } from '../../storage/sqlite-node'
import { delegatingAdapter } from '../../__tests__/helpers'

const files: string[] = []
afterEach(() => {
  for (const file of files.splice(0)) {
    for (const suffix of ['', '-wal', '-shm']) rmSync(file + suffix, { force: true })
  }
})

/** A workflow whose one step takes a while, recording how many ran at once. */
function tracked(concurrency: number) {
  const seen = { now: 0, most: 0, done: 0 }
  const wf = createWorkflow({ name: 'call-api', input: z.object({ n: z.number() }), concurrency })
    .step('call', async () => {
      seen.now++
      seen.most = Math.max(seen.most, seen.now)
      await new Promise((resolve) => setTimeout(resolve, 40))
      seen.now--
      seen.done++
    })
  return { wf, seen }
}

describe('workflow concurrency', () => {
  it('runs no more of a workflow at once than its limit, though the engine has room for more', async () => {
    const { wf, seen } = tracked(2)
    const engine = createEngine({ storage: new MemoryStorage(), workflows: [wf], concurrency: 10 })
    await engine.start(5)
    const runs = await Promise.all([1, 2, 3, 4, 5, 6].map((n) => engine.enqueue('call-api', { n })))
    await Promise.all(runs.map((run) => engine.waitForRun(run.id)))
    await engine.stop()

    expect([seen.most, seen.done]).toStrictEqual([2, 6])
  })

  it('holds the limit across two engines sharing one SQLite file', async () => {
    const file = join(tmpdir(), `reflow-limit-${randomUUID()}.db`)
    files.push(file)
    const { wf, seen } = tracked(1)
    const first = createEngine({ storage: new SQLiteStorage(file), workflows: [wf], concurrency: 5 })
    const second = createEngine({ storage: new SQLiteStorage(file), workflows: [wf], concurrency: 5 })
    await first.start(5)
    await second.start(5)

    const runs = await Promise.all([1, 2, 3, 4].map((n) => first.enqueue('call-api', { n })))
    await Promise.all(runs.map((run) => first.waitForRun(run.id, { pollIntervalMs: 10 })))
    await Promise.all([first.stop(), second.stop()])

    expect([seen.most, seen.done]).toStrictEqual([1, 4])
  })

  it('does not hold back other workflows while one is at its limit', async () => {
    const { wf, seen } = tracked(1)
    const other = createWorkflow({ name: 'other', input: z.object({}) }).step('go', async () => ({ ok: true }))
    const engine = createEngine({ storage: new MemoryStorage(), workflows: [wf, other], concurrency: 4 })
    await engine.start(5)

    await engine.enqueue('call-api', { n: 1 })
    await engine.enqueue('call-api', { n: 2 })
    const quick = await engine.enqueue('other', {})
    const result = await engine.waitForRun(quick.id)
    const stillRunning = seen.done < 2
    await engine.stop()

    expect([result.status, stillRunning]).toStrictEqual(['completed', true])
  })

  it.each([0, -1, 1.5, Number.NaN])('rejects a concurrency of %s', (concurrency) => {
    const define = () => createWorkflow({ name: 'bad', input: z.object({}), concurrency })
    expect(define).toThrow(new ConfigError('Workflow "bad" concurrency must be a positive integer'))
    expect(define).toThrow(ConfigError)
  })

  it('refuses storage that cannot enforce the limit', () => {
    const { wf } = tracked(1)
    const storage = delegatingAdapter(new MemoryStorage(), { supportsConcurrencyLimits: false })
    const create = () => createEngine({ storage, workflows: [wf] })
    expect(create).toThrow(new ConfigError('Workflow "call-api" sets concurrency, but this storage adapter does not enforce concurrency limits'))
    expect(create).toThrow(ConfigError)
  })
})
