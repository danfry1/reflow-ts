import { randomUUID } from 'node:crypto'
import { afterAll, describe, expect, it } from 'vitest'
import pg from 'pg'
import { z } from 'zod'
import { createEngine } from '../../core/engine'
import { ConfigError } from '../../core/errors'
import { createWorkflow } from '../../core/workflow'
import { storageConformanceCases } from '../conformance'
import { PostgresStorage } from '../postgres'

/**
 * Runs against a real PostgreSQL named by REFLOW_TEST_POSTGRES_URL, and is
 * skipped without one. CI starts one as a service container; locally:
 *
 *   docker run -d -p 55432:5432 -e POSTGRES_PASSWORD=reflow -e POSTGRES_USER=reflow postgres:17
 *   REFLOW_TEST_POSTGRES_URL=postgres://reflow:reflow@localhost:55432/reflow bun run test
 */
const url = process.env.REFLOW_TEST_POSTGRES_URL

describe.skipIf(url === undefined)('PostgresStorage', () => {
  const pool = new pg.Pool({ connectionString: url, max: 20 })
  const schemas: string[] = []

  /** Storage in a schema of its own, so no test can observe another's rows. */
  async function fresh(): Promise<PostgresStorage> {
    const schema = `reflow_test_${randomUUID().replaceAll('-', '').slice(0, 16)}`
    schemas.push(schema)
    const storage = new PostgresStorage({ pool, schema })
    await storage.initialize()
    return storage
  }

  afterAll(async () => {
    for (const schema of schemas) await pool.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`)
    await pool.end()
  })

  describe('storage contract', () => {
    for (const testCase of storageConformanceCases) {
      it(testCase.name, async () => {
        const storage = await fresh()
        if (testCase.appliesTo && !testCase.appliesTo(storage)) return
        await testCase.run(storage)
      })
    }
  })

  it('initializes once when several processes start together, and again as a no-op', async () => {
    const schema = `reflow_test_${randomUUID().replaceAll('-', '').slice(0, 16)}`
    schemas.push(schema)
    await Promise.all([1, 2, 3, 4, 5].map(() => new PostgresStorage({ pool, schema }).initialize()))
    await new PostgresStorage({ pool, schema }).initialize()
    const { rows } = await pool.query<{ version: number }>(`SELECT version FROM ${schema}.reflow_schema`)
    expect(rows).toStrictEqual([{ version: 1 }])
  })

  it('refuses a database written by a newer version', async () => {
    const storage = await fresh()
    const schema = schemas.at(-1) ?? ''
    await pool.query(`UPDATE ${schema}.reflow_schema SET version = 99`)
    await expect(storage.initialize()).rejects.toThrow(
      new ConfigError('Database schema version 99 is newer than this version of reflow-ts supports (1). Upgrade reflow-ts to open it.'),
    )
  })

  it.each(['Reflow', 'reflow-x', '1abc', 'a; DROP TABLE x'])('refuses the schema name %j', (schema) => {
    expect(() => new PostgresStorage({ pool, schema })).toThrow(ConfigError)
  })

  it('creates its own pool from a connection string, and ends it', async () => {
    const schema = `reflow_test_${randomUUID().replaceAll('-', '').slice(0, 16)}`
    schemas.push(schema)
    const storage = new PostgresStorage({ connectionString: url ?? '', poolConfig: { max: 2 }, schema })
    await storage.initialize()
    expect(await storage.countRuns()).toStrictEqual({ pending: 0, running: 0, sleeping: 0, waiting: 0, completed: 0, failed: 0, cancelled: 0 })
    await storage.end()
    await expect(storage.getRun('x')).rejects.toThrow('Cannot use a pool after calling end on the pool')
  })

  it('leaves a pool it was given open when closed', async () => {
    const storage = await fresh()
    storage.close()
    await storage.end()
    expect(await storage.getRun('missing')).toBe(null)
  })

  it('answers the edge cases without querying for nothing', async () => {
    const storage = await fresh()
    expect(await storage.deliverEvent('missing', 'approved', { ok: true })).toBe(false)
    expect(await storage.getStepResultsForRuns([])).toStrictEqual(new Map())
    expect(await storage.deleteRuns({ finishedBefore: Date.now(), statuses: [], limit: 10 })).toBe(0)
    expect(await storage.deleteRuns({ finishedBefore: Date.now(), statuses: ['completed'], limit: 10 })).toBe(0)
  })

  it('gives each of many concurrent claims a different run', async () => {
    const storage = await fresh()
    const now = Date.now()
    for (let i = 0; i < 40; i++) {
      await storage.createRun({
        id: `run_${i}`, workflow: 'w', input: {}, idempotencyKey: null, status: 'pending',
        createdAt: now + i, updatedAt: now + i, startedAt: null, finishedAt: null,
        output: undefined, error: null, wakeAt: null, waitingFor: null,
      })
    }
    const claims = await Promise.all(Array.from({ length: 50 }, () => storage.claimNextRun(['w'])))
    const ids = claims.flatMap((claim) => (claim ? [claim.id] : []))
    expect([ids.length, new Set(ids).size]).toStrictEqual([40, 40])
  })

  it('holds a workflow concurrency limit under concurrent claims', async () => {
    const storage = await fresh()
    const now = Date.now()
    for (let i = 0; i < 20; i++) {
      await storage.createRun({
        id: `run_${i}`, workflow: 'w', input: {}, idempotencyKey: null, status: 'pending',
        createdAt: now + i, updatedAt: now + i, startedAt: null, finishedAt: null,
        output: undefined, error: null, wakeAt: null, waitingFor: null,
      })
    }
    const limits = new Map([['w', 3]])
    const claims = await Promise.all(Array.from({ length: 20 }, () => storage.claimNextRun(['w'], now - 30_000, limits)))
    expect(claims.filter((claim) => claim !== null).length).toBe(3)
  })

  it('runs workflows end to end across two engines sharing a limit', async () => {
    const storage = await fresh()
    const seen = { now: 0, most: 0 }
    const charges: string[] = []
    const order = createWorkflow({ name: 'order', input: z.object({ n: z.number() }), concurrency: 2 })
      .step('charge', async ({ idempotencyKey }) => {
        charges.push(idempotencyKey)
        seen.now++
        seen.most = Math.max(seen.most, seen.now)
        await new Promise((resolve) => setTimeout(resolve, 30))
        seen.now--
        return { charged: true }
      })
      .step('ship', async ({ prev }) => ({ shipped: prev.charged }))

    const first = createEngine({ storage, workflows: [order], concurrency: 5 })
    const second = createEngine({ storage: new PostgresStorage({ pool, schema: schemas.at(-1) ?? '' }), workflows: [order], concurrency: 5 })
    await first.start(10)
    await second.start(10)
    const runs = await Promise.all([1, 2, 3, 4, 5, 6].map((n) => first.enqueue('order', { n })))
    const results = await Promise.all(runs.map((run) => first.waitForRun(run.id, { pollIntervalMs: 20 })))
    await Promise.all([first.stop(), second.stop()])

    expect(results.map((result) => [result.status, result.output])).toStrictEqual(
      runs.map(() => ['completed', { shipped: true }]),
    )
    expect(seen.most).toBe(2)
    expect(new Set(charges).size).toBe(6)
  })

  it('picks up a run another process was in the middle of when it died', async () => {
    const storage = await fresh()
    const schema = schemas.at(-1) ?? ''
    const attempts: string[] = []
    const wf = createWorkflow({ name: 'order', input: z.object({}) })
      .step('charge', async () => ({ chargeId: 'ch_1' }))
      .step('ship', async ({ idempotencyKey, signal }) => {
        attempts.push(idempotencyKey)
        if (attempts.length === 1) {
          // The first process dies here, mid-step.
          await new Promise<void>((resolve) => signal.addEventListener('abort', () => resolve(), { once: true }))
          throw new Error('process died')
        }
        return { tracking: 'TRK' }
      })

    const lease = { runLeaseDurationMs: 300, heartbeatIntervalMs: 100 }
    const dying = createEngine({ storage, workflows: [wf], ...lease })
    const run = await dying.enqueue('order', {})
    await dying.start(20)
    while (attempts.length === 0) await new Promise((resolve) => setTimeout(resolve, 10))
    await dying.stop()

    const next = createEngine({ storage: new PostgresStorage({ pool, schema }), workflows: [wf], ...lease })
    await next.start(20)
    const result = await next.waitForRun(run.id, { pollIntervalMs: 20 })
    await next.stop()
    const steps = await storage.getStepResults(run.id)

    expect([result.status, result.output]).toStrictEqual(['completed', { tracking: 'TRK' }])
    expect(attempts).toStrictEqual([`${run.id}:ship`, `${run.id}:ship`])
    expect(steps.map((step) => [step.name, step.status])).toStrictEqual([['charge', 'completed'], ['ship', 'completed']])
  })
})
