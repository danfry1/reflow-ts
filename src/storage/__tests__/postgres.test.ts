import { randomUUID } from 'node:crypto'
import { afterAll, describe, expect, it, vi } from 'vitest'
import pg from 'pg'
import { z } from 'zod'
import { createEngine } from '../../core/engine'
import { ConfigError } from '../../core/errors'
import { createWorkflow } from '../../core/workflow'
import { storageConformanceCases } from '../conformance'
import { PostgresStorage, POSTGRES_SCHEMA_VERSION } from '../postgres'

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
    for (const schema of schemas) await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`)
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
    expect(rows).toStrictEqual([{ version: POSTGRES_SCHEMA_VERSION }])
  })

  it('upgrades a schema from before step kinds, reading its rows with none', async () => {
    const storage = await fresh()
    const schema = schemas.at(-1) ?? ''
    await pool.query(`ALTER TABLE ${schema}.workflow_steps DROP COLUMN kind`)
    await pool.query(`UPDATE ${schema}.reflow_schema SET version = 2`)
    await pool.query(
      `INSERT INTO ${schema}.workflow_runs (id, workflow, input, status, created_at, updated_at) VALUES ('old', 'w', '{}', 'completed', 1, 1)`,
    )
    await pool.query(
      `INSERT INTO ${schema}.workflow_steps (id, run_id, name, status, output, error, attempts, created_at, updated_at, started_at, history)
       VALUES ('s', 'old', 'charge', 'completed', '{}', NULL, 1, 1, 2, 1, '[]')`,
    )

    await new PostgresStorage({ pool, schema }).initialize()

    const { rows } = await pool.query<{ version: number }>(`SELECT version FROM ${schema}.reflow_schema`)
    expect(rows).toStrictEqual([{ version: POSTGRES_SCHEMA_VERSION }])
    expect((await storage.getStepResults('old'))[0]).toMatchObject({ name: 'charge', kind: null })
  })

  it('refuses a database written by a newer version', async () => {
    const storage = await fresh()
    const schema = schemas.at(-1) ?? ''
    await pool.query(`UPDATE ${schema}.reflow_schema SET version = 99`)
    await expect(storage.initialize()).rejects.toThrow(
      new ConfigError(`Database schema version 99 is newer than this version of reflow-ts supports (${POSTGRES_SCHEMA_VERSION}). Upgrade reflow-ts to open it.`),
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

  it('stores fractional milliseconds rounded up rather than refusing them', async () => {
    const storage = await fresh()
    const wf = createWorkflow({ name: 'later', input: z.object({}) }).step('go', async () => {})
    const engine = createEngine({ storage, workflows: [wf] })
    const run = await engine.enqueue('later', {}, { delay: 2.5 })
    await storage.upsertSchedule({
      key: 's', workflow: 'later', input: {}, recurrence: { kind: 'interval', intervalMs: 1.5 },
      nextRunAt: Date.now() + 0.25, createdAt: Date.now(), updatedAt: Date.now(),
    })
    const stored = await storage.getRun(run.id)
    expect([stored?.status, Number.isInteger(stored?.wakeAt)]).toStrictEqual(['sleeping', true])
    expect((await storage.listSchedules()).map((schedule) => schedule.recurrence)).toStrictEqual([{ kind: 'interval', intervalMs: 2 }])
  })

  /** A pending run, for seeding storage directly. */
  function pendingRun(id: string, workflow: string, at: number) {
    return {
      id, workflow, input: {}, idempotencyKey: null, status: 'pending' as const,
      createdAt: at, updatedAt: at, startedAt: null, finishedAt: null,
      output: undefined, error: null, wakeAt: null, waitingFor: null,
    }
  }

  it('lets concurrent claims across workflows each take a run, rather than locking each other out', async () => {
    const storage = await fresh()
    const schema = schemas.at(-1) ?? ''
    const workers = [1, 2, 3, 4].map(() => new PostgresStorage({ pool: new pg.Pool({ connectionString: url, max: 2 }), schema }))
    let short = 0
    for (let round = 0; round < 20; round++) {
      const now = Date.now()
      for (const workflow of ['a', 'b', 'c', 'd']) await storage.createRun(pendingRun(`${workflow}_${round}`, workflow, now))
      const claims = await Promise.all(workers.map((worker) => worker.claimNextRun(['a', 'b', 'c', 'd'])))
      if (claims.filter((claim) => claim !== null).length < 4) short++
    }
    await Promise.all(workers.map((worker) => worker.end()))
    expect(short).toBe(0)
  })

  it('does not hold back claims of an unlimited workflow while another has a limit', async () => {
    const storage = await fresh()
    const schema = schemas.at(-1) ?? ''
    const now = Date.now()
    for (let i = 0; i < 20; i++) await storage.createRun(pendingRun(`free_${i}`, 'free', now + i))
    const workers = Array.from({ length: 20 }, () => new PostgresStorage({ pool: new pg.Pool({ connectionString: url, max: 1 }), schema }))
    const limits = new Map([['capped', 1]])
    const claims = await Promise.all(workers.map((worker) => worker.claimNextRun(['capped', 'free'], now - 30_000, limits)))
    await Promise.all(workers.map((worker) => worker.end()))
    expect(new Set(claims.map((claim) => claim?.id)).size).toBe(20)
  })

  it('works with a schema named after an SQL keyword', async () => {
    schemas.push('user')
    const storage = new PostgresStorage({ pool, schema: 'user' })
    await storage.initialize()
    await storage.createRun(pendingRun('r1', 'w', Date.now()))
    expect((await storage.claimNextRun(['w']))?.id).toBe('r1')
  })

  it('starts as a role that may read and write rows but not create tables, once migrated', async () => {
    // Migrated by the owner, as a deploy step would.
    await fresh()
    const schema = schemas.at(-1) ?? ''
    const role = `reflow_app_${schema.slice(-8)}`
    await pool.query(`CREATE ROLE ${role} LOGIN PASSWORD 'app'`)
    await pool.query(`GRANT USAGE ON SCHEMA ${schema} TO ${role}`)
    await pool.query(`GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA ${schema} TO ${role}`)
    await pool.query(`GRANT USAGE ON ALL SEQUENCES IN SCHEMA ${schema} TO ${role}`)
    const appUrl = new URL(url ?? '')
    appUrl.username = role
    appUrl.password = 'app'
    const app = new PostgresStorage({ connectionString: appUrl.toString(), schema })
    try {
      await app.initialize()
      await app.createRun(pendingRun('r1', 'w', Date.now()))
      expect((await app.claimNextRun(['w']))?.id).toBe('r1')
    } finally {
      await app.end()
      await pool.query(`DROP OWNED BY ${role}`)
      await pool.query(`DROP ROLE ${role}`)
    }
  })

  it('ends its pool once, however close() and end() are combined', async () => {
    const schema = `reflow_test_${randomUUID().replaceAll('-', '').slice(0, 16)}`
    schemas.push(schema)
    const storage = new PostgresStorage({ connectionString: url ?? '', schema })
    await storage.initialize()
    storage.close()
    await storage.end()
    await storage.end()
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

  /** Claim as a process whose clock is `skewMs` off the database's, with the default 30s lease. */
  async function claimWithClockOff(storage: PostgresStorage, skewMs: number, limits?: ReadonlyMap<string, number>) {
    const realNow = Date.now.bind(Date)
    const clock = vi.spyOn(Date, 'now').mockImplementation(() => realNow() + skewMs)
    try {
      return await storage.claimNextRun(['w'], Date.now() - 30_000, limits)
    } finally {
      clock.mockRestore()
    }
  }

  it('does not take a live run for a process whose clock runs ahead', async () => {
    const storage = await fresh()
    await storage.createRun(pendingRun('r', 'w', Date.now()))
    expect((await storage.claimNextRun(['w']))?.id).toBe('r')

    // A minute ahead, its own clock says the 30s lease ran out long ago.
    expect(await claimWithClockOff(storage, 60_000)).toBe(null)
  })

  it('takes over a run whose lease expired by the database clock, for a process whose clock runs behind', async () => {
    const storage = await fresh()
    const schema = schemas.at(-1) ?? ''
    await storage.createRun(pendingRun('r', 'w', Date.now()))
    expect((await storage.claimNextRun(['w']))?.id).toBe('r')
    // Its last renewal was a minute ago, by the database's clock.
    await pool.query(`UPDATE ${schema}.workflow_runs SET updated_at = updated_at - 60000 WHERE id = 'r'`)

    expect((await claimWithClockOff(storage, -60_000))?.id).toBe('r')
  })

  it('renews a lease on the database clock', async () => {
    const storage = await fresh()
    const schema = schemas.at(-1) ?? ''
    await storage.createRun(pendingRun('r', 'w', Date.now()))
    const claimed = await storage.claimNextRun(['w'])
    await pool.query(`UPDATE ${schema}.workflow_runs SET updated_at = updated_at - 60000 WHERE id = 'r'`)

    const realNow = Date.now.bind(Date)
    const clock = vi.spyOn(Date, 'now').mockImplementation(() => realNow() - 60_000)
    try {
      expect(await storage.heartbeatRun('r', claimed?.leaseId ?? '')).toBe(true)
    } finally {
      clock.mockRestore()
    }
    // Renewed now by the database, not a minute ago by the worker's clock.
    expect(await claimWithClockOff(storage, 0)).toBe(null)
  })

  it('holds a concurrency limit for a process whose clock runs ahead', async () => {
    const storage = await fresh()
    await storage.createRun(pendingRun('r1', 'w', Date.now() - 1))
    await storage.createRun(pendingRun('r2', 'w', Date.now()))
    expect((await storage.claimNextRun(['w'], Date.now() - 30_000, new Map([['w', 1]])))?.id).toBe('r1')

    // A minute ahead, it must still count r1 as running, and so leave r2.
    expect(await claimWithClockOff(storage, 60_000, new Map([['w', 1]]))).toBe(null)
  })

  it.each([
    { skewMs: 10_000, pendingFor: 7_000, first: 'pending' },
    { skewMs: 10_000, pendingFor: 3_000, first: 'stale' },
    { skewMs: -10_000, pendingFor: 7_000, first: 'pending' },
    { skewMs: -10_000, pendingFor: 3_000, first: 'stale' },
  ])('ranks a run whose lease expired 5s ago against one pending for $pendingFor ms, with the fleet $skewMs ms off the database', async ({ skewMs, pendingFor, first }) => {
    const storage = await fresh()
    const schema = schemas.at(-1) ?? ''
    await storage.createRun(pendingRun('stale', 'w', Date.now() - 60_000))
    expect((await storage.claimNextRun(['w']))?.id).toBe('stale')
    // Its last renewal, by the database's clock, was 35s ago: 5s past its 30s lease.
    await pool.query(`UPDATE ${schema}.workflow_runs SET updated_at = (extract(epoch FROM clock_timestamp()) * 1000)::bigint - 35000 WHERE id = 'stale'`)
    // Made by a worker on the fleet's clock.
    await storage.createRun(pendingRun('pending', 'w', Date.now() + skewMs - pendingFor))

    expect((await claimWithClockOff(storage, skewMs))?.id).toBe(first)
  })
})
