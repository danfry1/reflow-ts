/**
 * PostgreSQL storage, for running workflows from many processes on many
 * machines against one database.
 *
 * Takes a `pg` pool — your app's own, or one it creates from a connection
 * string — and keeps its tables in their own Postgres schema (`reflow` by
 * default), so they never meet your app's.
 *
 * @example
 * ```ts
 * import { PostgresStorage } from 'reflow-ts/postgres'
 *
 * const storage = new PostgresStorage({ connectionString: process.env.DATABASE_URL })
 * const engine = createEngine({ storage, workflows: [order] })
 * await engine.start()
 * ```
 */

import { randomUUID } from 'node:crypto'
import pg from 'pg'
import type { Pool, PoolClient, PoolConfig } from 'pg'
import { ConfigError, InternalError } from '../core/errors'
import { nextOccurrence } from '../core/schedule-timing'
import {
  RUN_STATUSES,
  type ClaimedRun,
  type CountRunsFilter,
  type CreateRunResult,
  type DeleteRunsFilter,
  type ListRunsFilter,
  type PersistedValue,
  type RunActivity,
  type RunActivityFilter,
  type RunCounts,
  type RunError,
  type ScheduleRecurrence,
  type StepResult,
  type StorageAdapter,
  type WorkflowRun,
  type WorkflowSchedule,
} from '../core/types'
import { deserializePersistedValue, serializePersistedValue } from './codec'
import { emptyRunCounts, parseRunError, parseRunStatus, parseStepHistory, parseStepStatus } from './records'

/** How to reach the database. */
export type PostgresStorageOptions = (
  | {
    /**
     * A pool you already have, such as your app's. Reflow borrows clients
     * from it and never ends it: `close()` leaves it to you.
     */
    pool: Pool
    connectionString?: never
    poolConfig?: never
  }
  | {
    /** A connection string, such as `process.env.DATABASE_URL`. Reflow creates its own pool, and `close()` ends it. */
    connectionString: string
    /** Other settings for the pool Reflow creates, such as `max` or `ssl`. */
    poolConfig?: Omit<PoolConfig, 'connectionString'>
    pool?: never
  }
) & {
  /**
   * The Postgres schema Reflow's tables live in, created if missing.
   * Defaults to `reflow`. Lowercase letters, digits, and underscores.
   */
  schema?: string
}

/**
 * The schema's history, oldest first. Append only: a released migration is
 * never edited, because databases in the field have already run it.
 */
const MIGRATIONS: ReadonlyArray<(schema: string) => string> = [
  (s) => `
    CREATE TABLE ${s}.workflow_runs (
      id               TEXT PRIMARY KEY,
      workflow         TEXT NOT NULL,
      input            TEXT NOT NULL,
      idempotency_key  TEXT,
      lease_id         TEXT,
      status           TEXT NOT NULL,
      wake_at          BIGINT,
      created_at       BIGINT NOT NULL,
      updated_at       BIGINT NOT NULL,
      started_at       BIGINT,
      finished_at      BIGINT,
      output           TEXT,
      error            TEXT,
      waiting_for      TEXT
    );

    CREATE TABLE ${s}.workflow_steps (
      id          TEXT PRIMARY KEY,
      run_id      TEXT NOT NULL,
      name        TEXT NOT NULL,
      status      TEXT NOT NULL,
      output      TEXT,
      error       TEXT,
      attempts    INTEGER NOT NULL DEFAULT 0,
      created_at  BIGINT NOT NULL,
      updated_at  BIGINT NOT NULL,
      started_at  BIGINT,
      history     TEXT
    );

    CREATE TABLE ${s}.workflow_events (
      seq         BIGSERIAL PRIMARY KEY,
      id          TEXT NOT NULL UNIQUE,
      run_id      TEXT NOT NULL,
      event_name  TEXT NOT NULL,
      payload     TEXT,
      created_at  BIGINT NOT NULL
    );

    CREATE TABLE ${s}.workflow_schedules (
      key          TEXT PRIMARY KEY,
      workflow     TEXT NOT NULL,
      input        TEXT NOT NULL,
      interval_ms  BIGINT,
      cron         TEXT,
      next_run_at  BIGINT NOT NULL,
      created_at   BIGINT NOT NULL,
      updated_at   BIGINT NOT NULL
    );

    -- Claiming seeks the oldest due run of each workflow, one per way a run
    -- becomes due: pending, a sleep or wait past its wake time, a stale lease.
    CREATE INDEX runs_pending ON ${s}.workflow_runs (workflow, updated_at, created_at) WHERE status = 'pending';
    CREATE INDEX runs_sleeping ON ${s}.workflow_runs (workflow, wake_at, created_at) WHERE status = 'sleeping';
    CREATE INDEX runs_waiting ON ${s}.workflow_runs (workflow, wake_at, created_at) WHERE status = 'waiting' AND wake_at IS NOT NULL;
    CREATE INDEX runs_running ON ${s}.workflow_runs (workflow, updated_at) WHERE status = 'running';

    -- Listing, newest first, overall, by workflow, and by status.
    CREATE INDEX runs_created ON ${s}.workflow_runs (created_at DESC, id DESC);
    CREATE INDEX runs_workflow_created ON ${s}.workflow_runs (workflow, created_at DESC, id DESC);
    CREATE INDEX runs_status_created ON ${s}.workflow_runs (status, created_at DESC, id DESC);
    CREATE INDEX runs_status_workflow_created ON ${s}.workflow_runs (status, workflow, created_at DESC, id DESC);
    CREATE INDEX runs_finished ON ${s}.workflow_runs (finished_at) WHERE finished_at IS NOT NULL;
    CREATE UNIQUE INDEX runs_idempotency ON ${s}.workflow_runs (workflow, idempotency_key) WHERE idempotency_key IS NOT NULL;

    CREATE INDEX steps_run ON ${s}.workflow_steps (run_id, created_at);
    CREATE INDEX events_run_name ON ${s}.workflow_events (run_id, event_name, seq);
    CREATE INDEX schedules_due ON ${s}.workflow_schedules (next_run_at);
  `,
]

/** The schema version this build of Reflow writes. */
export const POSTGRES_SCHEMA_VERSION = MIGRATIONS.length

/** Any key for the advisory lock held while migrating; the same in every process. */
const MIGRATION_LOCK = 4_271_100_771

/** Postgres returns BIGINT as a string, to be safe; every one here is epoch ms or a count, well inside 2^53. */
const BIGINT_OID = 20
const types = {
  getTypeParser: (oid: number, format?: 'text' | 'binary') =>
    oid === BIGINT_OID ? (value: string) => Number(value) : pg.types.getTypeParser(oid, format),
}

type SqlParam = string | number | null | readonly string[]

/** A connection to run statements on: the pool, or one client inside a transaction. */
interface Queryable {
  query<Row extends object>(config: { text: string; values: SqlParam[]; types: typeof types }): Promise<{ rows: Row[]; rowCount: number | null }>
}

interface WorkflowRunRow {
  id: string
  workflow: string
  input: string
  idempotency_key: string | null
  lease_id: string | null
  status: string
  wake_at: number | null
  created_at: number
  updated_at: number
  started_at: number | null
  finished_at: number | null
  output: string | null
  error: string | null
  waiting_for: string | null
}

interface WorkflowStepRow {
  id: string
  run_id: string
  name: string
  status: string
  output: string | null
  error: string | null
  attempts: number
  created_at: number
  updated_at: number
  started_at: number | null
  history: string | null
}

interface WorkflowScheduleRow {
  key: string
  workflow: string
  input: string
  interval_ms: number | null
  cron: string | null
  next_run_at: number
  created_at: number
  updated_at: number
}

/** A run that could be claimed, and when it became due. */
interface Candidate extends WorkflowRunRow {
  due: number
}

/** Stored in PostgreSQL. See the module documentation. */
export class PostgresStorage implements StorageAdapter {
  readonly supportsConcurrencyLimits = true
  private readonly pool: Pool
  private readonly ownsPool: boolean
  private readonly schema: string

  constructor(options: PostgresStorageOptions) {
    const schema = options.schema ?? 'reflow'
    if (!/^[a-z_][a-z0-9_]{0,62}$/.test(schema)) {
      throw new ConfigError(`PostgresStorage schema "${schema}" must be lowercase letters, digits, and underscores, starting with a letter or underscore`)
    }
    this.schema = schema
    if (options.pool) {
      this.pool = options.pool
      this.ownsPool = false
    } else {
      this.pool = new pg.Pool({ ...options.poolConfig, connectionString: options.connectionString })
      this.ownsPool = true
    }
  }

  /**
   * Create the schema and tables, or bring them up to date. Safe to call from
   * many processes at once: they take turns under an advisory lock, and each
   * migration runs exactly once.
   */
  async initialize(): Promise<void> {
    await this.transaction(async (db) => {
      await this.exec(db, `SELECT pg_advisory_xact_lock(${MIGRATION_LOCK})`)
      await this.exec(db, `CREATE SCHEMA IF NOT EXISTS ${this.schema}`)
      await this.exec(db, `CREATE TABLE IF NOT EXISTS ${this.schema}.reflow_schema (version INTEGER NOT NULL)`)
      const row = await this.one<{ version: number }>(db, `SELECT version FROM ${this.schema}.reflow_schema`)
      const current = row?.version ?? 0
      if (current > POSTGRES_SCHEMA_VERSION) {
        throw new ConfigError(
          `Database schema version ${current} is newer than this version of reflow-ts supports (${POSTGRES_SCHEMA_VERSION}). Upgrade reflow-ts to open it.`,
        )
      }
      for (const migration of MIGRATIONS.slice(current)) {
        await this.exec(db, migration(this.schema))
      }
      if (row === null) {
        await this.exec(db, `INSERT INTO ${this.schema}.reflow_schema (version) VALUES ($1)`, POSTGRES_SCHEMA_VERSION)
      } else if (current < POSTGRES_SCHEMA_VERSION) {
        await this.exec(db, `UPDATE ${this.schema}.reflow_schema SET version = $1`, POSTGRES_SCHEMA_VERSION)
      }
    })
  }

  async createRun(run: WorkflowRun): Promise<CreateRunResult> {
    const inserted = await this.one<WorkflowRunRow>(
      this.pool,
      `INSERT INTO ${this.t('workflow_runs')}
         (id, workflow, input, idempotency_key, lease_id, status, created_at, updated_at,
          wake_at, started_at, finished_at, output, error, waiting_for)
       VALUES ($1, $2, $3, $4, NULL, $5, $6, $7, $8, $9, $10, $11, $12, $13)
       ON CONFLICT (workflow, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
       RETURNING *`,
      run.id,
      run.workflow,
      serializePersistedValue(run.input, 'Workflow input'),
      run.idempotencyKey,
      run.status,
      run.createdAt,
      run.updatedAt,
      run.wakeAt,
      run.startedAt,
      run.finishedAt,
      run.status === 'completed' ? serializePersistedValue(run.output, 'Run output') : null,
      run.error === null ? null : JSON.stringify(run.error),
      run.waitingFor,
    )
    if (inserted) {
      return { run: mapRun(inserted), created: true }
    }

    const existing = await this.one<WorkflowRunRow>(
      this.pool,
      `SELECT * FROM ${this.t('workflow_runs')} WHERE workflow = $1 AND idempotency_key = $2`,
      run.workflow,
      run.idempotencyKey,
    )
    if (!existing) {
      throw new InternalError(`Run with idempotency key "${run.idempotencyKey ?? ''}" conflicted but could not be read`)
    }
    return { run: mapRun(existing), created: false }
  }

  async claimNextRun(
    allWorkflowNames: readonly string[],
    staleBefore?: number,
    limits?: ReadonlyMap<string, number>,
  ): Promise<ClaimedRun | null> {
    if (allWorkflowNames.length === 0) {
      return null
    }

    return this.transaction(async (db) => {
      const workflowNames = limits ? await this.belowLimit(db, allWorkflowNames, staleBefore, limits) : allWorkflowNames
      if (workflowNames.length === 0) {
        return null
      }

      const now = Date.now()
      // The oldest due run of each workflow, per way of becoming due, each
      // found by seeking its partial index and locked as it is read. Runs
      // another claim has locked are skipped rather than waited on, so
      // claimers never queue behind one another.
      const oldest = (due: string, where: string, order: string): string =>
        `SELECT runs.*, ${due} AS due
         FROM unnest($1::text[]) AS w(name)
         CROSS JOIN LATERAL (
           SELECT * FROM ${this.t('workflow_runs')}
           WHERE workflow = w.name AND ${where}
           ORDER BY ${order}, created_at, id
           LIMIT 1
           FOR UPDATE SKIP LOCKED
         ) AS runs`
      const branches = [
        oldest('runs.updated_at', `status = 'pending'`, 'updated_at'),
        oldest('runs.wake_at', `status = 'sleeping' AND wake_at <= $2`, 'wake_at'),
        oldest('runs.wake_at', `status = 'waiting' AND wake_at IS NOT NULL AND wake_at <= $2`, 'wake_at'),
      ]
      const params: SqlParam[] = [workflowNames, now]
      if (staleBefore !== undefined) {
        // A stale run became due when its lease expired.
        branches.push(oldest('runs.updated_at + $3', `status = 'running' AND updated_at <= $4`, 'updated_at'))
        params.push(now - staleBefore, staleBefore)
      }
      const candidates = await this.all<Candidate>(db, branches.join(' UNION ALL '), ...params)
      const chosen = candidates.reduce<Candidate | null>((best, row) =>
        best === null || row.due < best.due || (row.due === best.due && row.created_at < best.created_at) ? row : best, null)
      if (!chosen) {
        return null
      }

      const leaseId = randomUUID()
      const claimed = await this.one<WorkflowRunRow>(
        db,
        `UPDATE ${this.t('workflow_runs')}
         SET status = 'running', lease_id = $1, wake_at = NULL, waiting_for = NULL, updated_at = $2,
             started_at = COALESCE(started_at, $2)
         WHERE id = $3
         RETURNING *`,
        leaseId,
        now,
        chosen.id,
      )
      if (!claimed) {
        throw new InternalError(`Run "${chosen.id}" vanished while locked for claiming`)
      }
      return { ...mapRun(claimed), status: 'running', leaseId }
    })
  }

  /**
   * These workflows, less any already running as many runs as `limits`
   * allows. Each limited workflow is locked for the rest of the claim's
   * transaction first, in name order so two claims cannot deadlock, so no
   * other claim can count it until this one has taken its place.
   */
  private async belowLimit(
    db: Queryable,
    workflowNames: readonly string[],
    staleBefore: number | undefined,
    limits: ReadonlyMap<string, number>,
  ): Promise<string[]> {
    const limited = workflowNames.filter((name) => limits.has(name)).sort()
    const full = new Set<string>()
    for (const name of limited) {
      await this.exec(db, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, `${this.schema}:limit:${name}`)
      const row = await this.one<{ live: number }>(
        db,
        `SELECT count(*) AS live FROM (
           SELECT 1 FROM ${this.t('workflow_runs')}
           WHERE status = 'running' AND workflow = $1 AND updated_at > $2
           LIMIT $3
         ) AS live`,
        name,
        staleBefore ?? Number.MIN_SAFE_INTEGER,
        limits.get(name) ?? 0,
      )
      if ((row?.live ?? 0) >= (limits.get(name) ?? 0)) full.add(name)
    }
    return workflowNames.filter((name) => !full.has(name))
  }

  async heartbeatRun(runId: string, leaseId: string): Promise<boolean> {
    return await this.changes(
      this.pool,
      `UPDATE ${this.t('workflow_runs')} SET updated_at = $1 WHERE id = $2 AND status = 'running' AND lease_id = $3`,
      Date.now(),
      runId,
      leaseId,
    ) > 0
  }

  async sleepRun(runId: string, leaseId: string, wakeAt: number): Promise<boolean> {
    return await this.changes(
      this.pool,
      `UPDATE ${this.t('workflow_runs')}
       SET status = 'sleeping', lease_id = NULL, wake_at = $1, updated_at = $2
       WHERE id = $3 AND status = 'running' AND lease_id = $4`,
      wakeAt,
      Date.now(),
      runId,
      leaseId,
    ) > 0
  }

  async waitRun(runId: string, leaseId: string, eventName: string, wakeAt: number | null): Promise<boolean> {
    return this.transaction(async (db) => {
      // Locking the run orders this against a concurrent deliverEvent, which
      // locks it too: the event is either already buffered here, or arrives
      // once the run is waiting and wakes it.
      const held = await this.one<{ id: string }>(
        db,
        `SELECT id FROM ${this.t('workflow_runs')} WHERE id = $1 AND status = 'running' AND lease_id = $2 FOR UPDATE`,
        runId,
        leaseId,
      )
      if (!held) {
        return false
      }

      const buffered = await this.one<{ id: string }>(
        db,
        `SELECT id FROM ${this.t('workflow_events')} WHERE run_id = $1 AND event_name = $2 LIMIT 1`,
        runId,
        eventName,
      )
      if (buffered) {
        await this.exec(
          db,
          `UPDATE ${this.t('workflow_runs')} SET status = 'pending', lease_id = NULL, wake_at = NULL, waiting_for = NULL, updated_at = $1 WHERE id = $2`,
          Date.now(),
          runId,
        )
      } else {
        await this.exec(
          db,
          `UPDATE ${this.t('workflow_runs')} SET status = 'waiting', lease_id = NULL, wake_at = $1, waiting_for = $2, updated_at = $3 WHERE id = $4`,
          wakeAt,
          eventName,
          Date.now(),
          runId,
        )
      }
      return true
    })
  }

  async deliverEvent(runId: string, eventName: string, payload: PersistedValue): Promise<boolean> {
    return this.transaction(async (db) => {
      const exists = await this.one<{ id: string }>(db, `SELECT id FROM ${this.t('workflow_runs')} WHERE id = $1 FOR UPDATE`, runId)
      if (!exists) {
        return false
      }
      const now = Date.now()
      await this.exec(
        db,
        `INSERT INTO ${this.t('workflow_events')} (id, run_id, event_name, payload, created_at) VALUES ($1, $2, $3, $4, $5)`,
        randomUUID(),
        runId,
        eventName,
        serializePersistedValue(payload, 'Event payload'),
        now,
      )
      await this.exec(
        db,
        `UPDATE ${this.t('workflow_runs')} SET status = 'pending', lease_id = NULL, wake_at = NULL, waiting_for = NULL, updated_at = $1
         WHERE id = $2 AND status = 'waiting'`,
        now,
        runId,
      )
      return true
    })
  }

  async takeEvent(runId: string, eventName: string): Promise<{ payload: PersistedValue } | null> {
    const row = await this.one<{ payload: string | null }>(
      this.pool,
      `DELETE FROM ${this.t('workflow_events')}
       WHERE seq = (
         SELECT seq FROM ${this.t('workflow_events')}
         WHERE run_id = $1 AND event_name = $2
         ORDER BY created_at, seq
         LIMIT 1
         FOR UPDATE SKIP LOCKED
       )
       RETURNING payload`,
      runId,
      eventName,
    )
    if (!row) {
      return null
    }
    return { payload: row.payload === null ? null : deserializePersistedValue(row.payload) }
  }

  async getRun(runId: string): Promise<WorkflowRun | null> {
    const row = await this.one<WorkflowRunRow>(this.pool, `SELECT * FROM ${this.t('workflow_runs')} WHERE id = $1`, runId)
    return row ? mapRun(row) : null
  }

  async listRuns(filter: ListRunsFilter = {}): Promise<WorkflowRun[]> {
    const { status, workflow, limit = 100, before, beforeId } = filter
    if (status !== undefined && !RUN_STATUSES.includes(status)) {
      return []
    }
    const conditions: string[] = []
    const params: SqlParam[] = []
    const param = (value: SqlParam): string => {
      params.push(value)
      return `$${params.length}`
    }
    if (status !== undefined) conditions.push(`status = ${param(status)}`)
    if (workflow !== undefined) conditions.push(`workflow = ${param(workflow)}`)
    if (before !== undefined) {
      // Keyset cursor over the (created_at DESC, id DESC) order.
      conditions.push(beforeId === undefined
        ? `created_at < ${param(before)}`
        : `(created_at, id) < (${param(before)}, ${param(beforeId)})`)
    }
    const rows = await this.all<WorkflowRunRow>(
      this.pool,
      `SELECT * FROM ${this.t('workflow_runs')}
       ${conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : ''}
       ORDER BY created_at DESC, id DESC
       LIMIT ${param(limit)}`,
      ...params,
    )
    return rows.map(mapRun)
  }

  async requeueRun(runId: string): Promise<boolean> {
    return this.transaction(async (db) => {
      const changed = await this.changes(
        db,
        `UPDATE ${this.t('workflow_runs')}
         SET status = 'pending', lease_id = NULL, updated_at = $1, finished_at = NULL, output = NULL, error = NULL
         WHERE id = $2 AND status IN ('failed', 'cancelled')`,
        Date.now(),
        runId,
      )
      if (changed === 0) {
        return false
      }
      // A failed step runs again from scratch, and so does one that was
      // waiting out a backoff when the run was cancelled.
      await this.exec(db, `DELETE FROM ${this.t('workflow_steps')} WHERE run_id = $1 AND status IN ('failed', 'retrying')`, runId)
      return true
    })
  }

  async getStepResults(runId: string): Promise<StepResult[]> {
    const rows = await this.all<WorkflowStepRow>(
      this.pool,
      `SELECT * FROM ${this.t('workflow_steps')} WHERE run_id = $1 ORDER BY created_at ASC, id ASC`,
      runId,
    )
    return rows.map(mapStep)
  }

  async getStepResultsForRuns(runIds: readonly string[]): Promise<Map<string, StepResult[]>> {
    const byRun = new Map<string, StepResult[]>(runIds.map((id) => [id, []]))
    if (runIds.length === 0) {
      return byRun
    }
    const rows = await this.all<WorkflowStepRow>(
      this.pool,
      `SELECT * FROM ${this.t('workflow_steps')} WHERE run_id = ANY($1::text[]) ORDER BY run_id, created_at ASC, id ASC`,
      runIds,
    )
    for (const row of rows) {
      byRun.get(row.run_id)?.push(mapStep(row))
    }
    return byRun
  }

  async runActivity(filter: RunActivityFilter): Promise<RunActivity> {
    const { since, bucketMs, buckets } = filter
    const until = since + bucketMs * buckets
    const scope = filter.workflow === undefined ? '' : 'AND workflow = $5'
    const scoped: SqlParam[] = filter.workflow === undefined ? [since, bucketMs, since, until] : [since, bucketMs, since, until, filter.workflow]
    const activity: RunActivity = {
      started: new Array<number>(buckets).fill(0),
      completed: new Array<number>(buckets).fill(0),
      failed: new Array<number>(buckets).fill(0),
    }
    const started = await this.all<{ bucket: number; n: number }>(
      this.pool,
      `SELECT ((created_at - $1) / $2)::bigint AS bucket, count(*) AS n FROM ${this.t('workflow_runs')}
       WHERE created_at >= $3 AND created_at < $4 ${scope} GROUP BY bucket`,
      ...scoped,
    )
    for (const row of started) activity.started[row.bucket] = row.n
    const finished = await this.all<{ bucket: number; status: string; n: number }>(
      this.pool,
      `SELECT ((finished_at - $1) / $2)::bigint AS bucket, status, count(*) AS n FROM ${this.t('workflow_runs')}
       WHERE finished_at >= $3 AND finished_at < $4 AND status IN ('completed', 'failed') ${scope}
       GROUP BY bucket, status`,
      ...scoped,
    )
    for (const row of finished) {
      const counts = row.status === 'completed' ? activity.completed : activity.failed
      counts[row.bucket] = row.n
    }
    return activity
  }

  async saveStepResult(result: StepResult, leaseId?: string): Promise<boolean> {
    return this.transaction(async (db) => {
      if (leaseId) {
        // A shared lock: a claim taking the run over waits until this step is
        // saved, and a step saved after it took over is refused.
        const held = await this.one<{ id: string }>(
          db,
          `SELECT id FROM ${this.t('workflow_runs')} WHERE id = $1 AND status = 'running' AND lease_id = $2 FOR SHARE`,
          result.runId,
          leaseId,
        )
        if (!held) {
          return false
        }
      }
      await this.exec(
        db,
        `INSERT INTO ${this.t('workflow_steps')} (id, run_id, name, status, output, error, attempts, created_at, updated_at, started_at, history)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
         ON CONFLICT (id) DO UPDATE SET
           run_id = excluded.run_id, name = excluded.name, status = excluded.status, output = excluded.output,
           error = excluded.error, attempts = excluded.attempts, created_at = excluded.created_at,
           updated_at = excluded.updated_at, started_at = excluded.started_at, history = excluded.history`,
        result.id,
        result.runId,
        result.name,
        result.status,
        serializePersistedValue(result.output, 'Step output'),
        result.error,
        result.attempts,
        result.createdAt,
        result.updatedAt,
        result.startedAt,
        JSON.stringify(result.history),
      )
      return true
    })
  }

  async completeRun(runId: string, leaseId: string, output: PersistedValue): Promise<boolean> {
    const now = Date.now()
    return await this.changes(
      this.pool,
      `UPDATE ${this.t('workflow_runs')}
       SET status = 'completed', lease_id = NULL, wake_at = NULL, waiting_for = NULL, output = $1, updated_at = $2, finished_at = $2
       WHERE id = $3 AND status = 'running' AND lease_id = $4`,
      serializePersistedValue(output, 'Run output'),
      now,
      runId,
      leaseId,
    ) > 0
  }

  async failRun(runId: string, leaseId: string, error: RunError): Promise<boolean> {
    const now = Date.now()
    return await this.changes(
      this.pool,
      `UPDATE ${this.t('workflow_runs')}
       SET status = 'failed', lease_id = NULL, wake_at = NULL, waiting_for = NULL, error = $1, updated_at = $2, finished_at = $2
       WHERE id = $3 AND status = 'running' AND lease_id = $4`,
      JSON.stringify(error),
      now,
      runId,
      leaseId,
    ) > 0
  }

  async cancelRun(runId: string): Promise<boolean> {
    const now = Date.now()
    return await this.changes(
      this.pool,
      `UPDATE ${this.t('workflow_runs')}
       SET status = 'cancelled', lease_id = NULL, wake_at = NULL, waiting_for = NULL, updated_at = $1, finished_at = $1
       WHERE id = $2 AND status NOT IN ('completed', 'failed', 'cancelled')`,
      now,
      runId,
    ) > 0
  }

  async countRuns(filter: CountRunsFilter = {}): Promise<RunCounts> {
    const rows = filter.workflow === undefined
      ? await this.all<{ status: string; n: number }>(this.pool, `SELECT status, count(*) AS n FROM ${this.t('workflow_runs')} GROUP BY status`)
      : await this.all<{ status: string; n: number }>(
        this.pool,
        `SELECT status, count(*) AS n FROM ${this.t('workflow_runs')} WHERE workflow = $1 GROUP BY status`,
        filter.workflow,
      )
    const counts = emptyRunCounts()
    for (const row of rows) {
      counts[parseRunStatus(row.status)] = row.n
    }
    return counts
  }

  async deleteRuns(filter: DeleteRunsFilter): Promise<number> {
    if (filter.statuses.length === 0 || filter.limit < 1) {
      return 0
    }
    return this.transaction(async (db) => {
      const ids = (await this.all<{ id: string }>(
        db,
        `SELECT id FROM ${this.t('workflow_runs')}
         WHERE finished_at < $1 AND status = ANY($2::text[])
         ORDER BY finished_at ASC, id ASC
         LIMIT $3
         FOR UPDATE SKIP LOCKED`,
        filter.finishedBefore,
        filter.statuses,
        filter.limit,
      )).map((row) => row.id)
      if (ids.length === 0) {
        return 0
      }
      await this.exec(db, `DELETE FROM ${this.t('workflow_steps')} WHERE run_id = ANY($1::text[])`, ids)
      await this.exec(db, `DELETE FROM ${this.t('workflow_events')} WHERE run_id = ANY($1::text[])`, ids)
      return this.changes(db, `DELETE FROM ${this.t('workflow_runs')} WHERE id = ANY($1::text[])`, ids)
    })
  }

  /**
   * Re-registering keeps `next_run_at` unless the recurrence changed, so a
   * redeploy rejoins the existing cadence rather than pushing the next firing
   * out by a full interval each time.
   */
  async upsertSchedule(schedule: WorkflowSchedule): Promise<WorkflowSchedule> {
    const row = await this.one<WorkflowScheduleRow>(
      this.pool,
      `INSERT INTO ${this.t('workflow_schedules')} AS existing (key, workflow, input, interval_ms, cron, next_run_at, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
       ON CONFLICT (key) DO UPDATE SET
         workflow = excluded.workflow,
         input = excluded.input,
         interval_ms = excluded.interval_ms,
         cron = excluded.cron,
         next_run_at = CASE WHEN existing.interval_ms IS NOT DISTINCT FROM excluded.interval_ms
                             AND existing.cron IS NOT DISTINCT FROM excluded.cron
                            THEN existing.next_run_at
                            ELSE excluded.next_run_at END,
         updated_at = excluded.updated_at
       RETURNING *`,
      schedule.key,
      schedule.workflow,
      serializePersistedValue(schedule.input, 'Schedule input'),
      schedule.recurrence.kind === 'interval' ? schedule.recurrence.intervalMs : null,
      schedule.recurrence.kind === 'cron' ? schedule.recurrence.expression : null,
      schedule.nextRunAt,
      schedule.createdAt,
      schedule.updatedAt,
    )
    if (!row) {
      throw new InternalError(`Schedule "${schedule.key}" vanished immediately after upsert`)
    }
    return mapSchedule(row)
  }

  /**
   * Locking the due schedule and advancing it in one transaction is what
   * makes a claim exclusive: a second claim skips the locked row, and once
   * this commits it is no longer due.
   */
  async claimDueSchedule(workflowNames: readonly string[], now: number): Promise<WorkflowSchedule | null> {
    if (workflowNames.length === 0) {
      return null
    }
    return this.transaction(async (db) => {
      const row = await this.one<WorkflowScheduleRow>(
        db,
        `SELECT * FROM ${this.t('workflow_schedules')}
         WHERE next_run_at <= $1 AND workflow = ANY($2::text[])
         ORDER BY next_run_at ASC, key ASC
         LIMIT 1
         FOR UPDATE SKIP LOCKED`,
        now,
        workflowNames,
      )
      if (!row) {
        return null
      }
      await this.exec(
        db,
        `UPDATE ${this.t('workflow_schedules')} SET next_run_at = $1, updated_at = $2 WHERE key = $3`,
        nextOccurrence(row.next_run_at, recurrenceOf(row), now),
        now,
        row.key,
      )
      return mapSchedule(row)
    })
  }

  async deleteSchedule(key: string): Promise<boolean> {
    return await this.changes(this.pool, `DELETE FROM ${this.t('workflow_schedules')} WHERE key = $1`, key) > 0
  }

  async listSchedules(): Promise<WorkflowSchedule[]> {
    const rows = await this.all<WorkflowScheduleRow>(this.pool, `SELECT * FROM ${this.t('workflow_schedules')} ORDER BY key ASC`)
    return rows.map(mapSchedule)
  }

  /**
   * Ends the pool Reflow created from a connection string; a pool you passed
   * in is left open for you to end. Use {@link PostgresStorage.end} to wait
   * for it.
   */
  close(): void {
    if (this.ownsPool) {
      // Ending is asynchronous and close() is not; a failure to end still
      // surfaces as an unhandled rejection rather than being dropped.
      void this.pool.end()
    }
  }

  /** Like {@link PostgresStorage.close}, but resolves once the pool has ended. */
  async end(): Promise<void> {
    if (this.ownsPool) {
      await this.pool.end()
    }
  }

  /** A table's name, qualified by Reflow's schema. */
  private t(table: string): string {
    return `${this.schema}.${table}`
  }

  private async all<Row extends object>(db: Queryable, text: string, ...values: SqlParam[]): Promise<Row[]> {
    return (await db.query<Row>({ text, values, types })).rows
  }

  private async one<Row extends object>(db: Queryable, text: string, ...values: SqlParam[]): Promise<Row | null> {
    return (await this.all<Row>(db, text, ...values))[0] ?? null
  }

  private async exec(db: Queryable, text: string, ...values: SqlParam[]): Promise<void> {
    await db.query({ text, values, types })
  }

  private async changes(db: Queryable, text: string, ...values: SqlParam[]): Promise<number> {
    return (await db.query({ text, values, types })).rowCount ?? 0
  }

  /**
   * Run `fn` in a transaction on one client from the pool, rolling back if it
   * throws. A rollback that itself fails is reported alongside the error that
   * caused it, and the client is discarded rather than returned to the pool.
   */
  private async transaction<T>(fn: (db: Queryable) => Promise<T>): Promise<T> {
    const client: PoolClient = await this.pool.connect()
    let broken = false
    try {
      await client.query('BEGIN')
      let result: T
      try {
        result = await fn(client)
      } catch (error) {
        try {
          await client.query('ROLLBACK')
        } catch (rollbackError) {
          broken = true
          throw new AggregateError([error, rollbackError], 'Transaction failed and could not be rolled back', { cause: error })
        }
        throw error
      }
      await client.query('COMMIT')
      return result
    } finally {
      // A client whose rollback failed is in an unknown state: destroy it.
      client.release(broken)
    }
  }
}

function mapRun(row: WorkflowRunRow): WorkflowRun {
  return {
    id: row.id,
    workflow: row.workflow,
    input: deserializePersistedValue(row.input),
    idempotencyKey: row.idempotency_key,
    status: parseRunStatus(row.status),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    startedAt: row.started_at,
    finishedAt: row.finished_at,
    // NULL means no output was recorded; a recorded `null` is the string "null".
    output: row.output === null ? undefined : deserializePersistedValue(row.output),
    error: row.error === null ? null : parseRunError(row.error),
    wakeAt: row.wake_at,
    waitingFor: row.waiting_for,
  }
}

function mapStep(row: WorkflowStepRow): StepResult {
  return {
    id: row.id,
    runId: row.run_id,
    name: row.name,
    status: parseStepStatus(row.status),
    output: row.output === null ? null : deserializePersistedValue(row.output),
    error: row.error,
    attempts: row.attempts,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    startedAt: row.started_at,
    history: row.history === null ? [] : parseStepHistory(row.history),
  }
}

/** The recurrence a schedule row describes: `cron` when set, otherwise the interval. */
function recurrenceOf(row: WorkflowScheduleRow): ScheduleRecurrence {
  return row.cron !== null
    ? { kind: 'cron', expression: row.cron }
    : { kind: 'interval', intervalMs: row.interval_ms ?? 0 }
}

function mapSchedule(row: WorkflowScheduleRow): WorkflowSchedule {
  return {
    key: row.key,
    workflow: row.workflow,
    input: deserializePersistedValue(row.input),
    recurrence: recurrenceOf(row),
    nextRunAt: row.next_run_at,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  }
}
