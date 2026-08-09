/**
 * SQLite storage adapter for Node.js using the built-in `node:sqlite` module.
 * No native dependencies required — unlike the `better-sqlite3`-backed
 * `reflow-ts/sqlite-node` adapter.
 *
 * Requires **Node.js >= 22.5** (when `node:sqlite` was added). On Node 22.x and
 * 23.x before 23.4 it is gated behind the `--experimental-sqlite` flag; from
 * Node 23.4 it is available by default (still experimental). On older Node, or
 * on Bun, use `reflow-ts/sqlite-node` or `reflow-ts/sqlite-bun` instead.
 *
 * @example
 * ```ts
 * import { SQLiteStorage } from 'reflow-ts/sqlite-node-builtin'
 *
 * const storage = new SQLiteStorage('./reflow.db')
 * await storage.initialize()
 * ```
 */

import { randomUUID } from 'node:crypto'
import { createRequire } from 'node:module'
import type { DatabaseSync, StatementSync } from 'node:sqlite'
import type {
  ClaimedRun,
  CreateRunResult,
  PersistedValue,
  ListRunsFilter,
  RunStatus,
  StepResult,
  StorageAdapter,
  WorkflowRun,
  WorkflowSchedule,
  ScheduleRecurrence,
} from '../core/types'
import { deserializePersistedValue, serializePersistedValue } from './codec'
import { nextOccurrence } from '../core/schedule-timing'
import { InternalError } from '../core/errors'

// `node:sqlite` is loaded lazily through createRequire so importing this module
// is safe on any runtime — only constructing SQLiteStorage requires the module
// to be present (Node >= 22.5).
const nodeRequire = createRequire(import.meta.url)

interface WorkflowRunRow {
  id: string
  workflow: string
  input: string
  idempotency_key: string | null
  lease_id: string | null
  status: string
  created_at: number
  updated_at: number
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

export class SQLiteStorage implements StorageAdapter {
  private db: DatabaseSync

  constructor(path: string) {
    const sqlite = nodeRequire('node:sqlite') as typeof import('node:sqlite')
    this.db = new sqlite.DatabaseSync(path)
    this.db.exec('PRAGMA journal_mode = WAL')
    this.db.exec('PRAGMA synchronous = NORMAL')
    this.db.exec('PRAGMA busy_timeout = 5000')
  }

  async initialize(): Promise<void> {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS workflow_runs (
        id               TEXT PRIMARY KEY,
        workflow         TEXT NOT NULL,
        input            TEXT NOT NULL,
        idempotency_key  TEXT,
        lease_id         TEXT,
        status           TEXT NOT NULL,
        wake_at          INTEGER,
        created_at       INTEGER NOT NULL,
        updated_at       INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS workflow_steps (
        id          TEXT PRIMARY KEY,
        run_id      TEXT NOT NULL,
        name        TEXT NOT NULL,
        status      TEXT NOT NULL,
        output      TEXT,
        error       TEXT,
        attempts    INTEGER DEFAULT 0,
        created_at  INTEGER NOT NULL,
        updated_at  INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS workflow_events (
        id          TEXT PRIMARY KEY,
        run_id      TEXT NOT NULL,
        event_name  TEXT NOT NULL,
        payload     TEXT,
        created_at  INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS workflow_schedules (
        key          TEXT PRIMARY KEY,
        workflow     TEXT NOT NULL,
        input        TEXT NOT NULL,
        interval_ms  INTEGER,
        cron         TEXT,
        next_run_at  INTEGER NOT NULL,
        created_at   INTEGER NOT NULL,
        updated_at   INTEGER NOT NULL
      );
    `)

    // Migrate databases created before the wake_at column existed.
    const columns = this.all<{ name: string }>(`PRAGMA table_info(workflow_runs)`)
    if (!columns.some((column) => column.name === 'wake_at')) {
      this.db.exec(`ALTER TABLE workflow_runs ADD COLUMN wake_at INTEGER`)
    }


    // 0.6.0 shipped workflow_schedules with a NOT NULL interval_ms and no cron
    // column. Adding cron alone is not enough: a cron schedule leaves
    // interval_ms null, which that constraint rejects. SQLite cannot drop a NOT
    // NULL in place, so the table is rebuilt — cheap, since schedules are few.
    const scheduleColumns = this.all<{ name: string }>(`PRAGMA table_info(workflow_schedules)`)
    if (scheduleColumns.length > 0 && !scheduleColumns.some((column) => column.name === 'cron')) {
      this.db.exec(`
        ALTER TABLE workflow_schedules RENAME TO workflow_schedules_old;

        CREATE TABLE workflow_schedules (
          key          TEXT PRIMARY KEY,
          workflow     TEXT NOT NULL,
          input        TEXT NOT NULL,
          interval_ms  INTEGER,
          cron         TEXT,
          next_run_at  INTEGER NOT NULL,
          created_at   INTEGER NOT NULL,
          updated_at   INTEGER NOT NULL
        );

        INSERT INTO workflow_schedules (key, workflow, input, interval_ms, cron, next_run_at, created_at, updated_at)
        SELECT key, workflow, input, interval_ms, NULL, next_run_at, created_at, updated_at
        FROM workflow_schedules_old;

        DROP TABLE workflow_schedules_old;
      `)
    }

    this.db.exec(`
      CREATE INDEX IF NOT EXISTS idx_runs_status ON workflow_runs(status, workflow);
      CREATE INDEX IF NOT EXISTS idx_runs_wake ON workflow_runs(status, wake_at);
      CREATE INDEX IF NOT EXISTS idx_steps_run_id ON workflow_steps(run_id);
      CREATE INDEX IF NOT EXISTS idx_events_run_name ON workflow_events(run_id, event_name, created_at);
      CREATE INDEX IF NOT EXISTS idx_schedules_due ON workflow_schedules(next_run_at);
      CREATE UNIQUE INDEX IF NOT EXISTS idx_runs_workflow_idempotency
      ON workflow_runs(workflow, idempotency_key)
      WHERE idempotency_key IS NOT NULL;
    `)
  }

  async createRun(run: WorkflowRun): Promise<CreateRunResult> {
    const findExistingRun = (): WorkflowRunRow | null => {
      if (!run.idempotencyKey) {
        return null
      }

      return this.get<WorkflowRunRow>(
        `SELECT * FROM workflow_runs
         WHERE workflow = ? AND idempotency_key = ?
         LIMIT 1`,
        run.workflow,
        run.idempotencyKey,
      )
    }

    return this.transaction((): CreateRunResult => {
      const existing = findExistingRun()
      if (existing) {
        return {
          run: mapWorkflowRunRow(existing),
          created: false,
        }
      }

      const serializedInput = serializePersistedValue(run.input, 'Workflow input')

      try {
        this.db
          .prepare(
            `INSERT INTO workflow_runs (id, workflow, input, idempotency_key, lease_id, status, created_at, updated_at)
             VALUES (?, ?, ?, ?, NULL, ?, ?, ?)`,
          )
          .run(
            run.id,
            run.workflow,
            serializedInput,
            run.idempotencyKey,
            run.status,
            run.createdAt,
            run.updatedAt,
          )
      } catch (error) {
        if (run.idempotencyKey && isUniqueConstraintError(error)) {
          const racedExisting = findExistingRun()
          if (racedExisting) {
            return {
              run: mapWorkflowRunRow(racedExisting),
              created: false,
            }
          }
        }

        throw error
      }

      return {
        run: {
          ...run,
          input: deserializePersistedValue(serializedInput),
        },
        created: true,
      }
    })
  }

  async claimNextRun(workflowNames: readonly string[], staleBefore?: number): Promise<ClaimedRun | null> {
    if (workflowNames.length === 0) {
      return null
    }

    const placeholders = workflowNames.map(() => '?').join(', ')

    return this.transaction(() => {
      const now = Date.now()
      const conditions = [
        `status = 'pending'`,
        `(status IN ('sleeping', 'waiting') AND wake_at IS NOT NULL AND wake_at <= ?)`,
      ]
      const clauseParams: number[] = [now]
      if (staleBefore !== undefined) {
        conditions.push(`(status = 'running' AND updated_at <= ?)`)
        clauseParams.push(staleBefore)
      }
      const reclaimClause = `(${conditions.join(' OR ')})`

      const row = this.get<WorkflowRunRow>(
        `SELECT * FROM workflow_runs
         WHERE workflow IN (${placeholders}) AND ${reclaimClause}
         ORDER BY CASE status WHEN 'pending' THEN 0 ELSE 1 END ASC, created_at ASC, rowid ASC
         LIMIT 1`,
        ...workflowNames,
        ...clauseParams,
      )

      if (!row) {
        return null
      }

      const leaseId = randomUUID()
      const changes = this.run(
        `UPDATE workflow_runs
         SET status = 'running', lease_id = ?, wake_at = NULL, updated_at = ?
         WHERE id = ? AND ${reclaimClause}`,
        leaseId,
        now,
        row.id,
        ...clauseParams,
      )

      if (changes === 0) {
        return null
      }

      return {
        ...mapWorkflowRunRow(row),
        status: 'running' as RunStatus,
        updatedAt: now,
        leaseId,
      }
    })
  }

  async heartbeatRun(runId: string, leaseId: string): Promise<boolean> {
    const changes = this.run(
      `UPDATE workflow_runs
       SET updated_at = ?
       WHERE id = ? AND status = 'running' AND lease_id = ?`,
      Date.now(),
      runId,
      leaseId,
    )

    return changes > 0
  }

  async sleepRun(runId: string, leaseId: string, wakeAt: number): Promise<boolean> {
    const changes = this.run(
      `UPDATE workflow_runs
       SET status = 'sleeping', lease_id = NULL, wake_at = ?, updated_at = ?
       WHERE id = ? AND status = 'running' AND lease_id = ?`,
      wakeAt,
      Date.now(),
      runId,
      leaseId,
    )

    return changes > 0
  }

  async waitRun(runId: string, leaseId: string, eventName: string, wakeAt: number | null): Promise<boolean> {
    return this.transaction(() => {
      const held = this.get<{ _: number }>(
        `SELECT 1 AS _ FROM workflow_runs WHERE id = ? AND status = 'running' AND lease_id = ?`,
        runId,
        leaseId,
      )
      if (!held) {
        return false
      }

      // If a matching event is already buffered, stay reclaimable instead of
      // waiting — closes the deliver-during-suspend race.
      const buffered = this.get<{ _: number }>(
        `SELECT 1 AS _ FROM workflow_events WHERE run_id = ? AND event_name = ? LIMIT 1`,
        runId,
        eventName,
      )

      if (buffered) {
        this.run(
          `UPDATE workflow_runs SET status = 'pending', lease_id = NULL, wake_at = NULL, updated_at = ? WHERE id = ?`,
          Date.now(),
          runId,
        )
      } else {
        this.run(
          `UPDATE workflow_runs SET status = 'waiting', lease_id = NULL, wake_at = ?, updated_at = ? WHERE id = ?`,
          wakeAt,
          Date.now(),
          runId,
        )
      }
      return true
    })
  }

  async deliverEvent(runId: string, eventName: string, payload: PersistedValue): Promise<boolean> {
    return this.transaction(() => {
      const exists = this.get<{ _: number }>(`SELECT 1 AS _ FROM workflow_runs WHERE id = ?`, runId)
      if (!exists) {
        return false
      }

      this.run(
        `INSERT INTO workflow_events (id, run_id, event_name, payload, created_at) VALUES (?, ?, ?, ?, ?)`,
        randomUUID(),
        runId,
        eventName,
        serializePersistedValue(payload, 'Event payload'),
        Date.now(),
      )

      this.run(
        `UPDATE workflow_runs SET status = 'pending', lease_id = NULL, wake_at = NULL, updated_at = ? WHERE id = ? AND status = 'waiting'`,
        Date.now(),
        runId,
      )
      return true
    })
  }

  async takeEvent(runId: string, eventName: string): Promise<{ payload: PersistedValue } | null> {
    return this.transaction((): { payload: PersistedValue } | null => {
      const row = this.get<{ id: string; payload: string | null }>(
        `SELECT id, payload FROM workflow_events WHERE run_id = ? AND event_name = ? ORDER BY created_at ASC, rowid ASC LIMIT 1`,
        runId,
        eventName,
      )
      if (!row) {
        return null
      }
      this.run(`DELETE FROM workflow_events WHERE id = ?`, row.id)
      return { payload: row.payload === null ? null : deserializePersistedValue(row.payload) }
    })
  }

  async getRun(runId: string): Promise<WorkflowRun | null> {
    const row = this.get<WorkflowRunRow>(`SELECT * FROM workflow_runs WHERE id = ?`, runId)
    return row ? mapWorkflowRunRow(row) : null
  }

  async listRuns(filter: ListRunsFilter = {}): Promise<WorkflowRun[]> {
    const { status, workflow, limit = 100, before, beforeId } = filter
    const conditions: string[] = []
    const args: SqlParam[] = []

    if (status !== undefined) {
      conditions.push('status = ?')
      args.push(status)
    }
    if (workflow !== undefined) {
      conditions.push('workflow = ?')
      args.push(workflow)
    }
    if (before !== undefined) {
      // Keyset cursor over the (created_at DESC, id DESC) order.
      if (beforeId !== undefined) {
        conditions.push('(created_at < ? OR (created_at = ? AND id < ?))')
        args.push(before, before, beforeId)
      } else {
        conditions.push('created_at < ?')
        args.push(before)
      }
    }

    const where = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : ''
    args.push(limit)

    const rows = this.all<WorkflowRunRow>(
      `SELECT * FROM workflow_runs ${where}
       ORDER BY created_at DESC, id DESC
       LIMIT ?`,
      ...args,
    )

    return rows.map(mapWorkflowRunRow)
  }

  async requeueRun(runId: string): Promise<boolean> {
    return this.transaction(() => {
      const changes = this.run(
        `UPDATE workflow_runs
         SET status = 'pending', lease_id = NULL, updated_at = ?
         WHERE id = ? AND status IN ('failed', 'cancelled')`,
        Date.now(),
        runId,
      )

      if (changes === 0) {
        return false
      }

      this.run(`DELETE FROM workflow_steps WHERE run_id = ? AND status = 'failed'`, runId)

      return true
    })
  }

  async getStepResults(runId: string): Promise<StepResult[]> {
    const rows = this.all<WorkflowStepRow>(
      `SELECT * FROM workflow_steps WHERE run_id = ? ORDER BY created_at ASC`,
      runId,
    )

    return rows.map((row): StepResult => ({
      id: row.id,
      runId: row.run_id,
      name: row.name,
      status: row.status as StepResult['status'],
      output: row.output === null ? null : deserializePersistedValue(row.output),
      error: row.error,
      attempts: row.attempts,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
    }))
  }

  async saveStepResult(result: StepResult, leaseId?: string): Promise<boolean> {
    return this.transaction(() => {
      if (leaseId) {
        const run = this.get<{ _: number }>(
          `SELECT 1 AS _ FROM workflow_runs
           WHERE id = ? AND status = 'running' AND lease_id = ?`,
          result.runId,
          leaseId,
        )

        if (!run) {
          return false
        }
      }

      this.run(
        `INSERT OR REPLACE INTO workflow_steps (id, run_id, name, status, output, error, attempts, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        result.id,
        result.runId,
        result.name,
        result.status,
        serializePersistedValue(result.output, 'Step output'),
        result.error,
        result.attempts,
        result.createdAt,
        result.updatedAt,
      )

      return true
    })
  }

  async updateRunStatus(runId: string, status: RunStatus): Promise<boolean> {
    const changes = this.run(
      `UPDATE workflow_runs
       SET status = ?, lease_id = ?, wake_at = NULL, updated_at = ?
       WHERE id = ?`,
      status,
      null,
      Date.now(),
      runId,
    )

    return changes > 0
  }

  async updateClaimedRunStatus(runId: string, leaseId: string, status: RunStatus): Promise<boolean> {
    const changes = this.run(
      `UPDATE workflow_runs
       SET status = ?, lease_id = ?, wake_at = NULL, updated_at = ?
       WHERE id = ? AND status = 'running' AND lease_id = ?`,
      status,
      status === 'running' ? leaseId : null,
      Date.now(),
      runId,
      leaseId,
    )

    return changes > 0
  }

  /**
   * Re-registering preserves `next_run_at` unless the interval changed, so a
   * redeploy rejoins the existing cadence instead of pushing the next firing
   * out by a full interval each time.
   */
  async upsertSchedule(schedule: WorkflowSchedule): Promise<WorkflowSchedule> {
    return this.transaction(() => {
      this.run(
        `INSERT INTO workflow_schedules (key, workflow, input, interval_ms, cron, next_run_at, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(key) DO UPDATE SET
           workflow    = excluded.workflow,
           input       = excluded.input,
           interval_ms = excluded.interval_ms,
           cron        = excluded.cron,
           -- Keep the existing cadence unless the recurrence itself changed, so
           -- a redeploy does not push the next firing out every time. IS rather
           -- than = so a NULL on either side compares correctly.
           next_run_at = CASE WHEN workflow_schedules.interval_ms IS excluded.interval_ms
                               AND workflow_schedules.cron IS excluded.cron
                              THEN workflow_schedules.next_run_at
                              ELSE excluded.next_run_at END,
           updated_at  = excluded.updated_at`,
        schedule.key,
        schedule.workflow,
        serializePersistedValue(schedule.input, 'Schedule input'),
        schedule.recurrence.kind === 'interval' ? schedule.recurrence.intervalMs : null,
        schedule.recurrence.kind === 'cron' ? schedule.recurrence.expression : null,
        schedule.nextRunAt,
        schedule.createdAt,
        schedule.updatedAt,
      )

      const row = this.get<WorkflowScheduleRow>(
        `SELECT * FROM workflow_schedules WHERE key = ?`,
        schedule.key,
      )

      if (!row) {
        throw new InternalError(`Schedule "${schedule.key}" vanished immediately after upsert`)
      }

      return mapWorkflowScheduleRow(row)
    })
  }

  /**
   * Selecting and advancing inside one transaction is what makes a claim
   * exclusive: a second instance reaching this concurrently either blocks on
   * the write lock and then sees the advanced `next_run_at`, or loses the
   * guarded UPDATE and reports nothing due.
   */
  async claimDueSchedule(
    workflowNames: readonly string[],
    now: number,
  ): Promise<WorkflowSchedule | null> {
    if (workflowNames.length === 0) {
      return null
    }

    const placeholders = workflowNames.map(() => '?').join(', ')
    return this.transaction(() => {
      const row = this.get<WorkflowScheduleRow>(`SELECT * FROM workflow_schedules
         WHERE next_run_at <= ? AND workflow IN (${placeholders})
         ORDER BY next_run_at ASC, key ASC LIMIT 1`, now, ...workflowNames)
      if (!row) {
        return null
      }

      const changes = this.run(
        `UPDATE workflow_schedules SET next_run_at = ?, updated_at = ? WHERE key = ? AND next_run_at = ?`,
        nextOccurrence(row.next_run_at, rowRecurrence(row), now),
        now,
        row.key,
        row.next_run_at,
      )

      // Guarded on the slot we read, so a racing claim cannot fire it twice.
      if (changes === 0) {
        return null
      }

      return mapWorkflowScheduleRow(row)
    })
  }

  async deleteSchedule(key: string): Promise<boolean> {
    return this.run(`DELETE FROM workflow_schedules WHERE key = ?`, key) > 0
  }

  async listSchedules(): Promise<WorkflowSchedule[]> {
    return this
      .all<WorkflowScheduleRow>(`SELECT * FROM workflow_schedules ORDER BY key ASC`)
      .map(mapWorkflowScheduleRow)
  }

  close(): void {
    this.db.close()
  }

  // --- node:sqlite helpers -------------------------------------------------
  // node:sqlite has no transaction() helper and run() returns { changes },
  // so these wrap the StatementSync API to match the shape the methods expect.

  private prepare(sql: string): StatementSync {
    return this.db.prepare(sql)
  }

  private get<T>(sql: string, ...params: SqlParam[]): T | null {
    return (this.prepare(sql).get(...params) as T | undefined) ?? null
  }

  private all<T>(sql: string, ...params: SqlParam[]): T[] {
    return this.prepare(sql).all(...params) as T[]
  }

  private run(sql: string, ...params: SqlParam[]): number {
    return Number(this.prepare(sql).run(...params).changes)
  }

  private transaction<T>(fn: () => T): T {
    // BEGIN IMMEDIATE takes the write lock up front. A deferred BEGIN that does
    // a SELECT then an UPDATE (e.g. claimNextRun) can hit SQLITE_BUSY_SNAPSHOT
    // under concurrent writers in WAL mode — which busy_timeout does NOT retry.
    // The better-sqlite3 and bun adapters get this via their native helpers.
    this.db.exec('BEGIN IMMEDIATE')
    try {
      const result = fn()
      this.db.exec('COMMIT')
      return result
    } catch (error) {
      this.db.exec('ROLLBACK')
      throw error
    }
  }
}

// node:sqlite binds null as a value but typings model it via SupportedValueType.
type SqlParam = string | number | bigint | null | Uint8Array

function mapWorkflowRunRow(row: WorkflowRunRow): WorkflowRun {
  return {
    id: row.id,
    workflow: row.workflow,
    input: deserializePersistedValue(row.input),
    idempotencyKey: row.idempotency_key,
    status: row.status as RunStatus,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  }
}

function isUniqueConstraintError(error: unknown): boolean {
  if (!(error instanceof Error)) {
    return false
  }

  const code = (error as { code?: string }).code
  const errcode = (error as { errcode?: number }).errcode
  return code === 'SQLITE_CONSTRAINT_UNIQUE'
    || errcode === 2067 // SQLITE_CONSTRAINT_UNIQUE (extended result code)
    || errcode === 19 // SQLITE_CONSTRAINT (primary result code)
    || error.message.includes('UNIQUE constraint failed')
}

/** The recurrence a schedule row describes: `cron` when set, otherwise the interval. */
function rowRecurrence(row: WorkflowScheduleRow): ScheduleRecurrence {
  return row.cron !== null
    ? { kind: 'cron', expression: row.cron }
    : { kind: 'interval', intervalMs: row.interval_ms ?? 0 }
}

function mapWorkflowScheduleRow(row: WorkflowScheduleRow): WorkflowSchedule {
  return {
    key: row.key,
    workflow: row.workflow,
    input: deserializePersistedValue(row.input),
    recurrence: rowRecurrence(row),
    nextRunAt: row.next_run_at,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  }
}
