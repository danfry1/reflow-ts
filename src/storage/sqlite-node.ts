import { randomUUID } from 'node:crypto'
import Database from 'better-sqlite3'
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

/**
 * SQLite-backed storage adapter. Uses WAL mode for concurrent reads and
 * transactional claiming for safe multi-process access.
 *
 * @example
 * ```ts
 * const storage = new SQLiteStorage('./reflow.db')
 * await storage.initialize()
 * ```
 */
export class SQLiteStorage implements StorageAdapter {
  private db: Database.Database

  constructor(path: string) {
    this.db = new Database(path)
    this.db.pragma('journal_mode = WAL')
    this.db.pragma('synchronous = NORMAL')
    this.db.pragma('busy_timeout = 5000')
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
    const columns = this.db.prepare(`PRAGMA table_info(workflow_runs)`).all() as { name: string }[]
    if (!columns.some((column) => column.name === 'wake_at')) {
      this.db.exec(`ALTER TABLE workflow_runs ADD COLUMN wake_at INTEGER`)
    }


    // 0.6.0 shipped workflow_schedules with a NOT NULL interval_ms and no cron
    // column. Adding cron alone is not enough: a cron schedule leaves
    // interval_ms null, which that constraint rejects. SQLite cannot drop a NOT
    // NULL in place, so the table is rebuilt — cheap, since schedules are few.
    const scheduleColumns = this.db.prepare(`PRAGMA table_info(workflow_schedules)`).all() as { name: string }[]
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
    const findExistingRun = (): WorkflowRunRow | undefined => {
      if (!run.idempotencyKey) {
        return undefined
      }

      return this.db
        .prepare(
          `SELECT * FROM workflow_runs
           WHERE workflow = ? AND idempotency_key = ?
           LIMIT 1`,
        )
        .get(run.workflow, run.idempotencyKey) as WorkflowRunRow | undefined
    }

    const create = this.db.transaction((): CreateRunResult => {
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

    return create()
  }

  async claimNextRun(workflowNames: readonly string[], staleBefore?: number): Promise<ClaimedRun | null> {
    if (workflowNames.length === 0) {
      return null
    }

    const placeholders = workflowNames.map(() => '?').join(', ')
    const claim = this.db.transaction(() => {
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

      const row = this.db
        .prepare(
          `SELECT * FROM workflow_runs
           WHERE workflow IN (${placeholders}) AND ${reclaimClause}
           ORDER BY CASE status WHEN 'pending' THEN 0 ELSE 1 END ASC, created_at ASC, rowid ASC
           LIMIT 1`,
        )
        .get(...workflowNames, ...clauseParams) as WorkflowRunRow | undefined

      if (!row) {
        return null
      }

      const leaseId = randomUUID()
      const result = this.db
        .prepare(
          `UPDATE workflow_runs
           SET status = 'running', lease_id = ?, wake_at = NULL, updated_at = ?
           WHERE id = ? AND ${reclaimClause}`,
        )
        .run(leaseId, now, row.id, ...clauseParams)

      if (result.changes === 0) {
        return null
      }

      return {
        ...mapWorkflowRunRow(row),
        status: 'running' as RunStatus,
        updatedAt: now,
        leaseId,
      }
    })

    return claim()
  }

  async heartbeatRun(runId: string, leaseId: string): Promise<boolean> {
    const result = this.db
      .prepare(
        `UPDATE workflow_runs
         SET updated_at = ?
         WHERE id = ? AND status = 'running' AND lease_id = ?`,
      )
      .run(Date.now(), runId, leaseId)

    return result.changes > 0
  }

  async sleepRun(runId: string, leaseId: string, wakeAt: number): Promise<boolean> {
    const result = this.db
      .prepare(
        `UPDATE workflow_runs
         SET status = 'sleeping', lease_id = NULL, wake_at = ?, updated_at = ?
         WHERE id = ? AND status = 'running' AND lease_id = ?`,
      )
      .run(wakeAt, Date.now(), runId, leaseId)

    return result.changes > 0
  }

  async waitRun(runId: string, leaseId: string, eventName: string, wakeAt: number | null): Promise<boolean> {
    const wait = this.db.transaction(() => {
      const held = this.db
        .prepare(`SELECT 1 FROM workflow_runs WHERE id = ? AND status = 'running' AND lease_id = ?`)
        .get(runId, leaseId)
      if (!held) {
        return false
      }

      // If a matching event is already buffered, stay reclaimable instead of
      // waiting — closes the deliver-during-suspend race.
      const buffered = this.db
        .prepare(`SELECT 1 FROM workflow_events WHERE run_id = ? AND event_name = ? LIMIT 1`)
        .get(runId, eventName)

      if (buffered) {
        this.db
          .prepare(`UPDATE workflow_runs SET status = 'pending', lease_id = NULL, wake_at = NULL, updated_at = ? WHERE id = ?`)
          .run(Date.now(), runId)
      } else {
        this.db
          .prepare(`UPDATE workflow_runs SET status = 'waiting', lease_id = NULL, wake_at = ?, updated_at = ? WHERE id = ?`)
          .run(wakeAt, Date.now(), runId)
      }
      return true
    })

    return wait()
  }

  async deliverEvent(runId: string, eventName: string, payload: PersistedValue): Promise<boolean> {
    const deliver = this.db.transaction(() => {
      const exists = this.db.prepare(`SELECT 1 FROM workflow_runs WHERE id = ?`).get(runId)
      if (!exists) {
        return false
      }

      this.db
        .prepare(`INSERT INTO workflow_events (id, run_id, event_name, payload, created_at) VALUES (?, ?, ?, ?, ?)`)
        .run(randomUUID(), runId, eventName, serializePersistedValue(payload, 'Event payload'), Date.now())

      // Wake the run if it is currently waiting (for this or any event; a
      // mismatched wake simply re-suspends after re-checking).
      this.db
        .prepare(`UPDATE workflow_runs SET status = 'pending', lease_id = NULL, wake_at = NULL, updated_at = ? WHERE id = ? AND status = 'waiting'`)
        .run(Date.now(), runId)
      return true
    })

    return deliver()
  }

  async takeEvent(runId: string, eventName: string): Promise<{ payload: PersistedValue } | null> {
    const take = this.db.transaction((): { payload: PersistedValue } | null => {
      const row = this.db
        .prepare(`SELECT id, payload FROM workflow_events WHERE run_id = ? AND event_name = ? ORDER BY created_at ASC, rowid ASC LIMIT 1`)
        .get(runId, eventName) as { id: string; payload: string | null } | undefined
      if (!row) {
        return null
      }
      this.db.prepare(`DELETE FROM workflow_events WHERE id = ?`).run(row.id)
      return { payload: row.payload === null ? null : deserializePersistedValue(row.payload) }
    })

    return take()
  }

  async getRun(runId: string): Promise<WorkflowRun | null> {
    const row = this.db
      .prepare(`SELECT * FROM workflow_runs WHERE id = ?`)
      .get(runId) as WorkflowRunRow | undefined

    return row ? mapWorkflowRunRow(row) : null
  }

  async listRuns(filter: ListRunsFilter = {}): Promise<WorkflowRun[]> {
    const { status, workflow, limit = 100, before, beforeId } = filter
    const conditions: string[] = []
    const args: (string | number)[] = []

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

    const rows = this.db
      .prepare(
        `SELECT * FROM workflow_runs ${where}
         ORDER BY created_at DESC, id DESC
         LIMIT ?`,
      )
      .all(...args) as WorkflowRunRow[]

    return rows.map(mapWorkflowRunRow)
  }

  async requeueRun(runId: string): Promise<boolean> {
    const requeue = this.db.transaction(() => {
      const result = this.db
        .prepare(
          `UPDATE workflow_runs
           SET status = 'pending', lease_id = NULL, updated_at = ?
           WHERE id = ? AND status IN ('failed', 'cancelled')`,
        )
        .run(Date.now(), runId)

      if (result.changes === 0) {
        return false
      }

      this.db
        .prepare(`DELETE FROM workflow_steps WHERE run_id = ? AND status = 'failed'`)
        .run(runId)

      return true
    })

    return requeue()
  }

  async getStepResults(runId: string): Promise<StepResult[]> {
    const rows = this.db
      .prepare(`SELECT * FROM workflow_steps WHERE run_id = ? ORDER BY created_at ASC`)
      .all(runId) as WorkflowStepRow[]

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
    const save = this.db.transaction(() => {
      if (leaseId) {
        const run = this.db
          .prepare(
            `SELECT 1 FROM workflow_runs
             WHERE id = ? AND status = 'running' AND lease_id = ?`,
          )
          .get(result.runId, leaseId)

        if (!run) {
          return false
        }
      }

      this.db
        .prepare(
          `INSERT OR REPLACE INTO workflow_steps (id, run_id, name, status, output, error, attempts, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        )
        .run(
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

    return save()
  }

  async updateRunStatus(runId: string, status: RunStatus): Promise<boolean> {
    const result = this.db
      .prepare(
        `UPDATE workflow_runs
         SET status = ?, lease_id = ?, wake_at = NULL, updated_at = ?
         WHERE id = ?`,
      )
      .run(status, null, Date.now(), runId)

    return result.changes > 0
  }

  async updateClaimedRunStatus(runId: string, leaseId: string, status: RunStatus): Promise<boolean> {
    const result = this.db
      .prepare(
        `UPDATE workflow_runs
         SET status = ?, lease_id = ?, wake_at = NULL, updated_at = ?
         WHERE id = ? AND status = 'running' AND lease_id = ?`,
      )
      .run(status, status === 'running' ? leaseId : null, Date.now(), runId, leaseId)

    return result.changes > 0
  }

  /**
   * Re-registering preserves `next_run_at` unless the interval changed, so a
   * redeploy rejoins the existing cadence instead of pushing the next firing
   * out by a full interval each time.
   */
  async upsertSchedule(schedule: WorkflowSchedule): Promise<WorkflowSchedule> {
    const upsert = this.db.transaction(() => {
      this.db.prepare(`INSERT INTO workflow_schedules (key, workflow, input, interval_ms, cron, next_run_at, created_at, updated_at)
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
           updated_at  = excluded.updated_at`).run(
        schedule.key,
        schedule.workflow,
        serializePersistedValue(schedule.input, 'Schedule input'),
        schedule.recurrence.kind === 'interval' ? schedule.recurrence.intervalMs : null,
        schedule.recurrence.kind === 'cron' ? schedule.recurrence.expression : null,
        schedule.nextRunAt,
        schedule.createdAt,
        schedule.updatedAt,
      )

      return this.db
        .prepare(`SELECT * FROM workflow_schedules WHERE key = ?`)
        .get(schedule.key) as WorkflowScheduleRow
    })

    return mapWorkflowScheduleRow(upsert())
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
    const claim = this.db.transaction(() => {
      const row = this.db.prepare(`SELECT * FROM workflow_schedules
         WHERE next_run_at <= ? AND workflow IN (${placeholders})
         ORDER BY next_run_at ASC, key ASC LIMIT 1`).get(now, ...workflowNames) as WorkflowScheduleRow | undefined
      if (!row) {
        return null
      }

      const result = this.db
        .prepare(`UPDATE workflow_schedules SET next_run_at = ?, updated_at = ? WHERE key = ? AND next_run_at = ?`)
        .run(nextOccurrence(row.next_run_at, rowRecurrence(row), now), now, row.key, row.next_run_at)

      // Guarded on the slot we read, so a racing claim cannot fire it twice.
      if (result.changes === 0) {
        return null
      }

      return row
    })

    const claimed = claim()
    return claimed ? mapWorkflowScheduleRow(claimed) : null
  }

  async deleteSchedule(key: string): Promise<boolean> {
    const result = this.db.prepare(`DELETE FROM workflow_schedules WHERE key = ?`).run(key)
    return result.changes > 0
  }

  async listSchedules(): Promise<WorkflowSchedule[]> {
    const rows = this.db
      .prepare(`SELECT * FROM workflow_schedules ORDER BY key ASC`)
      .all() as WorkflowScheduleRow[]

    return rows.map(mapWorkflowScheduleRow)
  }

  close(): void {
    this.db.close()
  }

}

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

  return (error as { code?: string }).code === 'SQLITE_CONSTRAINT_UNIQUE'
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
