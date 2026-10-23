import { randomUUID } from 'node:crypto'
import type {
  ClaimedRun,
  CountRunsFilter,
  CreateRunResult,
  DeleteRunsFilter,
  PersistedValue,
  ListRunsFilter,
  RunActivity,
  RunActivityFilter,
  RunCounts,
  RunError,
  StepResult,
  StorageAdapter,
  WorkflowRun,
  WorkflowSchedule,
  ScheduleRecurrence,
} from '../../core/types'
import { emptyRunCounts, parseRunError, parseRunStatus, parseStepHistory, parseStepStatus } from '../records'
import { pendingMigrations, SCHEMA_VERSION } from './schema'
import { deserializePersistedValue, serializePersistedValue } from '../codec'
import { nextOccurrence } from '../../core/schedule-timing'
import { InternalError } from '../../core/errors'
import type { SqliteDriver, SqlParam } from './driver'

interface WorkflowRunRow {
  id: string
  workflow: string
  input: string
  idempotency_key: string | null
  lease_id: string | null
  status: string
  created_at: number
  updated_at: number
  wake_at: number | null
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

/** Options for the SQLite adapters' constructors. */
export interface SQLiteStorageOptions {
  /**
   * Open the database for reading only: every write fails, `initialize()`
   * included, and the file is left exactly as it was — no journal-mode change.
   * The file must already exist. For inspecting a database another process
   * owns, or a backup.
   */
  readOnly?: boolean
}

/**
 * The SQLite storage adapter, independent of which binding executes the SQL.
 *
 * Each public `SQLiteStorage` (`reflow-ts/sqlite-node`, `/sqlite-bun`,
 * `/sqlite-node-builtin`) is this class constructed with a driver for its
 * binding. Uses WAL mode for concurrent readers, and `BEGIN IMMEDIATE` for
 * every transaction so several processes can safely share one database file.
 */
export class SqliteStorageBase implements StorageAdapter {
  private readonly driver: SqliteDriver

  constructor(driver: SqliteDriver, options: SQLiteStorageOptions = {}) {
    this.driver = driver
    // First, so that even switching the journal mode waits for a lock held by
    // another process opening the same file, instead of failing at once.
    this.driver.exec('PRAGMA busy_timeout = 5000')
    if (options.readOnly) {
      // Refuses every write on this connection, whatever the binding's own
      // read-only flag does or does not honour. The journal-mode pragma below
      // would itself be a persistent write to the file, so it is skipped too.
      this.driver.exec('PRAGMA query_only = ON')
      return
    }
    this.driver.exec('PRAGMA journal_mode = WAL')
    this.driver.exec('PRAGMA synchronous = NORMAL')
  }

  /**
   * Bring the database up to {@link SCHEMA_VERSION}, applying each pending
   * migration in its own transaction.
   *
   * The version is re-read once the write lock is held, so processes starting
   * against one file at the same moment apply each migration exactly once
   * between them rather than racing to repeat it.
   */
  async initialize(): Promise<void> {
    for (;;) {
      const applied = this.transaction(() => {
        const [next] = pendingMigrations(this.currentSchemaVersion())
        if (!next) {
          return false
        }

        next.up({
          exec: (sql) => this.driver.exec(sql),
          columns: (table) => this.all<{ name: string }>(`PRAGMA table_info(${table})`).map((column) => column.name),
        })
        this.driver.exec(`PRAGMA user_version = ${next.version}`)
        return true
      })

      if (!applied) {
        return
      }
    }
  }

  /**
   * The database's schema version and the version this build writes. When
   * `current` is behind `latest`, `initialize()` will migrate the database.
   */
  schemaVersion(): { current: number; latest: number } {
    return { current: this.currentSchemaVersion(), latest: SCHEMA_VERSION }
  }

  private currentSchemaVersion(): number {
    return this.get<{ user_version: number }>('PRAGMA user_version')?.user_version ?? 0
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
        this.run(
          `INSERT INTO workflow_runs
             (id, workflow, input, idempotency_key, lease_id, status, created_at, updated_at,
              wake_at, started_at, finished_at, output, error, waiting_for)
           VALUES (?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          run.id,
          run.workflow,
          serializedInput,
          run.idempotencyKey,
          run.status,
          run.createdAt,
          run.updatedAt,
          run.wakeAt,
          run.startedAt,
          run.finishedAt,
          serializeRunOutput(run),
          serializeRunError(run.error),
          run.waitingFor,
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
      // Claim in the order runs became due, so a steady stream of new pending
      // runs cannot starve a woken sleeper or a crashed run awaiting reclaim.
      // A stale run became due when its lease expired, `staleLeaseMs` after
      // its last heartbeat.
      const staleLeaseMs = staleBefore === undefined ? 0 : now - staleBefore

      // `+workflow` keeps the planner off the indexes that lead with workflow,
      // which would scan every run of the workflow, finished ones included;
      // the status indexes reach only the runs that could be claimed, so a
      // claim costs the size of the queue, not of the history.
      const row = this.get<WorkflowRunRow>(
        `SELECT * FROM workflow_runs
         WHERE +workflow IN (${placeholders}) AND ${reclaimClause}
         ORDER BY CASE status
                    WHEN 'pending' THEN updated_at
                    WHEN 'running' THEN updated_at + ?
                    ELSE wake_at
                  END ASC, created_at ASC, rowid ASC
         LIMIT 1`,
        ...workflowNames,
        ...clauseParams,
        staleLeaseMs,
      )

      if (!row) {
        return null
      }

      const leaseId = randomUUID()
      const changes = this.run(
        `UPDATE workflow_runs
         SET status = 'running', lease_id = ?, wake_at = NULL, waiting_for = NULL, updated_at = ?,
             started_at = COALESCE(started_at, ?)
         WHERE id = ? AND ${reclaimClause}`,
        leaseId,
        now,
        now,
        row.id,
        ...clauseParams,
      )

      if (changes === 0) {
        return null
      }

      const claimed: ClaimedRun = {
        ...mapWorkflowRunRow(row),
        status: 'running',
        updatedAt: now,
        startedAt: row.started_at ?? now,
        wakeAt: null,
        waitingFor: null,
        leaseId,
      }
      return claimed
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
          `UPDATE workflow_runs SET status = 'pending', lease_id = NULL, wake_at = NULL, waiting_for = NULL, updated_at = ? WHERE id = ?`,
          Date.now(),
          runId,
        )
      } else {
        this.run(
          `UPDATE workflow_runs SET status = 'waiting', lease_id = NULL, wake_at = ?, waiting_for = ?, updated_at = ? WHERE id = ?`,
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
        `UPDATE workflow_runs SET status = 'pending', lease_id = NULL, wake_at = NULL, waiting_for = NULL, updated_at = ? WHERE id = ? AND status = 'waiting'`,
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
         SET status = 'pending', lease_id = NULL, updated_at = ?,
             finished_at = NULL, output = NULL, error = NULL
         WHERE id = ? AND status IN ('failed', 'cancelled')`,
        Date.now(),
        runId,
      )

      if (changes === 0) {
        return false
      }

      // A failed step runs again from scratch, and so does one that was waiting
      // out a backoff when the run was cancelled: its attempts start over.
      this.run(`DELETE FROM workflow_steps WHERE run_id = ? AND status IN ('failed', 'retrying')`, runId)

      return true
    })
  }

  async getStepResults(runId: string): Promise<StepResult[]> {
    const rows = this.all<WorkflowStepRow>(
      `SELECT * FROM workflow_steps WHERE run_id = ? ORDER BY created_at ASC`,
      runId,
    )

    return rows.map(mapStepRow)
  }

  async getStepResultsForRuns(runIds: readonly string[]): Promise<Map<string, StepResult[]>> {
    const byRun = new Map<string, StepResult[]>(runIds.map((id) => [id, []]))
    // In chunks, well under SQLite's limit on bound parameters.
    for (let offset = 0; offset < runIds.length; offset += 500) {
      const chunk = runIds.slice(offset, offset + 500)
      const rows = this.all<WorkflowStepRow>(
        `SELECT * FROM workflow_steps WHERE run_id IN (${chunk.map(() => '?').join(', ')})
         ORDER BY run_id, created_at ASC`,
        ...chunk,
      )
      for (const row of rows) {
        byRun.get(row.run_id)?.push(mapStepRow(row))
      }
    }
    return byRun
  }

  async runActivity(filter: RunActivityFilter): Promise<RunActivity> {
    const { since, bucketMs, buckets } = filter
    const until = since + bucketMs * buckets
    const scope = filter.workflow === undefined ? '' : 'AND workflow = ?'
    const scoped = (...args: SqlParam[]): SqlParam[] => filter.workflow === undefined ? args : [...args, filter.workflow]
    const activity: RunActivity = {
      started: new Array<number>(buckets).fill(0),
      completed: new Array<number>(buckets).fill(0),
      failed: new Array<number>(buckets).fill(0),
    }
    const started = this.all<{ bucket: number; n: number }>(
      `SELECT CAST((created_at - ?) / ? AS INTEGER) AS bucket, COUNT(*) AS n FROM workflow_runs
       WHERE created_at >= ? AND created_at < ? ${scope} GROUP BY bucket`,
      ...scoped(since, bucketMs, since, until),
    )
    for (const row of started) activity.started[row.bucket] = row.n
    const finished = this.all<{ bucket: number; status: string; n: number }>(
      `SELECT CAST((finished_at - ?) / ? AS INTEGER) AS bucket, status, COUNT(*) AS n FROM workflow_runs
       WHERE finished_at >= ? AND finished_at < ? AND status IN ('completed', 'failed') ${scope}
       GROUP BY bucket, status`,
      ...scoped(since, bucketMs, since, until),
    )
    for (const row of finished) {
      const counts = row.status === 'completed' ? activity.completed : activity.failed
      counts[row.bucket] = row.n
    }
    return activity
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
        `INSERT OR REPLACE INTO workflow_steps (id, run_id, name, status, output, error, attempts, created_at, updated_at, started_at, history)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
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
    return this.run(
      `UPDATE workflow_runs
       SET status = 'completed', lease_id = NULL, wake_at = NULL, waiting_for = NULL,
           output = ?, updated_at = ?, finished_at = ?
       WHERE id = ? AND status = 'running' AND lease_id = ?`,
      serializePersistedValue(output, 'Run output'),
      now,
      now,
      runId,
      leaseId,
    ) > 0
  }

  async failRun(runId: string, leaseId: string, error: RunError): Promise<boolean> {
    const now = Date.now()
    return this.run(
      `UPDATE workflow_runs
       SET status = 'failed', lease_id = NULL, wake_at = NULL, waiting_for = NULL,
           error = ?, updated_at = ?, finished_at = ?
       WHERE id = ? AND status = 'running' AND lease_id = ?`,
      serializeRunError(error),
      now,
      now,
      runId,
      leaseId,
    ) > 0
  }

  async cancelRun(runId: string): Promise<boolean> {
    const now = Date.now()
    return this.run(
      `UPDATE workflow_runs
       SET status = 'cancelled', lease_id = NULL, wake_at = NULL, waiting_for = NULL,
           updated_at = ?, finished_at = ?
       WHERE id = ? AND status NOT IN ('completed', 'failed', 'cancelled')`,
      now,
      now,
      runId,
    ) > 0
  }

  async countRuns(filter: CountRunsFilter = {}): Promise<RunCounts> {
    const rows = filter.workflow === undefined
      ? this.all<{ status: string; n: number }>(`SELECT status, COUNT(*) AS n FROM workflow_runs GROUP BY status`)
      : this.all<{ status: string; n: number }>(
        `SELECT status, COUNT(*) AS n FROM workflow_runs WHERE workflow = ? GROUP BY status`,
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

    const placeholders = filter.statuses.map(() => '?').join(', ')
    return this.transaction(() => {
      const ids = this.all<{ id: string }>(
        `SELECT id FROM workflow_runs
         WHERE finished_at < ? AND status IN (${placeholders})
         ORDER BY finished_at ASC, rowid ASC
         LIMIT ?`,
        filter.finishedBefore,
        ...filter.statuses,
        filter.limit,
      ).map((row) => row.id)

      if (ids.length === 0) {
        return 0
      }

      const idList = ids.map(() => '?').join(', ')
      this.run(`DELETE FROM workflow_steps WHERE run_id IN (${idList})`, ...ids)
      this.run(`DELETE FROM workflow_events WHERE run_id IN (${idList})`, ...ids)
      return this.run(`DELETE FROM workflow_runs WHERE id IN (${idList})`, ...ids)
    })
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
    this.driver.close()
  }

  private get<T>(sql: string, ...params: SqlParam[]): T | null {
    return this.driver.get<T>(sql, params) ?? null
  }

  private all<T>(sql: string, ...params: SqlParam[]): T[] {
    return this.driver.all<T>(sql, params)
  }

  private run(sql: string, ...params: SqlParam[]): number {
    return this.driver.run(sql, params)
  }

  /**
   * Run `fn` in a transaction that holds the write lock from its first
   * statement.
   *
   * A deferred `BEGIN` starts as a reader and upgrades on its first write. When
   * another connection commits in between — routine once several processes
   * claim from one file — that upgrade fails with `SQLITE_BUSY_SNAPSHOT`, which
   * `busy_timeout` cannot retry because the transaction's snapshot is already
   * stale. Every read-then-write here (claiming a run, waiting on an event,
   * advancing a schedule) is exactly that shape, so the lock is taken up front
   * and contention becomes a wait rather than an error.
   */
  private transaction<T>(fn: () => T): T {
    this.driver.exec('BEGIN IMMEDIATE')

    let result: T
    try {
      result = fn()
    } catch (error) {
      this.rollbackAfter(error)
      throw error
    }

    try {
      this.driver.exec('COMMIT')
    } catch (error) {
      // A COMMIT that fails (SQLITE_BUSY under a rollback journal, say) leaves
      // the transaction open, and every later BEGIN on this connection would
      // fail. Roll back so the connection stays usable.
      this.rollbackAfter(error)
      throw error
    }
    return result
  }

  /**
   * Roll back after `cause` aborted a transaction.
   *
   * Some failures (a full disk, an I/O error) make SQLite roll back on its own,
   * after which `ROLLBACK` itself fails. That second failure must not replace
   * the error that explains what went wrong, nor be dropped, so both are
   * surfaced together.
   */
  private rollbackAfter(cause: unknown): void {
    try {
      this.driver.exec('ROLLBACK')
    } catch (rollbackError) {
      throw new AggregateError(
        [cause, rollbackError],
        'Transaction failed and could not be rolled back',
        { cause },
      )
    }
  }
}

function mapStepRow(row: WorkflowStepRow): StepResult {
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

function mapWorkflowRunRow(row: WorkflowRunRow): WorkflowRun {
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

/** A run's output is stored only once it completes; any other run stores NULL. */
function serializeRunOutput(run: WorkflowRun): string | null {
  return run.status === 'completed' ? serializePersistedValue(run.output, 'Run output') : null
}

function serializeRunError(error: RunError | null): string | null {
  return error === null ? null : JSON.stringify(error)
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
