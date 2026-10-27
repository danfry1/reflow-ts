import { randomUUID } from 'node:crypto'
import type {
  CachedStep,
  ConsumeEventResult,
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
  RequeueOptions,
} from '../../core/types'
import { RUN_STATUSES } from '../../core/types'
import { emptyRunCounts, parseRunError, parseRunStatus, parseStepHistory, parseStepKind, parseStepStatus } from '../records'
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
  kind: string | null
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
  readonly supportsConcurrencyLimits = true
  readonly supportsKeepFailed = true
  readonly supportsEventNames = true
  private readonly driver: SqliteDriver

  constructor(driver: SqliteDriver, options: SQLiteStorageOptions = {}) {
    this.driver = driver
    // First, so that even switching the journal mode waits for a lock held by
    // another process opening the same file, instead of failing at once.
    this.driver.exec(`PRAGMA busy_timeout = ${BUSY_TIMEOUT_MS}`)
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
    // Another process may be migrating a large database, which holds the
    // write lock for seconds — building version 4's indexes takes a few per
    // million runs — so starting up waits longer for it than a claim would.
    this.driver.exec(`PRAGMA busy_timeout = ${MIGRATION_BUSY_TIMEOUT_MS}`)
    try {
      await this.migrate()
    } finally {
      this.driver.exec(`PRAGMA busy_timeout = ${BUSY_TIMEOUT_MS}`)
    }
  }

  private async migrate(): Promise<void> {
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

  async claimNextRun(
    allWorkflowNames: readonly string[],
    staleBefore?: number,
    limits?: ReadonlyMap<string, number>,
  ): Promise<ClaimedRun | null> {
    if (allWorkflowNames.length === 0) {
      return null
    }

    return this.transaction(() => {
      // Counted inside the claim's write transaction, so two processes cannot
      // both see a free place and both take it.
      const workflowNames = limits ? this.belowLimit(allWorkflowNames, staleBefore, limits) : allWorkflowNames
      if (workflowNames.length === 0) {
        return null
      }
      const now = Date.now()
      const row = this.oldestClaimable(workflowNames, now, staleBefore)

      // The update re-checks that the run is still claimable.
      const claimable = [
        `status = 'pending'`,
        `(status IN ('sleeping', 'waiting') AND wake_at IS NOT NULL AND wake_at <= ?)`,
        ...(staleBefore === undefined ? [] : [`(status = 'running' AND updated_at <= ?)`]),
      ]
      const reclaimClause = `(${claimable.join(' OR ')})`
      const clauseParams: number[] = staleBefore === undefined ? [now] : [now, staleBefore]

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

  /**
   * These workflows, less any already running as many runs as `limits`
   * allows. Each count stops at the limit, reading at most that many entries
   * of the status index.
   */
  private belowLimit(workflowNames: readonly string[], staleBefore: number | undefined, limits: ReadonlyMap<string, number>): string[] {
    return workflowNames.filter((name) => {
      const limit = limits.get(name)
      if (limit === undefined) return true
      const row = this.get<{ live: number }>(
        `SELECT count(*) AS live FROM (
           SELECT 1 FROM workflow_runs
           WHERE status = 'running' AND workflow = ? AND updated_at > ?
           LIMIT ?
         )`,
        name,
        staleBefore ?? Number.MIN_SAFE_INTEGER,
        limit,
      )
      return (row?.live ?? 0) < limit
    })
  }

  /**
   * The run that has been claimable longest among these workflows.
   *
   * Finds the oldest claimable run of each workflow in each of the ways a
   * run becomes claimable — pending, a sleep or wait past its wake time, a
   * lease that went stale — each by seeking an index ordered by when such
   * runs became due, so a claim reads a handful of index entries however
   * long the queue or the history. The earliest of those is claimed, so a
   * steady stream of new pending runs cannot starve a woken sleeper or a
   * crashed run. A stale run became due when its lease expired,
   * `now - staleBefore` after its last heartbeat.
   *
   * The workflows arrive as one JSON array, so the statement's text is the
   * same however many there are: compiling it does not grow with them.
   */
  private oldestClaimable(workflowNames: readonly string[], now: number, staleBefore?: number): WorkflowRunRow | null {
    const names = JSON.stringify(workflowNames)
    // For each workflow, the rowid of its oldest run of one kind: a seek on
    // the index, ordered by the indexed column itself (an expression would
    // not use the index), which orders the same as `due`.
    const oldest = (due: string, order: string, where: string): string =>
      `SELECT runs.*, runs.rowid AS claim_order, ${due} AS due
       FROM json_each(?) AS w
       JOIN workflow_runs AS runs ON runs.rowid = (
         SELECT rowid FROM workflow_runs
         WHERE workflow = w.value AND ${where}
         ORDER BY ${order}, created_at, rowid LIMIT 1
       )`
    const branches = [
      oldest('runs.updated_at', 'updated_at', `status = 'pending'`),
      oldest('runs.wake_at', 'wake_at', `status = 'sleeping' AND wake_at IS NOT NULL AND wake_at <= ?`),
      oldest('runs.wake_at', 'wake_at', `status = 'waiting' AND wake_at IS NOT NULL AND wake_at <= ?`),
    ]
    const params: SqlParam[] = [names, names, now, names, now]
    if (staleBefore !== undefined) {
      branches.push(oldest('runs.updated_at + ?', 'updated_at', `status = 'running' AND updated_at <= ?`))
      params.push(now - staleBefore, names, staleBefore)
    }

    return this.get<WorkflowRunRow>(
      `SELECT * FROM (${branches.join(' UNION ALL ')})
       ORDER BY due, created_at, claim_order
       LIMIT 1`,
      ...params,
    )
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

  async waitRun(runId: string, leaseId: string, eventName: string | readonly string[], wakeAt: number | null): Promise<boolean> {
    const names: readonly string[] = typeof eventName === 'string' ? [eventName] : eventName
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
        `SELECT 1 AS _ FROM workflow_events WHERE run_id = ? AND event_name IN (${names.map(() => '?').join(', ')}) LIMIT 1`,
        runId,
        ...names,
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
          names.join(', '),
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
    if (status !== undefined && !RUN_STATUSES.includes(status)) {
      return []
    }

    // What every way of reading shares: the workflow and the page cursor.
    const scope: string[] = []
    const scopeArgs: SqlParam[] = []
    if (workflow !== undefined) {
      scope.push('workflow = ?')
      scopeArgs.push(workflow)
    }
    if (before !== undefined) {
      // Keyset cursor over the (created_at DESC, id DESC) order.
      if (beforeId !== undefined) {
        scope.push('(created_at < ? OR (created_at = ? AND id < ?))')
        scopeArgs.push(before, before, beforeId)
      } else {
        scope.push('created_at < ?')
        scopeArgs.push(before)
      }
    }
    const page = (conditions: readonly string[], args: readonly SqlParam[]): WorkflowRun[] => {
      const all = [...conditions, ...scope]
      return this.all<WorkflowRunRow>(
        `SELECT * FROM workflow_runs ${all.length > 0 ? `WHERE ${all.join(' AND ')}` : ''}
         ORDER BY created_at DESC, id DESC
         LIMIT ?`,
        ...args, ...scopeArgs, limit,
      ).map(mapWorkflowRunRow)
    }

    if (status === undefined) {
      return page([], [])
    }
    if (status === 'failed' || status === 'cancelled') {
      // Spelled out, not bound, so the planner can use the partial index that
      // holds only this status, newest first, however long ago they stopped.
      return page([`status = '${status}'`], [])
    }

    // Any other status: when it has few runs, read them through the status
    // index and sort them. When it has many, look among the newest runs
    // first, walking the newest-first index: a full page there is the
    // newest page. Short of one, the matches are sparse among recent runs —
    // an old backlog — and sorting them through the status index is cheaper
    // than walking further. Either way the read is bounded by the window
    // or by the status, never the whole table.
    const counted = this.get<{ n: number }>(
      `SELECT COUNT(*) AS n FROM (
         SELECT 1 FROM workflow_runs WHERE status = ?${workflow === undefined ? '' : ' AND workflow = ?'} LIMIT ?
       )`,
      ...(workflow === undefined ? [status] : [status, workflow]),
      SORT_AT_MOST + 1,
    )?.n ?? 0
    if (counted > SORT_AT_MOST) {
      const edge = this.get<{ created_at: number }>(
        `SELECT created_at FROM workflow_runs ${scope.length > 0 ? `WHERE ${scope.join(' AND ')}` : ''}
         ORDER BY created_at DESC, id DESC
         LIMIT 1 OFFSET ?`,
        ...scopeArgs, WALK_AT_MOST - 1,
      )
      const recent = page(
        edge ? ['+status = ?', 'created_at >= ?'] : ['+status = ?'],
        edge ? [status, edge.created_at] : [status],
      )
      if (recent.length >= limit || !edge) {
        return recent
      }
    }
    return page(['status = ?'], [status])
  }

  async requeueRun(runId: string, options?: RequeueOptions): Promise<boolean> {
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
      // Failed steps a function caught are kept, to replay as failures.
      const keep = options?.keepFailed ?? []
      this.run(
        `DELETE FROM workflow_steps WHERE run_id = ? AND (status = 'retrying' OR (status = 'failed'${
          keep.length > 0 ? ` AND name NOT IN (${keep.map(() => '?').join(', ')})` : ''
        }))`,
        runId,
        ...keep,
      )

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

      this.writeStep(result)
      return true
    })
  }

  async consumeEvent(step: Omit<StepResult, 'output'>, leaseId: string, eventName = step.name): Promise<ConsumeEventResult> {
    return this.transaction((): ConsumeEventResult => {
      const held = this.get<{ _: number }>(
        `SELECT 1 AS _ FROM workflow_runs WHERE id = ? AND status = 'running' AND lease_id = ?`,
        step.runId,
        leaseId,
      )
      if (!held) {
        return { status: 'lease-lost' }
      }
      const event = this.get<{ id: string; payload: string | null }>(
        `SELECT id, payload FROM workflow_events WHERE run_id = ? AND event_name = ? ORDER BY created_at ASC, rowid ASC LIMIT 1`,
        step.runId,
        eventName,
      )
      if (!event) {
        return { status: 'none' }
      }
      this.run(`DELETE FROM workflow_events WHERE id = ?`, event.id)
      const payload = event.payload === null ? null : deserializePersistedValue(event.payload)
      this.writeStep({ ...step, output: payload })
      return { status: 'consumed', payload }
    })
  }

  async getCachedStep(key: string): Promise<CachedStep | null> {
    const row = this.get<{ key: string; workflow: string; step_name: string; output: string | null; created_at: number; expires_at: number }>(
      `SELECT * FROM workflow_step_cache WHERE key = ?`,
      key,
    )
    return row
      ? {
          key: row.key,
          workflow: row.workflow,
          stepName: row.step_name,
          output: row.output === null ? null : deserializePersistedValue(row.output),
          createdAt: row.created_at,
          expiresAt: row.expires_at,
        }
      : null
  }

  async putCachedStep(entry: CachedStep): Promise<void> {
    this.run(
      `INSERT OR REPLACE INTO workflow_step_cache (key, workflow, step_name, output, created_at, expires_at) VALUES (?, ?, ?, ?, ?, ?)`,
      entry.key,
      entry.workflow,
      entry.stepName,
      serializePersistedValue(entry.output, 'Cached step output'),
      entry.createdAt,
      entry.expiresAt,
    )
  }

  async deleteExpiredCachedSteps(now: number, limit: number): Promise<number> {
    return this.run(
      `DELETE FROM workflow_step_cache WHERE key IN (SELECT key FROM workflow_step_cache WHERE expires_at <= ? LIMIT ?)`,
      now,
      limit,
    )
  }

  /** Insert or replace a step's row. Call inside a transaction. */
  private writeStep(result: StepResult): void {
    this.run(
      `INSERT OR REPLACE INTO workflow_steps (id, run_id, name, status, output, error, attempts, created_at, updated_at, started_at, history, kind)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
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
      result.kind ?? null,
    )
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
      // Naming every status lets each be counted by seeking idx_runs_status
      // on (status, workflow), rather than walking the workflow's history.
      : this.all<{ status: string; n: number }>(
        `SELECT status, COUNT(*) AS n FROM workflow_runs
         WHERE status IN (${RUN_STATUSES.map(() => '?').join(', ')}) AND workflow = ?
         GROUP BY status`,
        ...RUN_STATUSES,
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

/**
 * Up to this many runs in a status, listing them sorts them all; beyond it
 * they are dense enough that walking the newest-first index finds a page
 * after reading a small multiple of it.
 */
const SORT_AT_MOST = 10_000

/** How long a statement waits for another connection's write lock. */
const BUSY_TIMEOUT_MS = 5_000
/** How long starting up waits, for a migration another process is applying. */
const MIGRATION_BUSY_TIMEOUT_MS = 60_000

/** How many of the newest runs a listing looks among before sorting instead. */
const WALK_AT_MOST = 5_000

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
    kind: parseStepKind(row.kind),
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
