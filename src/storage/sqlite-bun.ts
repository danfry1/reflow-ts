/**
 * SQLite storage adapter for Bun runtime using the built-in `bun:sqlite` module.
 * No native dependencies required.
 *
 * @example
 * ```ts
 * import { SQLiteStorage } from 'reflow-ts/sqlite-bun'
 *
 * const storage = new SQLiteStorage('./reflow.db')
 * await storage.initialize()
 * ```
 */

import { randomUUID } from 'node:crypto'
import type {
  ClaimedRun,
  CreateRunResult,
  PersistedValue,
  RunStatus,
  StepResult,
  StorageAdapter,
  WorkflowRun,
} from '../core/types'
import { deserializePersistedValue, serializePersistedValue } from './codec'

// Minimal type declarations for bun:sqlite to enable compilation on any runtime.
// At runtime this module only works on Bun.
interface BunDatabase {
  exec(sql: string): void
  prepare<T = Record<string, unknown>>(sql: string): BunStatement<T>
  transaction<T>(fn: () => T): () => T
  close(): void
}

interface BunRunResult {
  changes: number
  lastInsertRowid: number | bigint
}

interface BunStatement<T = Record<string, unknown>> {
  // bun:sqlite reports affected-row counts on the run() result, not on the
  // Database instance (db.changes is undefined), so callers must read it here.
  run(...params: unknown[]): BunRunResult
  get(...params: unknown[]): T | null
  all(...params: unknown[]): T[]
}

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

export class SQLiteStorage implements StorageAdapter {
  private db: BunDatabase

  constructor(path: string) {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const { Database } = require('bun:sqlite') as { Database: new (path: string) => BunDatabase }
    this.db = new Database(path)
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
    `)

    // Migrate databases created before the wake_at column existed.
    const columns = this.db
      .prepare<{ name: string }>(`PRAGMA table_info(workflow_runs)`)
      .all()
    if (!columns.some((column) => column.name === 'wake_at')) {
      this.db.exec(`ALTER TABLE workflow_runs ADD COLUMN wake_at INTEGER`)
    }

    this.db.exec(`
      CREATE INDEX IF NOT EXISTS idx_runs_status ON workflow_runs(status, workflow);
      CREATE INDEX IF NOT EXISTS idx_runs_wake ON workflow_runs(status, wake_at);
      CREATE INDEX IF NOT EXISTS idx_steps_run_id ON workflow_steps(run_id);
      CREATE INDEX IF NOT EXISTS idx_events_run_name ON workflow_events(run_id, event_name, created_at);
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

      return this.db
        .prepare<WorkflowRunRow>(
          `SELECT * FROM workflow_runs
           WHERE workflow = ? AND idempotency_key = ?
           LIMIT 1`,
        )
        .get(run.workflow, run.idempotencyKey)
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
        .prepare<WorkflowRunRow>(
          `SELECT * FROM workflow_runs
           WHERE workflow IN (${placeholders}) AND ${reclaimClause}
           ORDER BY CASE status WHEN 'pending' THEN 0 ELSE 1 END ASC, created_at ASC, rowid ASC
           LIMIT 1`,
        )
        .get(...workflowNames, ...clauseParams)

      if (!row) {
        return null
      }

      const leaseId = randomUUID()
      const updated = this.db
        .prepare(
          `UPDATE workflow_runs
           SET status = 'running', lease_id = ?, wake_at = NULL, updated_at = ?
           WHERE id = ? AND ${reclaimClause}`,
        )
        .run(leaseId, now, row.id, ...clauseParams)

      if (updated.changes === 0) {
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
        .prepare<{ _: number }>(`SELECT 1 AS _ FROM workflow_runs WHERE id = ? AND status = 'running' AND lease_id = ?`)
        .get(runId, leaseId)
      if (!held) {
        return false
      }

      // If a matching event is already buffered, stay reclaimable instead of
      // waiting — closes the deliver-during-suspend race.
      const buffered = this.db
        .prepare<{ _: number }>(`SELECT 1 AS _ FROM workflow_events WHERE run_id = ? AND event_name = ? LIMIT 1`)
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
      const exists = this.db.prepare<{ _: number }>(`SELECT 1 AS _ FROM workflow_runs WHERE id = ?`).get(runId)
      if (!exists) {
        return false
      }

      this.db
        .prepare(`INSERT INTO workflow_events (id, run_id, event_name, payload, created_at) VALUES (?, ?, ?, ?, ?)`)
        .run(randomUUID(), runId, eventName, serializePersistedValue(payload, 'Event payload'), Date.now())

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
        .prepare<{ id: string; payload: string | null }>(
          `SELECT id, payload FROM workflow_events WHERE run_id = ? AND event_name = ? ORDER BY created_at ASC, rowid ASC LIMIT 1`,
        )
        .get(runId, eventName)
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
      .prepare<WorkflowRunRow>(`SELECT * FROM workflow_runs WHERE id = ?`)
      .get(runId)

    return row ? mapWorkflowRunRow(row) : null
  }

  async getStepResults(runId: string): Promise<StepResult[]> {
    const rows = this.db
      .prepare<WorkflowStepRow>(`SELECT * FROM workflow_steps WHERE run_id = ? ORDER BY created_at ASC`)
      .all(runId)

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
          .prepare<{ _: number }>(
            `SELECT 1 AS _ FROM workflow_runs
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
