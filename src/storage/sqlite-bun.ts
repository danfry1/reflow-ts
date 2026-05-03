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
  readonly changes: number
}

interface BunStatement<T = Record<string, unknown>> {
  run(...params: unknown[]): void
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
  cache_key: string | null
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
    `)

    this.db.exec(`
      CREATE INDEX IF NOT EXISTS idx_runs_status ON workflow_runs(status, workflow);
      CREATE INDEX IF NOT EXISTS idx_steps_run_id ON workflow_steps(run_id);
      CREATE UNIQUE INDEX IF NOT EXISTS idx_runs_workflow_idempotency
      ON workflow_runs(workflow, idempotency_key)
      WHERE idempotency_key IS NOT NULL;
    `)

    try {
      this.db.exec(`ALTER TABLE workflow_steps ADD COLUMN cache_key TEXT`)
    } catch { /* column already exists on existing databases */ }
    this.db.exec(`
      CREATE INDEX IF NOT EXISTS idx_steps_cache_key
      ON workflow_steps(name, cache_key)
      WHERE cache_key IS NOT NULL
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
      const reclaimClause = staleBefore === undefined
        ? `status = 'pending'`
        : `(status = 'pending' OR (status = 'running' AND updated_at <= ?))`
      const queryArgs = staleBefore === undefined
        ? [...workflowNames]
        : [...workflowNames, staleBefore]

      const row = this.db
        .prepare<WorkflowRunRow>(
          `SELECT * FROM workflow_runs
           WHERE workflow IN (${placeholders}) AND ${reclaimClause}
           ORDER BY CASE status WHEN 'pending' THEN 0 ELSE 1 END ASC, created_at ASC, rowid ASC
           LIMIT 1`,
        )
        .get(...queryArgs)

      if (!row) {
        return null
      }

      const now = Date.now()
      const leaseId = randomUUID()
      const updateWhere = staleBefore === undefined
        ? `id = ? AND status = 'pending'`
        : `id = ? AND (status = 'pending' OR (status = 'running' AND updated_at <= ?))`
      const updateArgs = staleBefore === undefined
        ? [leaseId, now, row.id]
        : [leaseId, now, row.id, staleBefore]

      this.db
        .prepare(
          `UPDATE workflow_runs
           SET status = 'running', lease_id = ?, updated_at = ?
           WHERE ${updateWhere}`,
        )
        .run(...updateArgs)

      if (this.db.changes === 0) {
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
    this.db
      .prepare(
        `UPDATE workflow_runs
         SET updated_at = ?
         WHERE id = ? AND status = 'running' AND lease_id = ?`,
      )
      .run(Date.now(), runId, leaseId)

    return this.db.changes > 0
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

  async saveStepResult(result: StepResult, leaseId?: string, cacheKey?: string): Promise<boolean> {
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
          `INSERT OR REPLACE INTO workflow_steps (id, run_id, name, status, output, error, attempts, cache_key, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        )
        .run(
          result.id,
          result.runId,
          result.name,
          result.status,
          serializePersistedValue(result.output, 'Step output'),
          result.error,
          result.attempts,
          cacheKey ?? null,
          result.createdAt,
          result.updatedAt,
        )

      return true
    })

    return save()
  }

  async getCachedStepResult(stepName: string, cacheKey: string, ttlMs?: number): Promise<StepResult | null> {
    const cutoff = ttlMs !== undefined ? Date.now() - ttlMs : null
    const row = this.db
      .prepare<WorkflowStepRow>(
        `SELECT * FROM workflow_steps
         WHERE name = ?
           AND cache_key = ?
           AND status = 'completed'
           AND attempts > 0
           ${cutoff !== null ? 'AND updated_at >= ?' : ''}
         ORDER BY updated_at DESC
         LIMIT 1`,
      )
      .get(stepName, cacheKey, ...(cutoff !== null ? [cutoff] : []))

    if (!row) return null
    return {
      id: row.id,
      runId: row.run_id,
      name: row.name,
      status: row.status as StepResult['status'],
      output: row.output === null ? null : deserializePersistedValue(row.output),
      error: row.error,
      attempts: row.attempts,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
    }
  }

  async updateRunStatus(runId: string, status: RunStatus): Promise<boolean> {
    this.db
      .prepare(
        `UPDATE workflow_runs
         SET status = ?, lease_id = ?, updated_at = ?
         WHERE id = ?`,
      )
      .run(status, null, Date.now(), runId)

    return this.db.changes > 0
  }

  async updateClaimedRunStatus(runId: string, leaseId: string, status: RunStatus): Promise<boolean> {
    this.db
      .prepare(
        `UPDATE workflow_runs
         SET status = ?, lease_id = ?, updated_at = ?
         WHERE id = ? AND status = 'running' AND lease_id = ?`,
      )
      .run(status, status === 'running' ? leaseId : null, Date.now(), runId, leaseId)

    return this.db.changes > 0
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
