import { ConfigError } from '../../core/errors'

/** The slice of a connection a migration needs. */
export interface MigrationTarget {
  exec(sql: string): void
  columns(table: string): readonly string[]
}

/**
 * One step in the schema's history. Applied in order, each exactly once, and
 * recorded in SQLite's `user_version` so a database knows which it has had.
 */
interface Migration {
  readonly version: number
  readonly description: string
  up(db: MigrationTarget): void
}

/**
 * Every schema change, oldest first. Append only: a released migration is
 * never edited, because databases in the field have already run it.
 */
export const MIGRATIONS: readonly Migration[] = [
  {
    version: 1,
    description: 'tables, indexes, and the pre-0.8 column fixes',
    // Databases created before versioned migrations report `user_version` 0
    // whatever state they are in, so this baseline is written to converge
    // every earlier shape onto one: each statement is a no-op where it has
    // already happened.
    up(db) {
      db.exec(`
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

      // Databases from before 0.6 lack wake_at.
      if (!db.columns('workflow_runs').includes('wake_at')) {
        db.exec(`ALTER TABLE workflow_runs ADD COLUMN wake_at INTEGER`)
      }

      // 0.6.0 shipped workflow_schedules with a NOT NULL interval_ms and no cron
      // column. Adding cron alone is not enough: a cron schedule leaves
      // interval_ms null, which that constraint rejects. SQLite cannot drop a NOT
      // NULL in place, so the table is rebuilt — cheap, since schedules are few.
      if (!db.columns('workflow_schedules').includes('cron')) {
        db.exec(`
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

      db.exec(`
        CREATE INDEX IF NOT EXISTS idx_runs_status ON workflow_runs(status, workflow);
        CREATE INDEX IF NOT EXISTS idx_runs_wake ON workflow_runs(status, wake_at);
        CREATE INDEX IF NOT EXISTS idx_steps_run_id ON workflow_steps(run_id);
        CREATE INDEX IF NOT EXISTS idx_events_run_name ON workflow_events(run_id, event_name, created_at);
        CREATE INDEX IF NOT EXISTS idx_schedules_due ON workflow_schedules(next_run_at);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_runs_workflow_idempotency
        ON workflow_runs(workflow, idempotency_key)
        WHERE idempotency_key IS NOT NULL;
      `)
    },
  },
  {
    version: 2,
    description: 'run lifecycle: start and finish times, output, error, awaited event; step start times',
    up(db) {
      db.exec(`
        ALTER TABLE workflow_runs ADD COLUMN started_at INTEGER;
        ALTER TABLE workflow_runs ADD COLUMN finished_at INTEGER;
        ALTER TABLE workflow_runs ADD COLUMN output TEXT;
        ALTER TABLE workflow_runs ADD COLUMN error TEXT;
        ALTER TABLE workflow_runs ADD COLUMN waiting_for TEXT;
        ALTER TABLE workflow_steps ADD COLUMN started_at INTEGER;

        -- A finished run's last write was the one that finished it, so
        -- updated_at is when it finished. Without this, runs from before the
        -- column existed could never be pruned. Start times, outputs, and
        -- errors were never recorded and stay NULL.
        UPDATE workflow_runs SET finished_at = updated_at
        WHERE status IN ('completed', 'failed', 'cancelled');

        CREATE INDEX idx_runs_finished ON workflow_runs(finished_at) WHERE finished_at IS NOT NULL;
      `)
    },
  },
  {
    version: 3,
    description: 'step attempt history',
    up(db) {
      // JSON array of attempts. NULL for steps recorded before it existed,
      // which read back as an empty history.
      db.exec(`ALTER TABLE workflow_steps ADD COLUMN history TEXT`)
    },
  },
]

/** The schema version this build of Reflow writes. */
export const SCHEMA_VERSION = MIGRATIONS.length

/**
 * The migrations a database at `current` still needs, in order.
 *
 * Refuses a database newer than this build: its tables may hold columns or
 * meanings older code would silently mishandle.
 */
export function pendingMigrations(current: number): readonly Migration[] {
  if (current > SCHEMA_VERSION) {
    throw new ConfigError(
      `Database schema version ${current} is newer than this version of reflow-ts supports (${SCHEMA_VERSION}). Upgrade reflow-ts to open it.`,
    )
  }

  return MIGRATIONS.slice(current)
}
