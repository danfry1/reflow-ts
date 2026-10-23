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

import { SqliteStorageBase } from './sqlite/core'
import { statementCache, type SqlParam, type SqliteDriver } from './sqlite/driver'

// Minimal type declarations for bun:sqlite to enable compilation on any runtime.
// At runtime this module only works on Bun.
interface BunDatabase {
  exec(sql: string): void
  prepare(sql: string): BunStatement
  close(): void
}
interface BunRunResult {
  changes: number
  lastInsertRowid: number | bigint
}
interface BunStatement {
  // bun:sqlite reports affected-row counts on the run() result, not on the
  // Database instance (db.changes is undefined), so callers must read it here.
  run(...params: SqlParam[]): BunRunResult
  // A missing row is null here, where the other bindings return undefined.
  get(...params: SqlParam[]): unknown
  all(...params: SqlParam[]): unknown[]
}

function openBunSqlite(path: string): SqliteDriver {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const { Database } = require('bun:sqlite') as { Database: new (path: string) => BunDatabase }
  const db = new Database(path)
  const prepare = statementCache((sql) => db.prepare(sql))

  return {
    exec: (sql) => {
      db.exec(sql)
    },
    get: <T>(sql: string, params: readonly SqlParam[]) => (prepare(sql).get(...params) ?? undefined) as T | undefined,
    all: <T>(sql: string, params: readonly SqlParam[]) => prepare(sql).all(...params) as T[],
    run: (sql, params) => prepare(sql).run(...params).changes,
    close: () => {
      db.close()
    },
  }
}

/**
 * SQLite-backed storage adapter for Bun. Uses WAL mode for concurrent reads and
 * transactional claiming for safe multi-process access.
 */
export class SQLiteStorage extends SqliteStorageBase {
  constructor(path: string) {
    super(openBunSqlite(path))
  }
}
