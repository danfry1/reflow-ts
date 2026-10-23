/**
 * SQLite storage adapter for Node.js, backed by `better-sqlite3`.
 *
 * Supports every Node version Reflow does. On Node >= 22.5,
 * `reflow-ts/sqlite-node-builtin` avoids the native dependency.
 *
 * @example
 * ```ts
 * import { SQLiteStorage } from 'reflow-ts/sqlite-node'
 *
 * const storage = new SQLiteStorage('./reflow.db')
 * await storage.initialize()
 * ```
 */

import Database from 'better-sqlite3'
import { SqliteStorageBase } from './sqlite/core'
import { statementCache, type SqliteDriver } from './sqlite/driver'

function openBetterSqlite(path: string): SqliteDriver {
  const db = new Database(path)
  const prepare = statementCache((sql) => db.prepare(sql))

  return {
    exec: (sql) => {
      db.exec(sql)
    },
    get: <T>(sql: string, params: readonly unknown[]) => prepare(sql).get(...params) as T | undefined,
    all: <T>(sql: string, params: readonly unknown[]) => prepare(sql).all(...params) as T[],
    run: (sql, params) => prepare(sql).run(...params).changes,
    close: () => {
      db.close()
    },
  }
}

/**
 * SQLite-backed storage adapter. Uses WAL mode for concurrent reads and
 * transactional claiming for safe multi-process access.
 */
export class SQLiteStorage extends SqliteStorageBase {
  constructor(path: string) {
    super(openBetterSqlite(path))
  }
}
