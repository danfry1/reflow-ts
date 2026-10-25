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

import { createRequire } from 'node:module'
import { SqliteStorageBase, type SQLiteStorageOptions } from './sqlite/core'
import { statementCache, type SqlParam, type SqliteDriver } from './sqlite/driver'

// `node:sqlite` is loaded lazily through createRequire so importing this module
// is safe on any runtime — only constructing SQLiteStorage requires the module
// to be present (Node >= 22.5).
const nodeRequire = createRequire(import.meta.url)

export type { SQLiteStorageOptions }

function openNodeSqlite(path: string, options: SQLiteStorageOptions): SqliteDriver {
  const sqlite = nodeRequire('node:sqlite') as typeof import('node:sqlite')
  // `readOnly` arrived in Node 22.12 and is ignored before it; the base class's
  // query_only pragma refuses writes either way.
  const db = new sqlite.DatabaseSync(path, options.readOnly ? { readOnly: true } : {})
  const prepare = statementCache((sql) => db.prepare(sql))

  return {
    exec: (sql) => {
      db.exec(sql)
    },
    get: <T>(sql: string, params: readonly SqlParam[]) => prepare(sql).get(...params) as T | undefined,
    all: <T>(sql: string, params: readonly SqlParam[]) => prepare(sql).all(...params) as T[],
    // `changes` is `number | bigint`; a row count never needs the bigint range.
    run: (sql, params) => Number(prepare(sql).run(...params).changes),
    close: () => {
      db.close()
    },
  }
}

/**
 * SQLite-backed storage adapter on `node:sqlite`. Uses WAL mode for concurrent
 * reads and transactional claiming for safe multi-process access.
 */
export class SQLiteStorage extends SqliteStorageBase {
  constructor(path: string, options: SQLiteStorageOptions = {}) {
    super(openNodeSqlite(path, options), options)
  }
}
