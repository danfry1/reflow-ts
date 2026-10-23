/** A value SQLite can bind to a `?` placeholder. */
export type SqlParam = string | number | bigint | null

/**
 * The handful of synchronous primitives the shared SQLite storage needs from a
 * binding.
 *
 * `better-sqlite3`, `bun:sqlite`, and `node:sqlite` agree on SQL but not on
 * their JavaScript surface: a missing row is `undefined` in one and `null` in
 * another, and the affected-row count lives in a different place in each. A
 * driver absorbs those differences so the SQL, the schema, and the transaction
 * discipline exist once, in `SqliteStorageBase`, rather than once per binding.
 *
 * Transactions are deliberately absent. Every binding can run `BEGIN
 * IMMEDIATE` through {@link SqliteDriver.exec}, and owning that statement in
 * one place is what guarantees every adapter takes the write lock up front.
 */
export interface SqliteDriver {
  /** Run one or more statements that bind no parameters and return no rows. */
  exec(sql: string): void
  /** The first row the statement returns, or `undefined` when there is none. */
  get<T>(sql: string, params: readonly SqlParam[]): T | undefined
  /** Every row the statement returns. */
  all<T>(sql: string, params: readonly SqlParam[]): T[]
  /** Run a write and return how many rows it changed. */
  run(sql: string, params: readonly SqlParam[]): number
  /** Close the underlying connection. */
  close(): void
}

/**
 * Cache prepared statements by SQL text.
 *
 * Every query the storage issues is a fixed string, apart from the `IN (...)`
 * list in a claim, whose length is the number of registered workflows — so the
 * cache stays small and bounded while sparing each call a fresh `prepare`.
 */
export function statementCache<TStatement>(
  prepare: (sql: string) => TStatement,
): (sql: string) => TStatement {
  const statements = new Map<string, TStatement>()

  return (sql) => {
    const cached = statements.get(sql)
    if (cached !== undefined) {
      return cached
    }

    const statement = prepare(sql)
    statements.set(sql, statement)
    return statement
  }
}
