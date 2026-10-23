import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import Database from 'better-sqlite3'
import { existsSync, unlinkSync } from 'node:fs'
import { SqliteStorageBase } from '../sqlite/core'
import type { SqliteDriver } from '../sqlite/driver'
import { SerializationError } from '../../core/errors'
import type { WorkflowRun } from '../../core/types'

const DB_PATH = '/tmp/reflow-sqlite-core.db'

function removeDatabase(): void {
  for (const suffix of ['', '-wal', '-shm']) {
    if (existsSync(DB_PATH + suffix)) unlinkSync(DB_PATH + suffix)
  }
}

function makeRun(overrides: Partial<WorkflowRun> = {}): WorkflowRun {
  const now = Date.now()
  return {
    id: 'run_1',
    workflow: 'test',
    input: {},
    idempotencyKey: null,
    status: 'pending',
    createdAt: now,
    updatedAt: now,
    ...overrides,
  }
}

/**
 * A better-sqlite3 driver that calls `onRead` after every row lookup — the
 * point inside a transaction between its first read and its first write.
 */
function driverWithReadHook(path: string, onRead: (sql: string) => void): SqliteDriver {
  const db = new Database(path)
  return {
    exec: (sql) => {
      db.exec(sql)
    },
    get: <T>(sql: string, params: readonly unknown[]) => {
      const row = db.prepare(sql).get(...params) as T | undefined
      onRead(sql)
      return row
    },
    all: <T>(sql: string, params: readonly unknown[]) => db.prepare(sql).all(...params) as T[],
    run: (sql, params) => db.prepare(sql).run(...params).changes,
    close: () => {
      db.close()
    },
  }
}

describe('SqliteStorageBase transactions', () => {
  let other: Database.Database

  beforeEach(() => {
    removeDatabase()
    other = new Database(DB_PATH)
    // Fail at once rather than waiting out a lock that, in a single thread,
    // can never be released while this connection waits on it.
    other.pragma('busy_timeout = 0')
  })

  afterEach(() => {
    other.close()
    removeDatabase()
  })

  it('holds the write lock from the first read of a claim', async () => {
    let competingWrite: unknown = 'not attempted'
    const storage = new SqliteStorageBase(driverWithReadHook(DB_PATH, (sql) => {
      if (!sql.includes('ORDER BY') || competingWrite !== 'not attempted') return
      try {
        other.prepare(`UPDATE workflow_runs SET updated_at = updated_at WHERE id = 'run_2'`).run()
        competingWrite = 'committed'
      } catch (error) {
        competingWrite = error
      }
    }))
    await storage.initialize()
    await storage.createRun(makeRun({ id: 'run_1' }))
    await storage.createRun(makeRun({ id: 'run_2' }))

    const claimed = await storage.claimNextRun(['test'])
    storage.close()

    // Under a deferred BEGIN the competing write commits here, and the claim's
    // own UPDATE then fails with SQLITE_BUSY_SNAPSHOT.
    expect(competingWrite).toBeInstanceOf(Database.SqliteError)
    expect(competingWrite).toHaveProperty('code', 'SQLITE_BUSY')
    expect(claimed?.id).toBe('run_1')
  })

  it('rethrows the original error and leaves no transaction open when the body throws', async () => {
    const storage = new SqliteStorageBase(driverWithReadHook(DB_PATH, () => {}))
    await storage.initialize()

    await expect(storage.createRun(makeRun({ input: { fn: () => 1 } as never }))).rejects.toBeInstanceOf(
      SerializationError,
    )

    // The rollback released the lock, so another connection can write.
    other.prepare(`INSERT INTO workflow_events (id, run_id, event_name, payload, created_at) VALUES ('e', 'r', 'n', NULL, 0)`).run()
    expect(other.prepare(`SELECT COUNT(*) AS n FROM workflow_events`).get()).toStrictEqual({ n: 1 })
    storage.close()
  })
})

describe('SqliteStorageBase connection setup and recovery', () => {
  beforeEach(removeDatabase)
  afterEach(removeDatabase)

  it('sets busy_timeout before anything that might wait for a lock', () => {
    const statements: string[] = []
    const recording = driverWithReadHook(DB_PATH, () => {})
    const exec = recording.exec
    recording.exec = (sql) => {
      statements.push(sql)
      exec(sql)
    }

    new SqliteStorageBase(recording).close()

    expect(statements[0]).toBe('PRAGMA busy_timeout = 5000')
  })

  it('stays usable after a COMMIT fails', async () => {
    const driver = driverWithReadHook(DB_PATH, () => {})
    const exec = driver.exec
    let failNextCommit = false
    driver.exec = (sql) => {
      if (sql === 'COMMIT' && failNextCommit) {
        failNextCommit = false
        throw new Error('database is locked')
      }
      exec(sql)
    }
    const storage = new SqliteStorageBase(driver)
    await storage.initialize()

    failNextCommit = true
    await expect(storage.createRun(makeRun({ id: 'lost' }))).rejects.toThrow('database is locked')

    // Without a rollback the connection would still be inside the failed
    // transaction, and this would fail with "cannot start a transaction
    // within a transaction".
    await storage.createRun(makeRun({ id: 'kept' }))
    expect((await storage.getRun('kept'))?.status).toBe('pending')
    expect(await storage.getRun('lost')).toBeNull()
    storage.close()
  })
})
