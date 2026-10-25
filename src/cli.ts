#!/usr/bin/env node
/**
 * `reflow-ts` command-line tool.
 *
 *   reflow-ts dashboard <database> [--port 4400] [--host 127.0.0.1] [--migrate]
 *
 * Serves the dashboard for a SQLite database file, read-only: it can show
 * runs but not cancel or resume them, since no engine is attached.
 */

import { existsSync } from 'node:fs'
import { createServer } from 'node:http'
import { resolve } from 'node:path'
import { isIPv4, isIPv6 } from 'node:net'
import { parseArgs } from 'node:util'
import type { SqliteStorageBase, SQLiteStorageOptions } from './storage/sqlite/core'
import { createDashboard, toNodeHandler } from './dashboard/index'

const USAGE = `Usage: reflow-ts dashboard <database> [options]

Serve a read-only dashboard for a Reflow SQLite database.

Options:
  --port <n>     Port to listen on (default 4400)
  --host <addr>  Address to bind (default 127.0.0.1). Anyone who can reach
                 this address can read every run's input and output.
  --migrate      Upgrade the database's schema first, if it is older than
                 this version of reflow-ts
  -h, --help     Show this help
`

class UsageError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options)
    this.name = 'UsageError'
  }
}

async function main(argv: readonly string[]): Promise<void> {
  const { values, positionals } = parseArgs({
    args: [...argv],
    allowPositionals: true,
    options: {
      port: { type: 'string', default: '4400' },
      host: { type: 'string', default: '127.0.0.1' },
      migrate: { type: 'boolean', default: false },
      help: { type: 'boolean', short: 'h', default: false },
    },
  })

  const [command, file] = positionals
  if (values.help || command === undefined) {
    process.stdout.write(USAGE)
    return
  }
  if (command !== 'dashboard') {
    throw new UsageError(`Unknown command "${command}"`)
  }
  if (file === undefined) {
    throw new UsageError('Give the path to a Reflow SQLite database')
  }

  const path = resolve(file)
  // SQLite would silently create an empty database at a mistyped path.
  if (!existsSync(path)) {
    throw new UsageError(`No database at ${path}`)
  }

  const port = Number(values.port)
  if (!Number.isInteger(port) || port < 0 || port > 65_535) {
    throw new UsageError(`--port must be a port number, not "${values.port}"`)
  }
  if (isWildcard(values.host)) {
    // The dashboard answers only to the name it is reached by, and a wildcard
    // bind cannot say which that is: every real request would be turned away.
    throw new UsageError(
      `--host ${values.host} listens on every address, so the dashboard cannot tell which name it is reached by. ` +
        'Pass the one address to listen on, such as --host 192.168.1.20, or reach it over SSH.',
    )
  }

  // Read-only unless asked to migrate: an inspector must leave the file as it
  // found it, journal mode included.
  let storage = await openStorage(path, { readOnly: true })
  const { current, latest } = storage.schemaVersion()
  if (current > latest) {
    throw new UsageError(`${path} was written by a newer reflow-ts (schema ${current}); upgrade reflow-ts to inspect it`)
  }
  if (current < latest) {
    if (!values.migrate) {
      throw new UsageError(
        `${path} uses an older schema (${current}, current is ${latest}). Start your app on this version of reflow-ts ` +
          'to upgrade it, or pass --migrate to upgrade it now.',
      )
    }
    storage.close()
    const writable = await openStorage(path, { readOnly: false })
    await writable.initialize()
    writable.close()
    storage = await openStorage(path, { readOnly: true })
  }

  try {
    await storage.countRuns()
  } catch (error) {
    throw new UsageError(`${path} does not look like a Reflow database`, { cause: error })
  }

  const server = createServer((req, res) => serveRequest(req, res))
  let serveRequest: ReturnType<typeof toNodeHandler> = (_req, res) => {
    res.writeHead(503).end()
  }

  await new Promise<void>((resolveListen, rejectListen) => {
    server.once('error', rejectListen)
    server.listen(port, values.host, () => resolveListen())
  })

  const address = server.address()
  const boundPort = typeof address === 'object' && address !== null ? address.port : port
  const dashboard = createDashboard({
    storage,
    // No credentials to check, so the dashboard answers only to the address it
    // is bound on: a site re-pointing its own name at that address through DNS
    // rebinding sends its own Host, and is turned away.
    authorize: () => true,
    allowedHosts: hostNames(values.host).map((name) => `${name}:${boundPort}`),
  })
  serveRequest = toNodeHandler(dashboard)

  process.stdout.write(`Reflow dashboard for ${path}\nhttp://${displayHost(values.host)}:${boundPort}/  (read-only, Ctrl+C to stop)\n`)
  if (!isLoopback(values.host)) {
    process.stderr.write(`Warning: listening on ${values.host}, without authentication.\n`)
  }

  const shutdown = () => {
    server.close()
    storage.close()
  }
  process.once('SIGINT', shutdown)
  process.once('SIGTERM', shutdown)
}

/** The names a browser may use for `host` in a `Host` header. */
function hostNames(host: string): string[] {
  if (isLoopback(host)) {
    return [...new Set(['localhost', '127.0.0.1', '[::1]', displayHost(host)])]
  }
  return [displayHost(host)]
}

/** IPv6 literals are bracketed in URLs and `Host` headers. */
function displayHost(host: string): string {
  return isIPv6(host) ? `[${host}]` : host
}

/**
 * Open the database with whichever SQLite binding this runtime has: Bun's
 * built-in, Node's built-in (22.5+), or better-sqlite3 if it is installed.
 */
async function openStorage(path: string, options: SQLiteStorageOptions): Promise<SqliteStorageBase> {
  if ('Bun' in globalThis) {
    const { SQLiteStorage } = await import('./storage/sqlite-bun')
    return new SQLiteStorage(path, options)
  }

  const hasNodeSqlite = await import('node:sqlite').then(() => true, () => false)
  if (hasNodeSqlite) {
    const { SQLiteStorage } = await import('./storage/sqlite-node-builtin')
    return new SQLiteStorage(path, options)
  }

  try {
    const { SQLiteStorage } = await import('./storage/sqlite-node')
    return new SQLiteStorage(path, options)
  } catch (error) {
    throw new UsageError('No SQLite binding available: use Node 22.5 or newer, Bun, or install better-sqlite3', {
      cause: error,
    })
  }
}

/** An address that means every interface rather than one. */
function isWildcard(host: string): boolean {
  return host === '0.0.0.0' || host === '::' || host === '[::]'
}

function isLoopback(host: string): boolean {
  return host === 'localhost' || host === '::1' || (isIPv4(host) && host.startsWith('127.'))
}

main(process.argv.slice(2)).catch((error: unknown) => {
  if (error instanceof UsageError) {
    process.stderr.write(`reflow-ts: ${error.message}\n`)
    process.exitCode = 2
    return
  }
  process.stderr.write(`reflow-ts: ${error instanceof Error ? error.message : String(error)}\n`)
  process.exitCode = 1
})
