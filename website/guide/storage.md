# Storage

Storage is pluggable through the [`StorageAdapter`](/api/storage) interface. Reflow ships adapters for SQLite and PostgreSQL, and the same workflows run on either.

- **SQLite** — a file next to your app. Nothing to run, and several processes on one machine can share it. Start here.
- **PostgreSQL** — when workers run on more than one machine, or you want the database your platform already backs up and monitors.

## SQLiteStorage (Bun)

For the Bun runtime. Uses the built-in `bun:sqlite` module with **zero native dependencies**.

```typescript
import { SQLiteStorage } from 'reflow-ts/sqlite-bun'

const storage = new SQLiteStorage('./workflows.db')
```

## SQLiteStorage (Node.js)

For Node.js. Uses [`better-sqlite3`](https://github.com/WiseLibs/better-sqlite3) (a native addon), persists to disk, and runs in WAL mode. Install `better-sqlite3` alongside Reflow.

```typescript
import { SQLiteStorage } from 'reflow-ts/sqlite-node'

const storage = new SQLiteStorage('./workflows.db')
```

## SQLiteStorage (Node.js built-in)

For modern Node.js with **zero native dependencies** — uses the built-in [`node:sqlite`](https://nodejs.org/api/sqlite.html) module instead of `better-sqlite3`, the Node equivalent of the Bun adapter.

```typescript
import { SQLiteStorage } from 'reflow-ts/sqlite-node-builtin'

const storage = new SQLiteStorage('./workflows.db')
```

Works without a flag from **Node.js 22.13 and 23.4**; Node 22.5–22.12 and 23.0–23.3 need `--experimental-sqlite`. Node still marks the module experimental, so it prints an `ExperimentalWarning` at startup. For Node below 22.5, use the `better-sqlite3` adapter above.

::: tip Which Node adapter?
Use `sqlite-node-builtin` if you're on Node ≥ 22.13 and want to drop the `better-sqlite3` native dependency. Use `sqlite-node` for broad compatibility (Node ≥ 18.18) or to avoid the experimental module.
:::

### Read-only connections

Every SQLite adapter can open an existing database for reading only — to inspect a database another process owns, or a backup — without changing a byte of it:

```typescript
const storage = new SQLiteStorage('./workflows.db', { readOnly: true })
```

Writes fail, `initialize()` included, and the file's journal mode is left as it was. This is how [`reflow-ts dashboard`](/guide/dashboard) opens a database.

## PostgresStorage

For running workflows from many processes on many machines against one database. Install [`pg`](https://node-postgres.com) alongside Reflow:

```bash
npm install reflow-ts pg
```

```typescript
import { PostgresStorage } from 'reflow-ts/postgres'

const storage = new PostgresStorage({ connectionString: process.env.DATABASE_URL })
```

Or hand it the pool your app already has; Reflow borrows connections from it and leaves it open on `close()`:

```typescript
const storage = new PostgresStorage({ pool })
```

`initialize()` — which `engine.start()` calls — creates a `reflow` schema and its tables, so they never meet your app's; pass `schema: 'jobs'` to name it yourself. It is safe for every process to start at once: they take turns under an advisory lock, and each migration runs exactly once. Needs PostgreSQL 14 or later.

Every process runs the same engine against the same database, and they share the work: claiming takes each due run with `FOR UPDATE SKIP LOCKED`, so no two workers take one run and none waits on another. Crash recovery, leases, schedules, events, and [per-workflow concurrency limits](/guide/concurrency#limiting-one-workflow) all hold across machines. On a table of a million runs, a claim takes a few milliseconds.

Leases compare times written by different machines, so keep their clocks in sync — NTP, which every cloud provides, is plenty. A machine whose clock runs ahead of another's by more than the lease less the heartbeat interval (20 seconds with the defaults) would see that machine's live runs as stale and take them over, running their steps twice; a few hundred milliseconds of drift changes nothing. Give every process the same `runLeaseDurationMs`.

The [dashboard](/guide/dashboard) works the same against Postgres when mounted in your app. The `npx reflow-ts dashboard` command reads SQLite files only.

### Moving from SQLite

Runs do not move between databases: point your app at Postgres and new runs go there. To carry on the runs already in SQLite, run one engine on the old file beside the new one until they finish:

```typescript
const old = createEngine({ storage: new SQLiteStorage('./app.db'), workflows })
const engine = createEngine({ storage: new PostgresStorage({ connectionString }), workflows })
await old.start()
await engine.start()
// enqueue only on `engine`; stop `old` once its runs are done
```

`await old.countRuns()` shows what is left. Re-register your [schedules](/guide/scheduling) on the new engine; they are not copied either.

## MemoryStorage

An in-memory adapter, used internally by the [test helper](/guide/testing). For custom use, import it from `reflow-ts/test`. State is lost when the process exits — it offers no durability, so it's for tests and ephemeral work only.

```typescript
import { MemoryStorage } from 'reflow-ts/test'
```

## Persistable values

Everything Reflow stores — workflow input and step output — must be plain data: objects, arrays, strings, numbers, booleans, `null`, `undefined`, and `Date`. Returning a non-serializable value (a function, a class instance, `NaN`) throws a [`SerializationError`](/guide/error-handling).

## Custom adapters

Implement the [`StorageAdapter`](/api/storage) interface to back Reflow with any database — Postgres, MySQL, Redis, a hosted KV. The contract is small, but two methods carry the durability guarantees:

- `claimNextRun(workflowNames, staleBefore?)` must **atomically** claim the next pending or stale run, returning a unique `leaseId`. This is what prevents two workers from running the same run. It must also claim the run that has been claimable **longest**, so a steady stream of new work cannot starve a woken sleeper or a crashed run awaiting reclaim.
- `saveStepResult`, `completeRun`, and `failRun` take a `leaseId` and must **no-op when the lease no longer matches**, so a worker that lost its lease can't clobber a run another worker has taken over.

- `claimDueSchedule(workflowNames, now)` must claim a due [schedule](/guide/scheduling) **and advance its next occurrence in the same transaction**, so one occurrence cannot fire on two workers.
- To support [per-workflow concurrency](/guide/concurrency#limiting-one-workflow), set `supportsConcurrencyLimits = true` and honour `claimNextRun`'s third argument, a map of workflow name to limit: skip a workflow whose live `running` runs already reach its limit, counted **in the same transaction as the claim**.

See the [Storage API reference](/api/storage) for the full interface and each method's contract.

Two methods are optional, and only make the [dashboard](/guide/dashboard) cheaper to run against your storage; it works without them:

- `getStepResultsForRuns(runIds)` — several runs' steps in one read, rather than one `getStepResults` call per listed run.
- `runActivity({ workflow?, since, bucketMs, buckets })` — runs started, completed, and failed per time bucket. Without it the dashboard counts a sample of recent runs and shows only the stretch of time that sample covers.

Index your runs by creation time — overall and per workflow — and by workflow and finish time, so listing the newest runs reads a few rows rather than sorting the table. Keep the query that claims runs on your status indexes: an index that leads with workflow, chosen for a claim, reads every finished run of the workflow and gets slower as history grows.

### Verifying your adapter

Rather than inferring the contract from prose, run it. The suite the built-in adapters are held to ships with the package:

```typescript
import { storageConformanceCases } from 'reflow-ts/conformance'
import { MyStorage } from './my-storage'

for (const testCase of storageConformanceCases) {
  it(testCase.name, async () => {
    const storage = new MyStorage(/* ... */)
    await storage.initialize()
    try {
      await testCase.run(storage)
    } finally {
      storage.close()
    }
  })
}
```

Each case takes a freshly initialized, empty adapter and throws on failure, so it works with any test runner — or none. The cases deliberately avoid any test-framework import, which is also how the Bun adapter is covered: `bun:sqlite` cannot load under the Node-based test run, so the same list is driven against it by a Bun script.

Passing every case does not prove an adapter is correct under real concurrency — the atomicity requirements above need a database that actually enforces them — but failing one is a definite bug.
