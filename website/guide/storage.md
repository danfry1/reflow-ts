# Storage

Storage is pluggable through the [`StorageAdapter`](/api/storage) interface. Reflow ships three adapters.

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

Requires **Node.js ≥ 22.5** (when `node:sqlite` landed). On Node 22.x and 23.x before 23.4 it's gated behind the `--experimental-sqlite` flag; from Node 23.4 it's on by default (still experimental, so Node prints an `ExperimentalWarning`). For Node below 22.5, use the `better-sqlite3` adapter above.

::: tip Which Node adapter?
Use `sqlite-node-builtin` if you're on Node ≥ 22.5 and want to drop the `better-sqlite3` native dependency. Use `sqlite-node` for broad compatibility (Node ≥ 18.18) or to avoid the experimental module.
:::

### Read-only connections

Every SQLite adapter can open an existing database for reading only — to inspect a database another process owns, or a backup — without changing a byte of it:

```typescript
const storage = new SQLiteStorage('./workflows.db', { readOnly: true })
```

Writes fail, `initialize()` included, and the file's journal mode is left as it was. This is how [`reflow-ts dashboard`](/guide/dashboard) opens a database.

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
