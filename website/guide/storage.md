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

## MemoryStorage

An in-memory adapter, used internally by the [test helper](/guide/testing). For custom use, import it from `reflow-ts/test`. State is lost when the process exits — it offers no durability, so it's for tests and ephemeral work only.

```typescript
import { MemoryStorage } from 'reflow-ts/test'
```

## Persistable values

Everything Reflow stores — workflow input and step output — must be plain data: objects, arrays, strings, numbers, booleans, `null`, `undefined`, and `Date`. Returning a non-serializable value (a function, a class instance, `NaN`) throws a [`SerializationError`](/guide/error-handling).

## Custom adapters

Implement the [`StorageAdapter`](/api/storage) interface to back Reflow with any database — Postgres, MySQL, Redis, a hosted KV. The contract is small, but two methods carry the durability guarantees:

- `claimNextRun(workflowNames, staleBefore?)` must **atomically** claim the next pending or stale run, returning a unique `leaseId`. This is what prevents two workers from running the same run.
- `saveStepResult` / `updateClaimedRunStatus` take a `leaseId` and must **no-op when the lease no longer matches**, so a worker that lost its lease can't clobber a run another worker has taken over.

```typescript
interface StorageAdapter {
  initialize(): Promise<void>
  createRun(run: WorkflowRun): Promise<CreateRunResult>
  claimNextRun(workflowNames: readonly string[], staleBefore?: number): Promise<ClaimedRun | null>
  heartbeatRun(runId: string, leaseId: string): Promise<boolean>
  getRun(runId: string): Promise<WorkflowRun | null>
  getStepResults(runId: string): Promise<StepResult[]>
  saveStepResult(result: StepResult, leaseId?: string): Promise<boolean>
  updateRunStatus(runId: string, status: RunStatus): Promise<boolean>
  updateClaimedRunStatus(runId: string, leaseId: string, status: RunStatus): Promise<boolean>
  close(): void
}
```

See the [Storage API reference](/api/storage) for each method's contract.
