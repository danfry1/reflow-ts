# Storage

## `SQLiteStorage` — Bun

```typescript
import { SQLiteStorage } from 'reflow-ts/sqlite-bun'
const storage = new SQLiteStorage('./workflows.db')
```

SQLite adapter for the Bun runtime. Uses the built-in `bun:sqlite` module — no native dependencies. WAL mode with transactional claiming.

## `SQLiteStorage` — Node.js

```typescript
import { SQLiteStorage } from 'reflow-ts/sqlite-node'
const storage = new SQLiteStorage('./workflows.db')
```

SQLite adapter for Node.js. Uses [`better-sqlite3`](https://github.com/WiseLibs/better-sqlite3) (an optional peer dependency). WAL mode with transactional claiming.

## `SQLiteStorage` — Node.js built-in

```typescript
import { SQLiteStorage } from 'reflow-ts/sqlite-node-builtin'
const storage = new SQLiteStorage('./workflows.db')
```

SQLite adapter for Node.js using the built-in [`node:sqlite`](https://nodejs.org/api/sqlite.html) module — no native dependencies. Requires **Node.js ≥ 22.5** (`--experimental-sqlite` before Node 23.4). WAL mode with transactional claiming. See [Storage › Node built-in](/guide/storage#sqlitestorage-node-js-built-in).

All three SQLite constructors take a database file path. Pass `:memory:` for an ephemeral database.

## `MemoryStorage`

```typescript
import { MemoryStorage } from 'reflow-ts/test'
```

In-memory adapter for tests and ephemeral work. No durability across process exit.

## `testEngine(config)`

```typescript
import { testEngine } from 'reflow-ts/test'
const te = testEngine({ workflows: [orderWorkflow] })
const result = await te.run('order-fulfillment', { orderId: 'x', amount: 1 })
```

Creates a test engine backed by `MemoryStorage` that runs a workflow to completion in a single tick and returns typed step results keyed by name. See [Testing](/guide/testing).

## `StorageAdapter`

Implement this interface to back Reflow with any database. See [Storage](/guide/storage#custom-adapters) for the durability contract.

```typescript
interface StorageAdapter {
  initialize(): Promise<void>
  createRun(run: WorkflowRun): Promise<CreateRunResult>
  claimNextRun(workflowNames: readonly string[], staleBefore?: number): Promise<ClaimedRun | null>
  heartbeatRun(runId: string, leaseId: string): Promise<boolean>
  sleepRun(runId: string, leaseId: string, wakeAt: number): Promise<boolean>
  waitRun(runId: string, leaseId: string, eventName: string, wakeAt: number | null): Promise<boolean>
  deliverEvent(runId: string, eventName: string, payload: PersistedValue): Promise<boolean>
  takeEvent(runId: string, eventName: string): Promise<{ payload: PersistedValue } | null>
  getRun(runId: string): Promise<WorkflowRun | null>
  listRuns(filter?: ListRunsFilter): Promise<WorkflowRun[]>
  requeueRun(runId: string): Promise<boolean>
  getStepResults(runId: string): Promise<StepResult[]>
  saveStepResult(result: StepResult, leaseId?: string): Promise<boolean>
  updateRunStatus(runId: string, status: RunStatus): Promise<boolean>
  updateClaimedRunStatus(runId: string, leaseId: string, status: RunStatus): Promise<boolean>
  upsertSchedule(schedule: WorkflowSchedule): Promise<WorkflowSchedule>
  claimDueSchedule(workflowNames: readonly string[], now: number): Promise<WorkflowSchedule | null>
  deleteSchedule(key: string): Promise<boolean>
  listSchedules(): Promise<WorkflowSchedule[]>
  close(): void
}
```

| Method | Contract |
|---|---|
| `initialize` | Create tables/indexes. Called once by `engine.start()`. |
| `createRun` | Persist a new run. Must handle idempotency-key conflicts, returning `{ run, created }`. |
| `claimNextRun` | **Atomically** claim the next pending run (a stale one older than `staleBefore`, or a `sleeping`/`waiting` run past its wake time), returning a unique `leaseId`. |
| `heartbeatRun` | Renew the lease. Return `false` if the lease was lost. |
| `sleepRun` | Suspend a held run to `sleeping` until `wakeAt`, releasing the lease. Backs [`.sleep()`](/api/workflow). |
| `waitRun` | Suspend a held run to `waiting` (until an event or `wakeAt`), releasing the lease. Must stay reclaimable if a matching event is already buffered. Backs [`.waitForEvent()`](/api/workflow). |
| `deliverEvent` | Durably buffer an event and wake the run if it is `waiting`. Return `false` if the run does not exist. Backs `engine.sendEvent()`. |
| `takeEvent` | Atomically consume the oldest buffered event for `(runId, eventName)`, or return `null`. |
| `getRun` / `getStepResults` | Read a run / its step results. |
| `listRuns` | List runs most-recent-first, filtered by `status` / `workflow` and paged with `limit` / `before`. Backs `engine.listRuns()`. |
| `requeueRun` | Reset a `failed` / `cancelled` run to `pending` and discard its `failed` step results. Return `false` if the run is not in a resumable state. Backs `engine.resume()`. |
| `saveStepResult` | Persist a step result. With a `leaseId`, must fail (return `false`) if the lease is no longer held. |
| `updateRunStatus` | Update status without a lease check (used for cancellation). |
| `updateClaimedRunStatus` | Update status only if the caller still holds the lease. |
| `upsertSchedule` | Register or update a schedule by `key`. Must **preserve `nextRunAt`** when the interval is unchanged, so a redeploy rejoins the existing cadence instead of pushing the next firing out each time. |
| `claimDueSchedule` | **Atomically** claim the next schedule due at or before `now` whose workflow is in `workflowNames`, advancing its `nextRunAt` past `now` in the same transaction. Return the schedule carrying the occurrence it was claimed *for*. This atomicity is what stops N instances firing one occurrence N times. |
| `deleteSchedule` | Remove a schedule by key. Return `false` if it did not exist. |
| `listSchedules` | All schedules, ordered by key. |
| `close` | Release resources. |
