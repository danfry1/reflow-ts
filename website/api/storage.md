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

SQLite adapter for Node.js using the built-in [`node:sqlite`](https://nodejs.org/api/sqlite.html) module — no native dependencies. Requires **Node.js ≥ 22.5** (no flag needed from Node 22.13 and 23.4; earlier releases need `--experimental-sqlite`). WAL mode with transactional claiming. See [Storage › Node built-in](/guide/storage#sqlitestorage-node-js-built-in).

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
  claimNextRun(workflowNames: readonly string[], staleBefore?: number, limits?: ReadonlyMap<string, number>): Promise<ClaimedRun | null>
  readonly supportsConcurrencyLimits?: boolean
  heartbeatRun(runId: string, leaseId: string): Promise<boolean>
  sleepRun(runId: string, leaseId: string, wakeAt: number): Promise<boolean>
  waitRun(runId: string, leaseId: string, eventName: string, wakeAt: number | null): Promise<boolean>
  deliverEvent(runId: string, eventName: string, payload: PersistedValue): Promise<boolean>
  takeEvent(runId: string, eventName: string): Promise<{ payload: PersistedValue } | null>
  consumeEvent?(step: Omit<StepResult, 'output'>, leaseId: string): Promise<ConsumeEventResult>
  getRun(runId: string): Promise<WorkflowRun | null>
  listRuns(filter?: ListRunsFilter): Promise<WorkflowRun[]>
  requeueRun(runId: string): Promise<boolean>
  getStepResults(runId: string): Promise<StepResult[]>
  getStepResultsForRuns?(runIds: readonly string[]): Promise<Map<string, StepResult[]>>
  runActivity?(filter: RunActivityFilter): Promise<RunActivity>
  saveStepResult(result: StepResult, leaseId?: string): Promise<boolean>
  completeRun(runId: string, leaseId: string, output: PersistedValue): Promise<boolean>
  failRun(runId: string, leaseId: string, error: RunError): Promise<boolean>
  cancelRun(runId: string): Promise<boolean>
  countRuns(filter?: CountRunsFilter): Promise<RunCounts>
  deleteRuns(filter: DeleteRunsFilter): Promise<number>
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
| `supportsConcurrencyLimits` | Set `true` when `claimNextRun` honours `limits`: skip a workflow whose live `running` runs reach its limit, counted in the same transaction as the claim. An engine whose workflows set `concurrency` refuses an adapter without it. |
| `claimNextRun` | **Atomically** claim the next pending run (a stale one older than `staleBefore`, or a `sleeping`/`waiting` run past its wake time), returning a unique `leaseId`. Must claim the run that has been claimable **longest** — pending since its `updatedAt`, sleeping/waiting since its wake time, stale since its lease expired — so new work cannot starve woken or crashed runs. Sets `startedAt` on a run's first claim only, and clears `wakeAt` / `waitingFor`. |
| `heartbeatRun` | Renew the lease. Return `false` if the lease was lost. |
| `sleepRun` | Suspend a held run to `sleeping` until `wakeAt`, releasing the lease. Backs [`.sleep()`](/api/workflow). |
| `waitRun` | Suspend a held run to `waiting` (until an event or `wakeAt`), releasing the lease and recording `eventName` as `waitingFor`. Must stay reclaimable if a matching event is already buffered. Backs [`.waitForEvent()`](/api/workflow). |
| `deliverEvent` | Durably buffer an event and wake the run if it is `waiting`. Return `false` if the run does not exist. Backs `engine.sendEvent()`. |
| `takeEvent` | Atomically consume the oldest buffered event for `(runId, eventName)`, or return `null`. |
| `consumeEvent` | Optional. In one transaction, and only while `leaseId` is held: consume the oldest buffered event named `step.name` for `step.runId` and save `step` with the event's payload as its output. Returns `{ status: 'consumed', payload }`, `{ status: 'none' }`, or `{ status: 'lease-lost' }`, changing nothing in the last two cases. Without it, an event taken by a process that dies before saving the step is lost. |
| `getRun` / `getStepResults` | Read a run / its step results. |
| `listRuns` | List runs most-recent-first, filtered by `status` / `workflow` and paged with `limit` / `before`. Backs `engine.listRuns()`. |
| `requeueRun` | Reset a `failed` / `cancelled` run to `pending`, clear `finishedAt` / `output` / `error`, and discard its `failed` step results. Return `false` if the run is not in a resumable state. Backs `engine.resume()`. |
| `saveStepResult` | Persist a step result. With a `leaseId`, must fail (return `false`) if the lease is no longer held. |
| `completeRun` | Mark a held run `completed`, recording `output` and `finishedAt` and releasing the lease. Return `false`, changing nothing, if the lease is no longer held. |
| `failRun` | Mark a held run `failed`, recording `error` (`{ stepName, message, code }`) and `finishedAt`. Same lease rule as `completeRun`. |
| `cancelRun` | Mark a run `cancelled` and record `finishedAt`, whoever holds it — unless it has already finished. The check and the write must be **atomic**, so a run completing at the same moment is never reported cancelled. Backs `engine.cancel()`. |
| `countRuns` | Runs per status, optionally for one `workflow`. Every status is present, zero or not. Backs `engine.countRuns()`. |
| `deleteRuns` | Delete up to `limit` runs in the given terminal `statuses` that finished before `finishedBefore`, oldest-finished first, with their steps and buffered events. Never deletes a run that can still progress. Backs `engine.prune()`. |
| `upsertSchedule` | Register or update a schedule by `key`. Must **preserve `nextRunAt`** when the interval is unchanged, so a redeploy rejoins the existing cadence instead of pushing the next firing out each time. |
| `claimDueSchedule` | **Atomically** claim the next schedule due at or before `now` whose workflow is in `workflowNames`, advancing its `nextRunAt` past `now` in the same transaction. Return the schedule carrying the occurrence it was claimed *for*. This atomicity is what stops N instances firing one occurrence N times. |
| `deleteSchedule` | Remove a schedule by key. Return `false` if it did not exist. |
| `listSchedules` | All schedules, ordered by key. |
| `close` | Release resources. |
