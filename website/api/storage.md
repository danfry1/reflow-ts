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
  getRun(runId: string): Promise<WorkflowRun | null>
  getStepResults(runId: string): Promise<StepResult[]>
  saveStepResult(result: StepResult, leaseId?: string): Promise<boolean>
  updateRunStatus(runId: string, status: RunStatus): Promise<boolean>
  updateClaimedRunStatus(runId: string, leaseId: string, status: RunStatus): Promise<boolean>
  close(): void
}
```

| Method | Contract |
|---|---|
| `initialize` | Create tables/indexes. Called once by `engine.start()`. |
| `createRun` | Persist a new run. Must handle idempotency-key conflicts, returning `{ run, created }`. |
| `claimNextRun` | **Atomically** claim the next pending run (or a stale one older than `staleBefore`), returning a unique `leaseId`. |
| `heartbeatRun` | Renew the lease. Return `false` if the lease was lost. |
| `getRun` / `getStepResults` | Read a run / its step results. |
| `saveStepResult` | Persist a step result. With a `leaseId`, must fail (return `false`) if the lease is no longer held. |
| `updateRunStatus` | Update status without a lease check (used for cancellation). |
| `updateClaimedRunStatus` | Update status only if the caller still holds the lease. |
| `close` | Release resources. |
