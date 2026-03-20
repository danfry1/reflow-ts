# Reflow

Durable workflow execution for TypeScript. Define multi-step workflows with full type safety, automatic retries, and crash recovery via stale-run reclamation — powered by SQLite, no external services required.

```typescript
import { createWorkflow, createEngine } from 'reflow-ts'
import { SQLiteStorage } from 'reflow-ts/sqlite-node'
import { z } from 'zod' // or valibot, arktype, etc.

const orderWorkflow = createWorkflow({
  name: 'order-fulfillment',
  input: z.object({ orderId: z.string(), amount: z.number() }),
})
  .step('charge', async ({ input, signal }) => {
    const charge = await stripe.charges.create({ amount: input.amount })
    return { chargeId: charge.id }
  })
  .step('fulfill', async ({ prev }) => {
    const shipment = await warehouse.ship(prev.chargeId)
    return { trackingNumber: shipment.tracking }
  })
  .step('notify', async ({ prev, input }) => {
    await email.send(input.orderId, `Shipped! Track: ${prev.trackingNumber}`)
  })
  .onFailure(async ({ error, stepName, input }) => {
    await alerts.send(`Order ${input.orderId} failed at ${stepName}: ${error.message}`)
  })

const storage = new SQLiteStorage('./workflows.db')
const engine = createEngine({ storage, workflows: [orderWorkflow] })
await engine.start() // Initializes storage and starts polling

// Type-safe: only accepts 'order-fulfillment' with the correct input shape
await engine.enqueue('order-fulfillment', { orderId: 'ORD_123', amount: 5000 })
```

## The Problem

You have a multi-step operation — a signup, an import, an AI pipeline. You write it as a normal async function:

```typescript
app.post('/signup', async (req, res) => {
  await createAccount(req.body)     // ✅ done
  await chargeStripe(req.body)      // ✅ done
  // 💥 process crashes, deploy happens, laptop sleeps
  await sendWelcomeEmail(req.body)  // ❌ never runs
})
```

Now the user is charged but never got their welcome email. Worse — you don't know which steps completed. Do you re-run everything? Then they get double-charged.

**The usual fix** is to build manual checkpoint logic: state columns, retry loops, deduplication. That's 200 lines of infrastructure code that's hard to test and easy to get wrong.

**Reflow makes each step durable.** If the process crashes after step 2 of 5, a new engine instance can reclaim the stale run after its lease expires and pick up at step 3. Active workers heartbeat their lease while they run, completed steps are never re-executed, and each step's output is persisted in SQLite — no external services required.

## Who Is This For?

**Solo devs and small teams** who need reliable multi-step workflows but don't want to run Temporal clusters or pay for cloud workflow services.

- **SaaS apps** — Background jobs that must complete: signup flows, billing, provisioning
- **CLI tools** — Long-running imports or migrations that should resume after interruption
- **AI pipelines** — LLM calls that cost money — don't re-run a $0.05 call because the next step failed

| | Reflow | Temporal | Inngest |
|---|---|---|---|
| Infrastructure | None (SQLite file) | Temporal Server + DB | Cloud service |
| Type safety | Full end-to-end | Partial | Partial |
| Setup | `bun add reflow-ts`  | Cluster deployment | Account + SDK |
| Best for | Single-process apps, CLIs, AI agents | Large distributed systems | Serverless |

**Don't use Reflow when:**
- You need distributed execution across multiple machines
- You need sub-second latency on workflow dispatch
- You're already running Temporal or similar

## Install

```bash
# Bun (uses built-in bun:sqlite — no native dependencies)
bun add reflow-ts

# Node.js (requires better-sqlite3)
npm install reflow-ts better-sqlite3
```

Then pick a storage adapter based on your runtime:

```typescript
// Bun — zero native deps
import { SQLiteStorage } from 'reflow-ts/sqlite-bun'
const storage = new SQLiteStorage('./reflow.db')

// Node.js — uses better-sqlite3
import { SQLiteStorage } from 'reflow-ts/sqlite-node'
const storage = new SQLiteStorage('./reflow.db')
```

Reflow uses [Standard Schema](https://github.com/standard-schema/standard-schema) for input validation, so you can bring any compatible library:

```bash
bun add zod        # or
bun add valibot    # or
bun add arktype    # or any Standard Schema-compatible library
```

## Core Concepts

### Workflows

A workflow is a named sequence of steps with a validated input schema. Any [Standard Schema](https://github.com/standard-schema/standard-schema)-compatible library works (Zod, Valibot, ArkType, etc.).

```typescript
const workflow = createWorkflow({
  name: 'send-welcome',
  input: z.object({ userId: z.string(), email: z.email() }),
})
  .step('create-account', async ({ input }) => {
    // input is typed as { userId: string, email: string }
    return { accountId: await createAccount(input.userId) }
  })
  .step('send-email', async ({ prev, input, signal }) => {
    // prev is typed as { accountId: string }
    // input is still available
    // signal is aborted on cancellation / timeout
    await sendEmail(input.email, `Welcome! Your account: ${prev.accountId}`, { signal })
  })
```

Each `.step()` receives:
- `input` — the validated workflow input (same for every step)
- `prev` — the return value of the previous step (`undefined` for the first step)
- `steps` — typed access to all previously completed step results by name (e.g. `steps.charge.chargeId`)
- `signal` — an `AbortSignal` that is aborted when the run is cancelled, its lease is lost, or the step times out
- `complete(value?)` — finish the workflow early, skipping remaining steps (optionally persist a final value)

The builder is **immutable** — each `.step()` returns a new workflow instance, so you can safely branch:

```typescript
const base = createWorkflow({ name: 'base', input: z.object({}) })
const withLogging = base.step('log', async () => { /* ... */ })
const withMetrics = base.step('metric', async () => { /* ... */ })
// base, withLogging, and withMetrics are all independent
```

### Engine

The engine connects workflows to storage and handles execution.

```typescript
const storage = new SQLiteStorage('./workflows.db')
const engine = createEngine({ storage, workflows: [orderWorkflow, emailWorkflow] })

// start() initializes storage and begins polling
await engine.start(1000) // poll every 1000ms (default)

// Enqueue a run
const run = await engine.enqueue('order-fulfillment', { orderId: 'ORD_1', amount: 100 })
// run.id is a unique identifier for this run

// Stop polling (waits for in-flight work to finish)
await engine.stop()
```

By default, claimed runs use a `30_000ms` lease. If a worker crashes and stops updating a run, a later `tick()` can reclaim it after that lease expires:

```typescript
const engine = createEngine({
  storage,
  workflows: [orderWorkflow],
  runLeaseDurationMs: 30_000,
  heartbeatIntervalMs: 10_000,
})
```

The engine heartbeats active runs while they execute so long-running steps do not get reclaimed before they finish.

`enqueue()` is fully type-safe — it only accepts registered workflow names and their corresponding input types:

```typescript
engine.enqueue('order-fulfillment', { orderId: 'x', amount: 1 }) // OK
engine.enqueue('order-fulfillment', { wrong: 'shape' })          // Type error
engine.enqueue('nonexistent', {})                                 // Type error
```

If callers may retry `enqueue()`, give the run an idempotency key:

```typescript
const run = await engine.enqueue(
  'order-fulfillment',
  { orderId: 'ORD_1', amount: 100 },
  { idempotencyKey: 'checkout:ORD_1' },
)
```

Reusing the same idempotency key for the same workflow returns the existing run instead of creating a duplicate. Reusing it with different input throws.

### Retry

Steps can be configured with automatic retry and backoff:

```typescript
.step('call-api', {
  retry: {
    maxAttempts: 5,
    backoff: 'exponential', // or 'linear'
    initialDelayMs: 200,    // 200ms, 400ms, 800ms, 1600ms...
  },
  handler: async ({ input }) => {
    const response = await fetch(`https://api.example.com/${input.id}`)
    if (!response.ok) throw new Error(`API error: ${response.status}`)
    return await response.json()
  },
})
```

Without retry config, a failing step immediately fails the entire workflow run.

### Failure Handling

Attach an `onFailure` handler for compensation logic (saga pattern):

```typescript
const workflow = createWorkflow({ name: 'transfer', input: schema })
  .step('debit', async ({ input }) => {
    return await debitAccount(input.from, input.amount)
  })
  .step('credit', async ({ input }) => {
    return await creditAccount(input.to, input.amount)
  })
  .onFailure(async ({ error, stepName, input }) => {
    if (stepName === 'credit') {
      // Debit succeeded but credit failed — reverse the debit
      await creditAccount(input.from, input.amount)
    }
    await notifyOps(`Transfer failed at ${stepName}: ${error.message}`)
  })
```

### Steps Context

Each step handler receives a typed `steps` object with access to all previously completed step results by name. No need to forward data through `prev` across intermediate steps:

```typescript
const workflow = createWorkflow({ name: 'pipeline', input: schema })
  .step('fetch', async ({ input }) => {
    return { url: input.url, body: await fetchPage(input.url) }
  })
  .step('parse', async ({ prev }) => {
    return { title: extractTitle(prev.body), links: extractLinks(prev.body) }
  })
  .step('save', async ({ steps }) => {
    // Access any previous step directly — no forwarding needed
    await save(steps.fetch.url, steps.parse.title, steps.parse.links)
  })
```

The `steps` object is a frozen, deep-cloned snapshot — mutations to `prev` in one step will never affect what later steps see through `steps`.

### Early Completion

A step can finish the workflow early by calling `complete()`, skipping all remaining steps:

```typescript
const workflow = createWorkflow({ name: 'conditional', input: schema })
  .step('check', async ({ input, complete }) => {
    if (!input.eligible) {
      return complete({ reason: 'ineligible' })
    }
    return { eligible: true }
  })
  .step('process', async ({ prev }) => {
    // Only runs if check didn't call complete()
    return await doWork(prev)
  })
```

The optional value passed to `complete()` is persisted as the step result and visible via `getRunStatus()`. Early completion is crash-safe — if the engine crashes after saving the step but before marking the run completed, recovery will detect the early-complete marker and finish the run without re-executing later steps.

### Run Status

Query the status of any run and its step results:

```typescript
const run = await engine.enqueue('order-fulfillment', { orderId: 'ORD_1', amount: 100 })

// Later...
const info = await engine.getRunStatus(run.id)
if (info) {
  info.run.status    // 'pending' | 'running' | 'completed' | 'failed' | 'cancelled'
  info.steps         // StepResult[] — each step's output, error, and attempt count
}
```

### Hooks

Add observability with lifecycle hooks:

```typescript
const engine = createEngine({
  storage,
  workflows: [orderWorkflow],
  hooks: {
    onStepComplete: ({ runId, stepName, output, attempts }) => {
      console.log(`Step ${stepName} completed in ${attempts} attempt(s)`)
    },
    onRunComplete: ({ runId, workflow }) => {
      metrics.increment('workflow.completed', { workflow })
    },
    onRunFailed: ({ runId, workflow, stepName, error }) => {
      alerting.notify(`${workflow} failed at ${stepName}: ${error.message}`)
    },
    onError: (error) => {
      // Fires on background failures (scheduled enqueues, poll cycles)
      console.error('Engine error:', error)
    },
  },
})
```

### Step Timeouts

Prevent steps from hanging indefinitely:

```typescript
.step('call-external-api', {
  timeoutMs: 5000, // Fail after 5 seconds
  handler: async ({ input }) => {
    return await fetch(`https://slow-api.example.com/${input.id}`)
  },
})
```

Timeouts can also be set via the retry config:

```typescript
.step('flaky-service', {
  retry: {
    maxAttempts: 3,
    backoff: 'exponential',
    initialDelayMs: 500,
    timeoutMs: 10000, // Each attempt times out after 10s
  },
  handler: async ({ input }) => { /* ... */ },
})
```

Step-level `timeoutMs` takes precedence over `retry.timeoutMs`.

### Concurrency

By default, the engine processes one run at a time. Set `concurrency` to process multiple runs in parallel per tick:

```typescript
const engine = createEngine({
  storage,
  workflows: [orderWorkflow],
  concurrency: 5, // Process up to 5 runs in parallel per tick (default: 1)
})
```

With `concurrency: 5`, each tick claims up to 5 pending runs and executes them concurrently. Steps within a single run still execute sequentially.

### Run Cancellation

Cancel pending or running workflows:

```typescript
const run = await engine.enqueue('order-fulfillment', { orderId: 'ORD_1', amount: 100 })

const cancelled = await engine.cancel(run.id)
// true if cancelled, false if already completed/failed/cancelled
```

Cancellation aborts the current step's `AbortSignal` immediately and prevents later steps from starting. If a handler ignores the signal, its underlying work may continue outside Reflow, but the run remains `cancelled`.

If your step handler cooperates with the provided `AbortSignal`, cancellation can stop it immediately:

```typescript
.step('fetch-profile', async ({ input, signal }) => {
  const response = await fetch(`https://api.example.com/users/${input.userId}`, { signal })
  return await response.json()
})
```

### Scheduled Workflows

Enqueue workflows on a recurring interval:

```typescript
// Enqueue a cleanup workflow every hour
const scheduleId = engine.schedule('cleanup', { olderThanDays: 30 }, 60 * 60 * 1000)

// Stop the schedule
engine.unschedule(scheduleId)

// await engine.stop() also clears all schedules
```

### Crash Recovery

Reflow automatically resumes workflows from the last completed step. If your process crashes after step 2 of 5, a later engine instance can reclaim the stale `running` run after `runLeaseDurationMs` and continue at step 3 — completed steps are never re-executed.

```typescript
// Process crashes here after 'charge' completed but before 'fulfill'
// On restart, the engine claims the run and skips 'charge'
await engine.start()
```

### Storage

Reflow ships with three storage adapters:

**SQLiteStorage** — for Bun runtime. Uses the built-in `bun:sqlite` module with zero native dependencies.

```typescript
import { SQLiteStorage } from 'reflow-ts/sqlite-bun'

const storage = new SQLiteStorage('./workflows.db')
```

**SQLiteStorage** — for Node.js. Uses `better-sqlite3` (native addon). Persists to disk, uses WAL mode.

```typescript
import { SQLiteStorage } from 'reflow-ts/sqlite-node'

const storage = new SQLiteStorage('./workflows.db')
```

**MemoryStorage** — used internally by the test helper. For custom use, import from `reflow-ts/test`.

```typescript
import { testEngine } from 'reflow-ts/test'
```

You can implement your own adapter by conforming to the `StorageAdapter` interface:

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

Persisted workflow input and step output must be plain data: objects, arrays, strings, numbers, booleans, `null`, `undefined`, and `Date`.

## Testing

Reflow includes a test helper that runs workflows synchronously and returns typed results:

```typescript
import { testEngine } from 'reflow-ts/test'

const te = testEngine({ workflows: [orderWorkflow] })

const result = await te.run('order-fulfillment', { orderId: 'test', amount: 100 })

result.status              // 'completed' | 'failed'
result.steps.charge.output // { chargeId: string } — fully typed
result.steps.charge.status // 'completed' | 'failed'
result.steps.charge.error  // string | null
```

Use it in your test suite:

```typescript
import { describe, it, expect } from 'vitest'
import { testEngine } from 'reflow-ts/test'

describe('order workflow', () => {
  it('charges and fulfills', async () => {
    const te = testEngine({ workflows: [orderWorkflow] })
    const result = await te.run('order-fulfillment', { orderId: 'ORD_1', amount: 100 })

    expect(result.status).toBe('completed')
    expect(result.steps.charge.output.chargeId).toBeTruthy()
    expect(result.steps.fulfill.output.trackingNumber).toBeTruthy()
  })
})
```

## Type Safety

Reflow tracks types through the entire workflow chain:

- **Workflow name** is a string literal type (`'order-fulfillment'`, not `string`)
- **Input** is validated by your schema library and inferred at the type level
- **Step chaining** — each step's `prev` is typed as the return value of the previous step
- **Engine** — `enqueue()` only accepts registered workflow names with matching input
- **Test engine** — `run()` returns typed step results keyed by step name

```typescript
// These are all compile-time errors, not runtime surprises:
engine.enqueue('typo', {})                    // 'typo' is not a registered workflow
engine.enqueue('order-fulfillment', {})       // missing required fields
workflow.step('x', async ({ prev }) => {
  prev.nonexistent                            // property doesn't exist on prev
})
```

## Error Handling

Every error Reflow throws extends `ReflowError`, so you can catch all Reflow errors with a single `instanceof` check. More specific subclasses carry structured context — no message parsing needed.

```typescript
import {
  ReflowError,
  WorkflowNotFoundError,
  ValidationError,
  IdempotencyConflictError,
  StepTimeoutError,
} from 'reflow-ts'

try {
  await engine.enqueue('nonexistent', {})
} catch (error) {
  if (error instanceof WorkflowNotFoundError) {
    console.log(error.workflowName) // 'nonexistent'
  }
  if (error instanceof ValidationError) {
    console.log(error.issues) // [{ message: '...', path: [...] }]
  }
  if (error instanceof ReflowError) {
    // Catch-all for any Reflow error
  }
}
```

In hooks, you can identify timeout failures:

```typescript
hooks: {
  onRunFailed: ({ error }) => {
    if (error instanceof StepTimeoutError) {
      console.log(`Timed out after ${error.timeoutMs}ms`)
    }
  },
}
```

**Available error classes:**

| Error | Thrown when | Structured properties |
|---|---|---|
| `ReflowError` | Base class for all errors | — |
| `ConfigError` | Invalid engine, retry, or schedule config | — |
| `WorkflowNotFoundError` | `enqueue()` / `schedule()` with unknown name | `workflowName` |
| `DuplicateWorkflowError` | Same workflow registered twice | `workflowName` |
| `DuplicateStepError` | `.step()` reuses an existing name | `workflowName`, `stepName` |
| `ValidationError` | Input fails schema validation | `issues` |
| `IdempotencyConflictError` | Same idempotency key with different input | `workflowName`, `idempotencyKey` |
| `SerializationError` | Step output contains non-JSON data (NaN, functions, etc.) | `path` |
| `StepTimeoutError` | Step exceeds `timeoutMs` | `timeoutMs` |
| `RunCancelledError` | Run cancelled via `engine.cancel()` | `runId` |
| `LeaseExpiredError` | Worker lost its lease on a run | `runId` |

## API Reference

### `createWorkflow(config)`

Creates a new workflow builder.

| Parameter | Type | Description |
|---|---|---|
| `config.name` | `string` | Unique workflow name (becomes a literal type) |
| `config.input` | `StandardSchemaV1` | Any Standard Schema-compatible schema for input validation |

Returns a `Workflow` with `.step()`, `.onFailure()`, and `.parseInput()` methods.

### `workflow.step(name, handler | config)`

Adds a step to the workflow. Accepts either a handler function or a config object.

**Handler function form:**

```typescript
.step('name', async ({ input, prev, steps, signal, complete }) => {
  return { result: 'value' }
})
```

**Step context:**

| Field | Type | Description |
|---|---|---|
| `input` | `TInput` | Validated workflow input (same for every step) |
| `prev` | `TPrev` | Return value of the previous step (`undefined` for the first step) |
| `steps` | `TStepsSoFar` | Typed record of all previously completed step results by name |
| `signal` | `AbortSignal` | Aborted on cancellation, lease loss, or step timeout |
| `complete` | `(value?) => never` | Finish the workflow early, skipping remaining steps |

**Config object form:**

| Parameter | Type | Description |
|---|---|---|
| `handler` | `(ctx) => Promise<T>` | Step handler. Receives `{ input, prev, steps, signal, complete }` |
| `retry` | `RetryConfig` | Optional retry configuration (see below) |
| `timeoutMs` | `number` | Optional timeout per attempt in milliseconds |

**RetryConfig:**

| Parameter | Type | Description |
|---|---|---|
| `maxAttempts` | `number` | Maximum number of attempts (default: 1, no retry) |
| `backoff` | `'linear' \| 'exponential'` | Backoff strategy between retries |
| `initialDelayMs` | `number` | Base delay in milliseconds (default: 1000) |
| `timeoutMs` | `number` | Timeout per attempt. Step-level `timeoutMs` takes precedence |

### `workflow.onFailure(handler)`

Attaches a failure handler for compensation logic. Receives `{ error, stepName, input }`. Called when a step fails after exhausting all retry attempts.

### `createEngine(config)`

Creates an engine that executes workflows.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `config.storage` | `StorageAdapter` | required | Storage backend |
| `config.workflows` | `Workflow[]` | required | Workflows to register |
| `config.hooks` | `EngineHooks` | `undefined` | Lifecycle hooks (`onStepComplete`, `onRunComplete`, `onRunFailed`, `onError`) |
| `config.concurrency` | `number` | `1` | Number of runs to process in parallel per tick |
| `config.runLeaseDurationMs` | `number` | `30000` | How long a claimed run stays `running` before another engine may reclaim it |
| `config.heartbeatIntervalMs` | `number` | `leaseDuration / 3` | How often the active worker renews its lease |

Returns an `Engine` with the methods below.

### `engine.start(pollIntervalMs?)`

Initializes storage and starts the polling loop. Runs `tick()` every `pollIntervalMs` (default: `1000`). Call this once at startup, then `enqueue()` work as it arrives.

### `engine.stop()`

Stops the polling loop, clears all schedules, and waits for any in-flight tick to finish. Returns a `Promise<void>`.

### `engine.tick()`

Claims up to `concurrency` pending or stale runs and executes them in parallel. Useful for CLI tools or tests where you want explicit control instead of polling. If you use `tick()` without `start()`, call `storage.initialize()` first.

### `engine.enqueue(name, input, options?)`

Submits a workflow run. Type-safe - only accepts registered workflow names with their corresponding input types. Returns the created `WorkflowRun`.

| Option | Type | Description |
|---|---|---|
| `idempotencyKey` | `string` | Prevents duplicate runs. Same key + same input returns the existing run. Same key + different input throws |

### `engine.cancel(runId)`

Cancels a pending or running workflow. Returns `true` if cancelled, `false` if already completed/failed/cancelled. Aborts the current step's `AbortSignal` immediately.

### `engine.schedule(name, input, intervalMs)`

Enqueues a workflow run on a recurring interval. Returns a `scheduleId` for later cancellation with `engine.unschedule(scheduleId)`.

### `engine.getRunStatus(runId)`

Returns `{ run, steps }` with the run's current status and all step results, or `null` if not found.

### `testEngine(config)`

Creates a test engine with in-memory storage. Accepts `{ workflows }` and returns a `run()` method for synchronous workflow execution.

### `SQLiteStorage(path)` — Bun

SQLite storage adapter for Bun runtime. Uses the built-in `bun:sqlite` module — no native dependencies. WAL mode and transactional claiming.

### `SQLiteStorage(path)` — Node.js

SQLite storage adapter for Node.js. Uses `better-sqlite3`. WAL mode and transactional claiming.

## License

MIT
