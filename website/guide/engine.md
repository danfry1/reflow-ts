# The Engine

The engine connects workflows to storage and handles execution, polling, leasing, and scheduling.

```typescript
import { createEngine } from 'reflow-ts'
import { SQLiteStorage } from 'reflow-ts/sqlite-node'

const storage = new SQLiteStorage('./workflows.db')
const engine = createEngine({ storage, workflows: [orderWorkflow, emailWorkflow] })

// start() initializes storage and begins polling
await engine.start(1000) // poll every 1000ms (default)

const run = await engine.enqueue('order-fulfillment', { orderId: 'ORD_1', amount: 100 })

// Stop polling (waits for in-flight work to finish)
await engine.stop()
```

## Enqueuing runs

`enqueue()` is fully type-safe — it only accepts registered workflow names and their corresponding input types:

```typescript
engine.enqueue('order-fulfillment', { orderId: 'x', amount: 1 }) // ✅ OK
engine.enqueue('order-fulfillment', { wrong: 'shape' })          // ❌ Type error
engine.enqueue('nonexistent', {})                                 // ❌ Type error
```

The returned `WorkflowRun` carries `run.id`, which you pass to [`getRunStatus()`](/api/engine) or [`cancel()`](/guide/cancellation).

## Idempotency

If callers may retry `enqueue()` (a flaky client, an at-least-once queue), give the run an idempotency key:

```typescript
const run = await engine.enqueue(
  'order-fulfillment',
  { orderId: 'ORD_1', amount: 100 },
  { idempotencyKey: 'checkout:ORD_1' },
)
```

Reusing the same key for the same workflow **returns the existing run** instead of creating a duplicate. Reusing it with **different** input throws [`IdempotencyConflictError`](/guide/error-handling).

## Leases and heartbeats

When the engine claims a run it takes a **lease** (default `30_000ms`). While a run executes, the engine **heartbeats** the lease so long-running steps aren't reclaimed mid-flight. If a worker crashes and stops heartbeating, a later `tick()` on any engine instance can reclaim the run once the lease expires — this is what makes [crash recovery](/guide/crash-recovery) work.

```typescript
const engine = createEngine({
  storage,
  workflows: [orderWorkflow],
  runLeaseDurationMs: 30_000, // how long a claim is valid without a heartbeat
  heartbeatIntervalMs: 10_000, // how often to renew (default: lease / 3)
})
```

`heartbeatIntervalMs` must be smaller than `runLeaseDurationMs`.

## Polling vs. manual ticks

`start(pollIntervalMs?)` runs a `tick()` on an interval. For CLI tools or tests, call `tick()` yourself to process exactly one batch:

```typescript
await storage.initialize() // start() does this for you; tick() alone does not
await engine.enqueue('order-fulfillment', { orderId: 'ORD_1', amount: 100 })
await engine.tick()
```

Each tick claims up to [`concurrency`](/guide/concurrency) runs and executes them in parallel.

## Engine configuration

| Option | Type | Default | Description |
|---|---|---|---|
| `storage` | `StorageAdapter` | required | Backend for runs and step results |
| `workflows` | `Workflow[]` | required | Workflows to register |
| `hooks` | `EngineHooks` | — | [Lifecycle hooks](/guide/hooks) (may be `async`) |
| `concurrency` | `number` | `1` | Runs processed in parallel per tick |
| `runLeaseDurationMs` | `number` | `30000` | Lease validity before reclaim |
| `heartbeatIntervalMs` | `number` | `lease / 3` | Lease renewal interval |

See the [createEngine API](/api/create-engine) for the full reference.
