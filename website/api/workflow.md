# Workflow methods

Methods on the [`Workflow`](/api/create-workflow) builder. All are immutable — each returns a new `Workflow`.

## `.step(name, handler | config)`

Adds a sequential step. Accepts either a bare handler or a config object.

**Handler form:**

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
| `steps` | `TStepsSoFar` | Frozen, typed record of all previously completed step results by name |
| `signal` | `AbortSignal` | Aborted on cancellation, lease loss, or step timeout |
| `complete` | `(value?) => never` | Finish the workflow early, skipping remaining steps |

**Config form:**

| Field | Type | Description |
|---|---|---|
| `handler` | `(ctx) => Promise<T>` | Step handler. Receives the step context above. |
| `retry` | `RetryConfig` | Optional retry configuration. |
| `timeoutMs` | `number` | Optional timeout per attempt (ms). Takes precedence over `retry.timeoutMs`. |

**`RetryConfig`:**

| Field | Type | Description |
|---|---|---|
| `maxAttempts` | `number` | Maximum attempts (default `1`, no retry) |
| `backoff` | `'linear' \| 'exponential'` | Backoff strategy between retries |
| `initialDelayMs` | `number` | Base delay in ms (default `1000`) |
| `timeoutMs` | `number` | Timeout per attempt; step-level `timeoutMs` wins |

The step's return value must be a [persistable value](/guide/storage#persistable-values). Reusing a name throws [`DuplicateStepError`](/api/errors). See [Retry & Timeouts](/guide/retry).

## `.parallel(branches)`

Adds a group of concurrent steps. `branches` is a record of `{ branchName: handler | config }`. All branches run at once; the next step's `prev` is the merged `{ [branchName]: output }`.

```typescript
.parallel({
  a: async ({ prev }) => ({ x: prev.value * 2 }),
  b: {
    retry: { maxAttempts: 3, backoff: 'linear' },
    timeoutMs: 5000,
    handler: async () => await someCall(),
  },
})
```

Each branch accepts the same handler/config form as `.step()`. Branch names share the step namespace — duplicates throw [`DuplicateStepError`](/api/errors). At least one branch is required. Calling `complete()` inside a branch throws [`ParallelCompleteError`](/api/errors). See [Parallel Steps](/guide/parallel).

## `.sleep(name, duration)`

Durably pauses the workflow for `duration` before the next step. The run is persisted as `sleeping` and its lease released, so the process can exit during the wait; any engine instance resumes it once the time elapses. `prev` passes through unchanged.

```typescript
.step('start-trial', async ({ input }) => provisionTrial(input.userId))
.sleep('trial-period', '14d')
.step('charge', async ({ input }) => convertOrExpire(input.userId))
```

`duration` is a number of milliseconds or a string with a unit suffix (`'500ms'`, `'30s'`, `'15m'`, `'24h'`, `'7d'`); an invalid value throws [`ConfigError`](/api/errors). `name` shares the step namespace — duplicates throw [`DuplicateStepError`](/api/errors). See [Durable Sleep](/guide/sleep).

## `.waitForEvent(name, options?)`

Durably pauses the workflow until an external event named `name` is delivered via [`engine.sendEvent(runId, name, payload)`](/api/engine). The run is persisted as `waiting` with its lease released, so it survives process exit and resumes — on any engine instance — when the event arrives. The delivered payload becomes the next step's `prev` (and `steps[name]`).

```typescript
.step('request-approval', async ({ input }) => notifyApprover(input.requestId))
.waitForEvent('approved', { schema: z.object({ approver: z.string() }), timeoutMs: 24 * 60 * 60 * 1000 })
.step('proceed', async ({ prev }) => fulfil(prev.approver)) // prev = the event payload
```

| Option | Type | Description |
|---|---|---|
| `schema` | `StandardSchemaV1<T>` | Validates the payload on delivery; infers the `prev`/`steps[name]` type as `T`. |
| `timeoutMs` | `number` | If set, the run fails with [`WaitTimeoutError`](/api/errors) when no event arrives within this many ms. |

Delivery is durable and order-independent — an event sent before the run reaches the wait is buffered and consumed when it gets there. `name` shares the step namespace (duplicates throw [`DuplicateStepError`](/api/errors)). See [Waiting for Events](/guide/wait-for-event).

## `.onFailure(handler)`

Attaches a compensation handler, called when a step fails after exhausting its retries.

```typescript
.onFailure(async ({ error, stepName, input }) => {
  // roll back side effects based on how far the run got
})
```

| Field | Type | Description |
|---|---|---|
| `error` | `Error` | The error that caused the failure |
| `stepName` | `string` | The step (or parallel branch) that failed |
| `input` | `TInput` | The original validated input |

The handler runs after the run is already marked `failed`; errors it throws are swallowed. See [Failure Handling](/guide/failure-handling).
