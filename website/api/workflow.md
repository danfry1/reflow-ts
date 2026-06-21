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
| `when` | `(ctx) => boolean \| Promise<boolean>` | Optional predicate. Receives `{ input, prev, steps }`. When it returns `false` the step is skipped (see below). |

**Conditional steps (`when`):**

When `when` returns `false`, the step does not run: `prev` passes through unchanged to the next step, the step is recorded with status `skipped`, and a [`stepSkipped`](/api/events) event fires. The decision is evaluated once and persisted, so it is never recomputed on replay. A skipped step is `undefined` in the next step's `steps` map (read it via `prev` instead). If `when` throws, the run fails at that step (reaching `onFailure` / `onRunFailed`). `when` is supported on sequential `.step()` only — passing it to a `.parallel()` branch throws [`ConfigError`](/api/errors).

```typescript
.step('charge', async () => ({ tier: 'base' }))
.step('upgrade', {
  when: ({ input }) => input.premium,   // only run for premium orders
  handler: async () => ({ tier: 'premium' }),
})
.step('finalize', async ({ prev }) => prev.tier) // 'base' when upgrade was skipped
```

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
