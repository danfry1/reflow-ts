# Retry & Timeouts

## Retry

Configure automatic retry and backoff per step using the config form of `.step()`:

```typescript
.step('call-api', {
  retry: {
    maxAttempts: 5,
    backoff: 'exponential', // or 'linear'
    initialDelayMs: 200,    // 200ms, 400ms, 800ms, 1600ms…
  },
  handler: async ({ input }) => {
    const response = await fetch(`https://api.example.com/${input.id}`)
    if (!response.ok) throw new Error(`API error: ${response.status}`)
    return await response.json()
  },
})
```

The delay between attempts grows with the backoff strategy:

- **`linear`** — `initialDelayMs × attempt`: `200ms, 400ms, 600ms, …`
- **`exponential`** — `initialDelayMs × 2^(attempt-1)`: `200ms, 400ms, 800ms, …`

`initialDelayMs` defaults to `1000`. Without a `retry` config, a failing step **immediately fails the entire run** — there is no implicit retry.

| Field | Type | Description |
|---|---|---|
| `maxAttempts` | `number` | Maximum attempts before the step fails (must be ≥ 1) |
| `backoff` | `'linear' \| 'exponential'` | Delay growth strategy (required) |
| `initialDelayMs` | `number` | Base delay in ms (default `1000`) |
| `timeoutMs` | `number` | Per-attempt timeout (see below) |

A step's return value is saved, so return plain data — `response.json()`, not the `Response`.

In a [workflow written as a function](/guide/functions#step-run-name-handler-config), `step.run` takes the same settings under different names: `retry.initialDelay` and `timeout`, as milliseconds or a duration such as `'30s'`. There `backoff` is optional and defaults to `'linear'`.

## Backoff is durable

On a [started](/guide/engine#polling-vs-manual-ticks) engine, the run is suspended in storage between attempts rather than waiting in memory. While it backs off:

- it holds **no lease and no [concurrency](/guide/concurrency) slot**, so a long backoff does not stop other runs from executing;
- the run is `sleeping`, with `wakeAt` set to the next attempt, and the step is `retrying`, recording the attempts so far and the latest error;
- a crash or deploy **does not reset the attempt count** — whichever engine picks the run up continues at the next attempt, and `maxAttempts` holds across restarts.

```typescript
const info = await engine.getRunStatus(runId)
const step = info?.steps.find((s) => s.name === 'call-api')
info?.run.status // 'sleeping'
info?.run.wakeAt // when the next attempt starts
step?.status     // 'retrying'
step?.attempts   // attempts made so far
step?.error      // the latest attempt's error
```

A started engine wakes a run backing off for under five minutes on time; longer backoffs are picked up by the next poll after they end. `onStepStart` fires once, before the first attempt, and `onStepComplete` once the step finally succeeds.

Two cases wait in process instead, as before:

- An engine driven only by `tick()` — tests, CLI scripts — has no polling to resume a suspended run, so it waits out the backoff and the tick still finishes the step. [`testEngine`](/guide/testing) works this way.
- Branches of a [`.parallel()`](/guide/parallel) group: their siblings are still running.

`onRunStart` fires each time an engine picks the run up again after a backoff, as it does after a sleep or a wait.

## Timeouts

Stop a step from hanging indefinitely with `timeoutMs`:

```typescript
.step('call-external-api', {
  timeoutMs: 5000, // fail the attempt after 5 seconds
  handler: async ({ input, signal }) => {
    const response = await fetch(`https://slow-api.example.com/${input.id}`, { signal })
    return await response.json()
  },
})
```

A timeout aborts the step's `signal`. When the attempt exceeds `timeoutMs`, it fails with a [`StepTimeoutError`](/guide/error-handling) — which, if retries remain, counts as one failed attempt.

You can also set a timeout inside the retry config so it applies per attempt:

```typescript
.step('flaky-service', {
  retry: {
    maxAttempts: 3,
    backoff: 'exponential',
    initialDelayMs: 500,
    timeoutMs: 10000, // each attempt times out after 10s
  },
  handler: async ({ input, signal }) => { /* … */ },
})
```

Step-level `timeoutMs` takes precedence over `retry.timeoutMs`.

::: tip Cooperate with the signal
Timeouts and cancellation both work through the step's `AbortSignal`. Forward it to `fetch`, database drivers, and other abortable APIs so work actually stops when the deadline passes.
:::

## Retry vs. recover vs. compensate

Retry is one of three error-handling tools. Use it for **transient** failures. For expected errors, `try/catch` inside the step; for cleanup after a run has failed, use [`onFailure`](/guide/failure-handling). See [Error Handling](/guide/error-handling) for how they combine.
