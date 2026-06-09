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
| `backoff` | `'linear' \| 'exponential'` | Delay growth strategy |
| `initialDelayMs` | `number` | Base delay in ms (default `1000`) |
| `timeoutMs` | `number` | Per-attempt timeout (see below) |

## Timeouts

Stop a step from hanging indefinitely with `timeoutMs`:

```typescript
.step('call-external-api', {
  timeoutMs: 5000, // fail the attempt after 5 seconds
  handler: async ({ input, signal }) => {
    return await fetch(`https://slow-api.example.com/${input.id}`, { signal })
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
