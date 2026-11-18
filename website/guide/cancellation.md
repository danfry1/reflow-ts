# Cancellation

Cancel any run that hasn't finished — pending, running, sleeping, or waiting — with `engine.cancel(runId)`:

```typescript
const run = await engine.enqueue('order-fulfillment', { orderId: 'ORD_1', amount: 100 })

const cancelled = await engine.cancel(run.id)
// true if cancelled, false if the run already completed / failed / cancelled
```

Cancellation is recorded at once: the run's status becomes `cancelled` and no later step starts. On the engine that called `cancel()`, the current step's `AbortSignal` aborts immediately. A run executing in another process is aborted at that process's next heartbeat — within `heartbeatIntervalMs`, 10 seconds with the default lease.

## Cooperate with the signal

If a handler ignores its `signal`, the underlying work may keep running outside Reflow — the run is still marked `cancelled`, but the in-flight I/O won't stop on its own. Forward the signal to abortable APIs so cancellation takes effect immediately:

```typescript
.step('fetch-profile', async ({ input, signal }) => {
  const response = await fetch(`https://api.example.com/users/${input.userId}`, { signal })
  return await response.json()
})
```

The same `signal` is aborted on lease loss and step timeout, so cooperative handlers get fast, uniform cancellation across all three cases.

## Cancellation is not failure

A cancelled run does **not** trigger [`onFailure`](/guide/failure-handling) or the [`onRunFailed`](/guide/hooks) hook — cancellation is an intentional stop, not an error. If you cancel a run that's paused on stream [backpressure](/guide/streaming), the cancellation still releases it cleanly.
