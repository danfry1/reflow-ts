# Hooks

Lifecycle hooks give you push-based observability over execution — timing, metrics, logging, alerting. Pass them to `createEngine`.

```typescript
const engine = createEngine({
  storage,
  workflows: [orderWorkflow],
  hooks: {
    onRunStart: ({ runId, workflow }) => {},
    onStepStart: ({ runId, workflow, stepName }) => {},
    onStepComplete: ({ runId, workflow, stepName, output, attempts }) => {
      console.log(`${stepName} completed in ${attempts} attempt(s)`)
    },
    onRunComplete: ({ runId, workflow, output }) => {
      // output is the workflow's final result
      metrics.increment('workflow.completed', { workflow })
    },
    onRunFailed: ({ runId, workflow, stepName, error }) => {
      alerting.notify(`${workflow} failed at ${stepName}: ${error.message}`)
    },
    onError: (error) => {
      // background failures (scheduled enqueues, poll cycles)
      console.error('Engine error:', error)
    },
  },
})
```

## Events

Every hook receives an event carrying `runId` and `workflow`. The shapes mirror the [`EngineEvent`](/api/events) union:

| Hook | Fires | Extra fields |
|---|---|---|
| `onRunStart` | A run begins executing (also on crash-recovery resume) | — |
| `onStepStart` | Before each step runs | `stepName` |
| `onStepComplete` | After a step's result is persisted | `stepName`, `output`, `attempts` |
| `onRunComplete` | A run finishes successfully | `output` (the run's final result) |
| `onRunFailed` | A run fails | `stepName`, `error` |
| `onError` | A background operation fails (scheduled enqueue, poll cycle) | _(receives the `Error` directly)_ |

## Hooks may be async

A hook may be synchronous or `async` — an async hook is **awaited** before the engine proceeds. Use that to flush a metric, persist an audit row, or apply ordering guarantees:

```typescript
hooks: {
  onStepComplete: async (event) => {
    await auditLog.write(event) // the next step waits for this to finish
  },
}
```

## Hooks never affect engine state

A hook that throws — or an async hook that rejects — can never fail a workflow or crash the engine. The error is swallowed. Likewise, the `output` and `error` your hooks receive are **defensive clones**: mutating them does not affect engine state, the persisted result, or what other observers see.

Without an `onError` hook, background errors (from scheduled enqueues or the poll loop) are silently swallowed — set it if you want visibility.

## Pull-based alternative

Hooks are fire-and-forget callbacks. When you'd rather **pull** results — to apply backpressure or feed a producer/consumer loop — use [`engine.stream()`](/guide/streaming) instead. Both can run at once; they observe the same events.
