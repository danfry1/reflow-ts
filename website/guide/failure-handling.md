# Failure Handling

When a step fails after exhausting its retries, the run is marked `failed` and execution stops. Attach an `onFailure` handler to run compensation logic — the saga pattern.

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

## The failure context

`onFailure` receives:

| Field | Type | Description |
|---|---|---|
| `error` | `Error` | The error that caused the failure |
| `stepName` | `string` | The step that failed |
| `input` | `TInput` | The original validated workflow input |

Use `stepName` to decide which compensations to run — it tells you how far the run progressed before failing. For [parallel groups](/guide/parallel), `stepName` is the branch that actually caused the failure, not a sibling aborted by the failure propagation.

## `onFailure` is best-effort

The handler runs **after** the run has already been marked `failed`. An error thrown inside `onFailure` is swallowed — it cannot un-fail the run or crash the engine. Make compensation logic idempotent and defensive.

## Relationship to hooks

`onFailure` is workflow-scoped compensation. For engine-wide observability of failures across all workflows, use the [`onRunFailed` hook](/guide/hooks) or the [result stream](/guide/streaming). The two are complementary: `onFailure` rolls back, hooks observe.

See [Error Handling](/guide/error-handling) for the full picture of recover / retry / compensate.
