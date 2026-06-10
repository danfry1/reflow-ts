# Error Handling

There are three ways to handle errors, each for a different purpose. Most workflows combine all three.

## 1. Recover inside the step

Use `try/catch` when you can handle the error and continue:

```typescript
.step('fetch', async ({ input }) => {
  try {
    return await callAPI(input.url)
  } catch {
    return { data: null, failed: true }
  }
})
```

## 2. Retry transient failures

Let the error throw and configure [retry](/guide/retry):

```typescript
.step('charge', {
  retry: { maxAttempts: 3, backoff: 'exponential' },
  handler: async ({ input }) => await stripe.charge(input.amount),
})
```

## 3. Compensate with `onFailure`

Roll back after the run has failed — the saga pattern. See [Failure Handling](/guide/failure-handling):

```typescript
.onFailure(async ({ error, stepName, input }) => {
  if (stepName === 'charge') await refundAccount(input.userId)
})
```

## The error hierarchy

Every error Reflow throws extends `ReflowError`, so a single `instanceof` check catches them all. Subclasses carry structured context — no message parsing needed.

```typescript
import { ReflowError, WorkflowNotFoundError, ValidationError } from 'reflow-ts'

try {
  await engine.enqueue('nonexistent', {})
} catch (error) {
  if (error instanceof WorkflowNotFoundError) {
    console.log(error.workflowName) // 'nonexistent'
  }
  if (error instanceof ValidationError) {
    console.log(error.issues) // [{ message: '…', path: [...] }]
  }
  if (error instanceof ReflowError) {
    // catch-all for any Reflow error
  }
}
```

In hooks you can branch on the failure type:

```typescript
import { StepTimeoutError } from 'reflow-ts'

hooks: {
  onRunFailed: ({ error }) => {
    if (error instanceof StepTimeoutError) {
      console.log(`Timed out after ${error.timeoutMs}ms`)
    }
  },
}
```

## Error classes

| Error | Thrown when | Structured properties |
|---|---|---|
| `ReflowError` | Base class for all errors | — |
| `ConfigError` | Invalid engine, retry, or schedule config | — |
| `WorkflowNotFoundError` | `enqueue()` / `schedule()` with an unknown name | `workflowName` |
| `DuplicateWorkflowError` | Same workflow registered twice | `workflowName` |
| `DuplicateStepError` | `.step()` / `.parallel()` reuses a name | `workflowName`, `stepName` |
| `ParallelCompleteError` | `complete()` called inside a parallel branch | `stepName` |
| `ValidationError` | Input fails schema validation | `issues` |
| `IdempotencyConflictError` | Same idempotency key with different input | `workflowName`, `idempotencyKey` |
| `SerializationError` | Step output contains non-JSON data (NaN, functions, …) | `path` |
| `StepTimeoutError` | Step exceeds `timeoutMs` | `timeoutMs` |
| `RunCancelledError` | Run cancelled via `engine.cancel()` | `runId` |
| `LeaseExpiredError` | Worker lost its lease on a run | `runId` |

See the [Errors API reference](/api/errors) for the full hierarchy.
