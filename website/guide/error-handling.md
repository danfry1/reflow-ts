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

Every error Reflow raises itself extends `ReflowError` and carries a string `code` — `'WORKFLOW_NOT_FOUND'`, `'VALIDATION'`, `'STEP_TIMEOUT'`, and so on. Branch on `code`: the codes are part of the public API, they survive serialization (a failed run's `error.code`), and unlike `instanceof` they still match when two copies of the package end up installed. Subclasses also carry structured context — no message parsing needed.

```typescript
import { ReflowError } from 'reflow-ts'

try {
  await engine.enqueue('nonexistent', {})
} catch (error) {
  if (error instanceof ReflowError) {
    switch (error.code) {
      case 'WORKFLOW_NOT_FOUND':
        // a registered name was mistyped, or the workflow isn't on this engine
        break
      case 'VALIDATION':
        // the input failed the workflow's schema
        break
    }
  }
}
```

`instanceof` against a subclass works too, and narrows to its properties:

```typescript
import { ValidationError } from 'reflow-ts'

if (error instanceof ValidationError) {
  console.log(error.issues) // [{ message: '…', path: [...] }]
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

One exception: a schema that validates asynchronously is not supported, and `enqueue()`, `schedule()`, and `sendEvent()` reject it with a plain `TypeError`.

## Error classes

| Error | `code` | Thrown when | Structured properties |
|---|---|---|---|
| `ReflowError` | — | Base class for all errors | `code` |
| `ConfigError` | `CONFIG` | Invalid engine, workflow, retry, schedule, or enqueue config | — |
| `WorkflowNotFoundError` | `WORKFLOW_NOT_FOUND` | `enqueue()` / `schedule()` / `resume()` with a workflow this engine doesn't have | `workflowName` |
| `RunNotFoundError` | `RUN_NOT_FOUND` | `waitForRun()` with an unknown run id | `runId` |
| `DuplicateWorkflowError` | `DUPLICATE_WORKFLOW` | Same workflow registered twice | `workflowName` |
| `DuplicateStepError` | `DUPLICATE_STEP` | A step, parallel group, sleep, or wait reuses a name | `workflowName`, `stepName` |
| `ParallelCompleteError` | `PARALLEL_COMPLETE` | `complete()` called inside a parallel branch | `stepName` |
| `ValidationError` | `VALIDATION` | Input or an event payload fails schema validation | `issues` |
| `IdempotencyConflictError` | `IDEMPOTENCY_CONFLICT` | Same idempotency key with different input | `workflowName`, `idempotencyKey` |
| `SerializationError` | `SERIALIZATION` | Step output contains non-JSON data (NaN, functions, …) | `path` |
| `StepTimeoutError` | `STEP_TIMEOUT` | Step exceeds its timeout | `timeoutMs` |
| `WaitTimeoutError` | `WAIT_TIMEOUT` | A chained `.waitForEvent()` times out | `eventName`, `timeoutMs` |
| `StepFailedError` | `STEP_FAILED` | A function workflow's `step.run` fails every attempt, or a step's attempts end without an error of its own | `stepName`, `attempts` |
| `HookError` | `HOOK` | A hook, stream delivery, or `onFailure` handler throws (reported to `onError`) | `source` |
| `StorageError` | `STORAGE` | A storage operation fails; the driver's error is on `cause` | `operation` |
| `ThrownValueError` | `THROWN_VALUE` | A step throws something that isn't an `Error` (`throw 'boom'`) | `value` |
| `TestRunIncompleteError` | `TEST_RUN_INCOMPLETE` | [`testEngine().run()`](/guide/testing) on a workflow that sleeps or waits | `runId`, `status` |
| `RunCancelledError` | `RUN_CANCELLED` | Run cancelled via `engine.cancel()` | `runId` |
| `LeaseExpiredError` | `LEASE_EXPIRED` | Worker lost its lease on a run | `runId` |
| `InternalError` | `INTERNAL` | A bug in Reflow — please report it | — |

`RunCancelledError` and `LeaseExpiredError` are what a step's `signal` aborts with. They, and the internal `RUN_SUSPENDED` error a function workflow is suspended with, are run-control errors: recognise them with `isRunControlError(error)`. `RunControlError` and `RunSuspendedError` are exported as types only.

See the [Errors API reference](/api/errors) for the full hierarchy.
