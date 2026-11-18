# Errors

Every error Reflow raises itself extends `ReflowError`, so one `instanceof ReflowError` check catches them all, and carries a stable literal `code` (the `ReflowErrorCode` union). The exception is an asynchronous schema: validating with one throws a plain `TypeError`. Subclasses carry structured context in typed fields — no message parsing needed. See [Error Handling](/guide/error-handling) for usage patterns.

```typescript
import { ReflowError, WorkflowNotFoundError, ValidationError, StepTimeoutError } from 'reflow-ts'
```

**Prefer `code` over `instanceof`.** `ReflowErrorCode` is a closed union, so a `switch` over it can be checked for exhaustiveness, and it keeps working across bundling, duplicate copies of the package in a dependency tree, and realm boundaries (worker threads, vm contexts) — all cases where `instanceof` silently fails.

```typescript
if (error instanceof ReflowError) {
  switch (error.code) {
    case 'STEP_TIMEOUT':   return retryLater(error.timeoutMs)
    case 'WAIT_TIMEOUT':   return escalate(error.eventName)
    case 'VALIDATION':     return badRequest(error.issues)
    default:               return report(error)
  }
}
```

| Error | `code` | Thrown when | Properties |
|---|---|---|---|
| `ReflowError` | — | Base class for all Reflow errors | `code` |
| `ConfigError` | `CONFIG` | Invalid configuration or arguments: engine, workflow (`concurrency`, `maxOperations`, cache `ttl`, wait timeout), retry, schedule, stream, `enqueue` options, `sendEvent` to an undeclared event, `listRuns` limit, `prune` statuses, storage options or a database schema newer than this build. Also the mistakes a [function workflow](/api/create-workflow#createworkflow-config-handler) makes at run time | — |
| `WorkflowNotFoundError` | `WORKFLOW_NOT_FOUND` | `enqueue()` / `schedule()` with an unknown name, or `resume()` of a run whose workflow is not registered | `workflowName` |
| `RunNotFoundError` | `RUN_NOT_FOUND` | `waitForRun()` with an unknown run id | `runId` |
| `DuplicateWorkflowError` | `DUPLICATE_WORKFLOW` | Same workflow name registered twice in one engine | `workflowName` |
| `DuplicateStepError` | `DUPLICATE_STEP` | `.step()` / `.parallel()` / `.sleep()` / `.waitForEvent()` reuses a name, or two operations of a function workflow's run share one | `workflowName`, `stepName` |
| `ParallelCompleteError` | `PARALLEL_COMPLETE` | `complete()` called inside a parallel branch | `stepName` |
| `ValidationError` | `VALIDATION` | Input fails schema validation | `issues` |
| `IdempotencyConflictError` | `IDEMPOTENCY_CONFLICT` | Same idempotency key with different input | `workflowName`, `idempotencyKey` |
| `SerializationError` | `SERIALIZATION` | A step output / input contains non-persistable data | `path` |
| `StepTimeoutError` | `STEP_TIMEOUT` | A step attempt exceeds `timeoutMs` | `timeoutMs` |
| `WaitTimeoutError` | `WAIT_TIMEOUT` | A `waitForEvent` step's `timeoutMs` elapses before the event arrives | `eventName`, `timeoutMs` |
| `RunCancelledError` | `RUN_CANCELLED` | A run is cancelled via `engine.cancel()` | `runId` |
| `LeaseExpiredError` | `LEASE_EXPIRED` | A worker loses its lease on a run | `runId` |
| `RunSuspendedError` | `RUN_SUSPENDED` | Inside the engine, when a [workflow written as a function](/guide/functions) is suspended to sleep or wait. The function never sees it: its pending operations are left unsettled. Type-only export | `runId` |
| `StorageError` | `STORAGE` | A storage backend operation failed; the driver's error is on `cause` | `operation`, `cause` |
| `StepFailedError` | `STEP_FAILED` | A step exhausts its retries with no error of its own (run aborted first). In a [workflow written as a function](/guide/functions#when-a-step-fails), what `step.run()` rejects with once every attempt has failed, the handler's error as its `cause` | `stepName`, `attempts` |
| `HookError` | `HOOK` | A lifecycle hook, stream consumer, or `onFailure` handler threw. Delivered to `onError`, never thrown into a run | `source`, `cause` |
| `ThrownValueError` | `THROWN_VALUE` | User code threw a non-`Error` value (`throw 'boom'`) | `value`, `cause` |
| `TestRunIncompleteError` | `TEST_RUN_INCOMPLETE` | `testEngine.run()` left a run non-terminal (usually a suspending workflow) | `runId`, `status` |
| `InternalError` | `INTERNAL` | An invariant was violated — a bug in reflow-ts | — |

Three more codes belong to classes that are not exported, so a `switch` checked for exhaustiveness must still handle them: `RUN_CONTROL` (the `RunControlError` base, also used when the engine stops), `EARLY_COMPLETE` (how `complete()` ends a run), and `BRANCH_FAILED` (carries a failed parallel branch out of its group). None of them reaches user code as a failure.

`toError(value)` turns anything thrown into an `Error` (a non-`Error` value becomes a `ThrownValueError`), and `assertNever(value, context)` throws an `InternalError` — both exported for code that handles Reflow errors.

## Serializing errors

Every error implements `toJSON()`, so `JSON.stringify(error)` yields the discriminant, the structured context, and a flattened `cause` chain — no custom serializer needed.

```typescript
JSON.stringify(new WaitTimeoutError('approval', 50))
// {
//   "name": "WaitTimeoutError",
//   "code": "WAIT_TIMEOUT",
//   "message": "Timed out after 50ms waiting for event \"approval\"",
//   "context": { "eventName": "approval", "timeoutMs": 50 }
// }
```

Keep `context()` free of secrets in custom subclasses — the output is intended to be logged.

## `ValidationError.issues`

An array of `{ message: string; path?: ... }` describing each schema violation, surfaced directly from your Standard Schema library.

## Control-flow errors

`RunCancelledError` (`RUN_CANCELLED`), `LeaseExpiredError` (`LEASE_EXPIRED`), and `RunSuspendedError` (`RUN_SUSPENDED`) are control-flow signals, not failures. `isRunControlError(error)` is true for all of them. `RunControlError` and `RunSuspendedError` are exported as types only, so recognise them with `isRunControlError()` or by `code`, not `instanceof`. A [workflow written as a function](/guide/functions) never sees them: its pending operations are left unsettled instead. They do **not** reach `onRunFailed` or `onFailure`, and they leave a run reclaimable rather than marking it `failed`. `StepTimeoutError`, by contrast, is a real failure and reaches the failure paths.
