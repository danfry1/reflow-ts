# Errors

Every error Reflow throws extends `ReflowError`, so one `instanceof ReflowError` check catches them all. Subclasses carry structured context — no message parsing needed. See [Error Handling](/guide/error-handling) for usage patterns.

```typescript
import { ReflowError, WorkflowNotFoundError, ValidationError, StepTimeoutError } from 'reflow-ts'
```

| Error | Thrown when | Properties |
|---|---|---|
| `ReflowError` | Base class for all Reflow errors | — |
| `ConfigError` | Invalid engine, retry, schedule, or stream config | — |
| `WorkflowNotFoundError` | `enqueue()` / `schedule()` with an unknown name | `workflowName` |
| `DuplicateWorkflowError` | Same workflow name registered twice in one engine | `workflowName` |
| `DuplicateStepError` | `.step()` / `.parallel()` reuses a name | `workflowName`, `stepName` |
| `ParallelCompleteError` | `complete()` called inside a parallel branch | `stepName` |
| `ValidationError` | Input fails schema validation | `issues` |
| `IdempotencyConflictError` | Same idempotency key with different input | `workflowName`, `idempotencyKey` |
| `SerializationError` | A step output / input contains non-persistable data | `path` |
| `StepTimeoutError` | A step attempt exceeds `timeoutMs` | `timeoutMs` |
| `WaitTimeoutError` | A `waitForEvent` step's `timeoutMs` elapses before the event arrives | `eventName`, `timeoutMs` |
| `RunCancelledError` | A run is cancelled via `engine.cancel()` | `runId` |
| `LeaseExpiredError` | A worker loses its lease on a run | `runId` |

## `ValidationError.issues`

An array of `{ message: string; path?: ... }` describing each schema violation, surfaced directly from your Standard Schema library.

## Control-flow errors

`RunCancelledError` and `LeaseExpiredError` are control-flow signals, not failures — they do **not** reach `onRunFailed` or `onFailure`, and they leave a run reclaimable rather than marking it `failed`. `StepTimeoutError`, by contrast, is a real failure and reaches the failure paths.
