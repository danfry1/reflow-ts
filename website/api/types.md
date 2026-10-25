# Types

All public types are exported from the package root (`reflow-ts`).

## Persisted data

```typescript
type PersistedPrimitive = string | number | boolean | null | undefined | Date
type PersistedValue = PersistedPrimitive | PersistedValue[] | { [key: string]: PersistedValue }
```

Everything stored as workflow input or step output must be a `PersistedValue`. See [Storage](/guide/storage#persistable-values).

## Runs and steps

```typescript
type RunStatus = 'pending' | 'running' | 'sleeping' | 'waiting' | 'completed' | 'failed' | 'cancelled'
type TerminalRunStatus = 'completed' | 'failed' | 'cancelled'
type StepStatus =
  | 'pending' | 'running' | 'completed' | 'completed-early'
  | 'skipped' | 'sleeping' | 'waiting' | 'retrying' | 'failed'
```

`RUN_STATUSES` and `STEP_STATUSES` are exported as readonly arrays of every value, for building filters or validating input.

| Type | Shape |
|---|---|
| `WorkflowRun` | `{ id, workflow, input, idempotencyKey, status, createdAt, updatedAt, startedAt, finishedAt, output, error, wakeAt, waitingFor }` — see [Inspecting runs](/guide/inspecting-runs) |
| `RunError` | `{ stepName, message, code }` — why a `failed` run failed; `code` is the error's string code, or `null` |
| `ClaimedRun` | `WorkflowRun & { leaseId }` |
| `StepResult` | `{ id, runId, name, status, output, error, attempts, createdAt, updatedAt, startedAt, history }` — `history` is a `StepAttempt[]` |
| `StepAttempt` | `{ startedAt, finishedAt, error }` — one attempt at a step's handler; `error` is `null` for the one that succeeded |
| `RunCounts` | `Record<RunStatus, number>` — returned by `countRuns()` |
| `PruneOptions` | `{ olderThan, statuses? }` — what `engine.prune()` deletes |
| `RunInfo` | `{ run: WorkflowRun; steps: StepResult[] }` — returned by `getRunStatus()` |
| `CreateRunResult` | `{ run: WorkflowRun; created: boolean }` — returned by `storage.createRun()` |
| `WorkflowSchedule` | `{ key, workflow, input, recurrence, nextRunAt, createdAt, updatedAt }` — a registered [schedule](/guide/scheduling) |
| `ScheduleRecurrence` | `{ kind: 'interval'; intervalMs }` \| `{ kind: 'cron'; expression }` — how a schedule repeats |
| `ScheduleSpec` | What `engine.schedule()` accepts: `number` \| `{ every }` \| `{ cron }` |
| `RetryConfig` | `{ maxAttempts, backoff, initialDelayMs?, timeoutMs? }` |

## Workflow & engine types

| Type | Purpose |
|---|---|
| `Workflow<TName, TInput, TPrev, TSteps>` | The workflow builder type |
| `AnyWorkflow` | A workflow with any parameters (for collections) |
| `StepContext` | The object passed to step handlers |
| `StepConfig` | The config-object form of `.step()` |
| `StepDefinition` | Internal step representation exposed on the workflow |
| `ParallelBranch` / `InferBranchOutput` | Branch handler type and its output inference |
| `ExecutionUnit` | A sequential step or a parallel group |
| `FailureContext` | The object passed to `onFailure` |
| `Engine` / `EngineConfig` / `EngineHooks` | Engine surface |
| `EngineEvent` / `EngineEventOf` / `StreamOptions` / `ResultStream` | [Events & streams](/api/events) |
| `EnqueueOptions` | `{ idempotencyKey?, delay?, startAt? }` |
| `StorageAdapter` | [Custom storage contract](/api/storage) |

## Inference helpers

For building typed wrappers around Reflow:

```typescript
type InferInput<W>        // extract a workflow's input type
type InferSteps<W>        // extract a workflow's accumulated steps map
type WorkflowInputMap<T>  // { [name]: inputType } across a workflow tuple
type WorkflowStepsMap<T>  // { [name]: stepsRecord } across a workflow tuple
```

`WorkflowInputMap` is what powers `enqueue()`'s type-safety — it maps each registered workflow name to its required input shape.
