# Types

Core types are exported from the package root (`reflow-ts`). Types belonging to one entry point come from it: `SQLiteStorageOptions` from the SQLite adapters, `PostgresStorageOptions` from `reflow-ts/postgres`, `StepOutput` / `SkippedStepOutput` / `RunResult` / `TestEngine` from `reflow-ts/test` ([testEngine](/api/storage#testengine-config)), `ConformanceCase` from `reflow-ts/conformance`, and the [dashboard](/api/dashboard) types from `reflow-ts/dashboard`.

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

`RUN_STATUSES` and `STEP_STATUSES` are exported as readonly arrays of every value, for building filters or validating input, as is `STEP_KINDS` (`['step', 'sleep', 'wait']`, typed `StepKind`): what kind of operation a step row records.

| Type | Shape |
|---|---|
| `WorkflowRun` | `{ id, workflow, input, idempotencyKey, status, createdAt, updatedAt, startedAt, finishedAt, output, error, wakeAt, waitingFor }` — see [Inspecting runs](/guide/inspecting-runs) |
| `RunError` | `{ stepName, message, code }` — why a `failed` run failed; `code` is the error's string code, or `null` |
| `ClaimedRun` | `WorkflowRun & { leaseId }` |
| `StepResult` | `{ id, runId, name, status, output, error, attempts, createdAt, updatedAt, startedAt, history, kind? }` — `history` is a `StepAttempt[]`; `kind` is `'step' \| 'sleep' \| 'wait'`, or `null` for rows saved before 0.11 |
| `StepAttempt` | `{ startedAt, finishedAt, error }` — one attempt at a step's handler; `error` is `null` for the one that succeeded |
| `RunCounts` | `Record<RunStatus, number>` — returned by `countRuns()` |
| `PruneOptions` | `{ olderThan, statuses? }` — what `engine.prune()` deletes |
| `RunInfo` | `{ run: WorkflowRun; steps: StepResult[] }` — returned by `getRunStatus()` |
| `CreateRunResult` | `{ run: WorkflowRun; created: boolean }` — returned by `storage.createRun()` |
| `WorkflowSchedule` | `{ key, workflow, input, recurrence, nextRunAt, createdAt, updatedAt }` — a registered [schedule](/guide/scheduling) |
| `ScheduleRecurrence` | `{ kind: 'interval'; intervalMs }` \| `{ kind: 'cron'; expression }` — how a schedule repeats |
| `ScheduleSpec` | What `engine.schedule()` accepts: `number` \| `{ every }` \| `{ cron }` |
| `RetryConfig` | `{ maxAttempts, backoff, initialDelayMs?, timeoutMs? }` — `maxAttempts` and `backoff` are required |
| `ListRunsFilter` / `CountRunsFilter` / `DeleteRunsFilter` | What `listRuns()`, `countRuns()`, and `storage.deleteRuns()` take |
| `RunActivity` / `RunActivityFilter` | What `storage.runActivity()` returns and takes |
| `CachedStep` | `{ key, workflow, stepName, output, createdAt, expiresAt }` — a [cached step result](/guide/caching) |
| `ConsumeEventResult` | What `storage.consumeEvent()` returns |
| `RequeueOptions` | `{ keepFailed? }` — what `storage.requeueRun()` takes |
| `PersistedObject` | `{ [key: string]: PersistedValue }` |

## Workflow & engine types

| Type | Purpose |
|---|---|
| `Workflow<TName, TInput, TPrev, TSteps>` | The workflow builder type (`kind: 'chain'`) |
| `ChainWorkflow` | Any chained workflow, whatever its name, input, and steps |
| `FunctionWorkflow` | A [workflow written as a function](/api/create-workflow#createworkflow-config-handler) (`kind: 'function'`) — beta |
| `AnyWorkflow` | `ChainWorkflow \| FunctionWorkflow`, for collections. Narrow on `kind` — or use `ChainWorkflow` — before reading `executionUnits` |
| `WorkflowDefinition` | What both kinds share: `kind`, `name`, `inputSchema`, `failureHandler`, `label`, `concurrency`, `parseInput()` |
| `WorkflowConfig` / `FunctionWorkflowConfig` | What `createWorkflow` takes, without and with a handler |
| `WorkflowContext` / `WorkflowSteps` / `StepRunContext` / `StepHandler` | A function workflow's `{ input, step, runId }`, its `step` operations, and a `step.run` handler and its context — beta |
| `FunctionStepConfig` / `FunctionRetryConfig` / `FunctionStepCache` | The config form of `step.run` — beta |
| `WaitForEventOptions` / `WaitForEventResult` | `step.waitForEvent`'s options and what it resolves to — beta |
| `Persistable<T>` / `Saveable<T>` | Whether `T` is data that can be saved, interfaces such as an SDK's results included: `Persistable` is `true` or `false`; `Saveable` is `T`, or a type naming why not, so an unsaveable step result is an error where it is returned — beta |
| `StepContext` | The object passed to step handlers |
| `StepConfig` | The config-object form of `.step()` |
| `StepCache` | A step's `cache` option |
| `ConditionalStepConfig` / `StepCondition` / `StepConditionContext` | A step with `when`, the predicate, and what it receives |
| `StepDefinition` | Internal step representation exposed on the workflow |
| `ParallelBranch` / `InferBranchOutput` | Branch handler type and its output inference |
| `ExecutionUnit` | One unit of a chained workflow: a step, a parallel group, a durable sleep, or a wait for an event (`kind: 'step' \| 'parallel' \| 'sleep' \| 'waitForEvent'`) |
| `FailureContext` | The object passed to `onFailure` |
| `Engine` / `EngineConfig` / `EngineHooks` | Engine surface |
| `EngineEvent` / `EngineEventOf` / `StreamOptions` / `ResultStream` | [Events & streams](/api/events) |
| `EnqueueOptions` | `{ idempotencyKey?, delay?, startAt? }` |
| `WaitForRunOptions` | `{ signal?, pollIntervalMs? }` — see [`waitForRun()`](/api/engine#engine-waitforrun-runid-options) |
| `ScheduleSpec` / `ScheduleOptions` | What `engine.schedule()` takes |
| `CronExpression` | A parsed cron expression, from `parseCron()` |
| `ReflowErrorCode` / `SerializedReflowError` / `ValidationIssue` | [Error](/api/errors) codes, `toJSON()` output, and a validation issue |
| `StorageAdapter` | [Custom storage contract](/api/storage) |

## Inference helpers

For building typed wrappers around Reflow:

```typescript
type InferInput<W>        // extract a workflow's input type
type InferSteps<W>        // extract a workflow's accumulated steps map
type WorkflowInputMap<T>  // { [name]: inputType } across a workflow tuple
type WorkflowStepsMap<T>  // { [name]: stepsRecord } across a workflow tuple
type InferOutput<W>       // a function workflow's output (beta); PersistedValue for a chained one
type InferEvents<W>       // a function workflow's { [event]: payloadType } (beta)
type WorkflowOutputMap<T> // { [name]: outputType } across a workflow tuple (beta)
```

`WorkflowInputMap` is what powers `enqueue()`'s type-safety — it maps each registered workflow name to its required input shape.

## Cron helpers

```typescript
import { parseCron, nextCronOccurrence } from 'reflow-ts'

const cron = parseCron('0 9 * * 1-5')            // throws ConfigError if malformed
nextCronOccurrence(cron, Date.now())             // next matching minute after that time, in UTC (epoch ms)
```

`parseCron` checks syntax only; an expression that can never match, such as `'0 0 30 2 *'`, throws [`ConfigError`](/api/errors) from `nextCronOccurrence` (and so from `engine.schedule()`). See [Scheduled Workflows](/guide/scheduling).
