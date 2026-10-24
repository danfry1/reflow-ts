# Engine methods

Methods on the [`Engine`](/api/create-engine) returned by `createEngine`.

## `engine.start(pollIntervalMs?)`

Initializes storage and starts the engine, which keeps up to `concurrency` runs executing and refills each slot as soon as its run finishes. Work this instance enqueues, resumes, or sends an event to starts at once; every `pollIntervalMs` (default `1000`) it also checks storage for work from other processes, elapsed sleeps, due schedules, and stale runs. Call once at startup. Returns `Promise<void>`.

## `engine.stop()`

Stops the polling loop, ends all open [streams](/api/events), aborts in-flight steps, and waits for in-flight claims and runs to settle. In-flight runs are left `running` so they can be reclaimed. Registered [schedules](/guide/scheduling) are deliberately left in place — they belong to the storage, not to this instance. Returns `Promise<void>`.

## `engine.tick()`

Claims pending or stale runs into the free `concurrency` slots, executes them in parallel, and resolves once they have finished. Useful for CLI tools and tests. If you use `tick()` without `start()`, call `storage.initialize()` first. Returns `Promise<void>`.

## `engine.enqueue(name, input, options?)`

Submits a run. Type-safe — only accepts registered workflow names with matching input. Returns the created `WorkflowRun` (use `run.id` to track it).

| Option | Type | Description |
|---|---|---|
| `idempotencyKey` | `string` | Same key + same input returns the existing run; same key + different input throws [`IdempotencyConflictError`](/api/errors) |
| `delay` | `number \| string` | Start the run this long from now — milliseconds or a duration such as `'10m'`. See [starting later](/guide/engine#starting-a-run-later) |
| `startAt` | `Date \| number` | Start the run at this time. A time already past starts it now. Not with `delay` |

Throws [`WorkflowNotFoundError`](/api/errors) for an unknown name, [`ValidationError`](/api/errors) if input fails the schema, and [`ConfigError`](/api/errors) for an invalid `delay` or `startAt`, or both together.

## `engine.stream(options?)`

Returns a pull-based [`ResultStream`](/api/events) — an `AsyncIterableIterator<EngineEvent>` (and `AsyncDisposable`) of execution events. Each call returns an independent stream.

| Option | Type | Default | Description |
|---|---|---|---|
| `bufferSize` | `number` | `Infinity` | Max events buffered before the engine pauses (backpressure). `0` = strict rendezvous; any non-negative integer allows that many buffered events. |

Invalid `bufferSize` (negative, or a non-integer that isn't `Infinity`) throws [`ConfigError`](/api/errors). Breaking out of the loop, `await using`, or `engine.stop()` unsubscribes automatically. See [Streaming Results](/guide/streaming).

## `engine.cancel(runId)`

Cancels a run that has not finished — pending, running, sleeping, or waiting. Returns `true` if cancelled, `false` if it does not exist or already completed / failed / cancelled; the check and the cancel are one atomic storage write, so a run finishing at that moment is never reported cancelled. Aborts the current step's `AbortSignal` immediately. See [Cancellation](/guide/cancellation).

## `engine.sendEvent(runId, eventName, payload)`

Delivers an external event to a run that is (or will be) waiting on [`waitForEvent(eventName)`](/api/workflow). The `payload` — validated against that wait's `schema`, if any — becomes the wait's result and the next step's `prev`.

```typescript
await engine.sendEvent(run.id, 'approved', { approver: 'alice' })
```

Delivery is durable and order-independent: an event sent before the run reaches the wait is buffered and consumed when it gets there. Returns `false` if the run does not exist or has already finished (completed / failed / cancelled); throws [`ConfigError`](/api/errors) if the workflow has no such event step, or [`ValidationError`](/api/errors) if the payload fails the wait's schema. See [Waiting for Events](/guide/wait-for-event).

## `engine.schedule(name, input, recurrence, options?)`

Registers a durable recurring schedule. Returns `Promise<string>` resolving to its key. Validates `name`, `input`, and the recurrence immediately.

`recurrence` is a number of milliseconds, `{ every }` (milliseconds or a duration string like `'1h'`), or `{ cron }` (a five-field expression, evaluated in **UTC**). A malformed or unsatisfiable cron expression throws `ConfigError` here rather than on the first tick.

The schedule is persisted rather than held as an in-process timer, so it survives restarts; registering the same key again updates it in place, preserving the cadence unless the interval changed. Due firings are claimed atomically, so N instances sharing a schedule produce one run per interval. `options.key` sets the identity explicitly (it otherwise defaults to a hash of the name, interval, and input). See [Scheduled Workflows](/guide/scheduling).

## `engine.unschedule(key)`

Removes a durable schedule. Returns `Promise<boolean>` — `false` if no such schedule existed. Because schedules are shared, this stops it for every instance.

## `engine.listSchedules()`

Returns `Promise<readonly WorkflowSchedule[]>` — every registered schedule, ordered by key, each carrying its `nextRunAt`.

## `engine.getRunStatus(runId)`

Returns `{ run, steps }` — the run's current status and all its step results — or `null` if the run is not found.

```typescript
const info = await engine.getRunStatus(run.id)
info?.run.status // 'pending' | 'running' | 'sleeping' | 'waiting' | 'completed' | 'failed' | 'cancelled'
info?.steps      // StepResult[]
```

## `engine.listRuns(filter?)`

Lists runs ordered by `createdAt` descending, then `id` descending (a total, stable order even when runs share a `createdAt`). For inspection or dead-letter visibility. All filter fields are optional:

```typescript
await engine.listRuns({
  status: 'failed',   // only this RunStatus
  workflow: 'order',  // only this workflow
  limit: 50,          // max rows (default 100; must be a positive integer)
  before: cursor,     // keyset cursor (createdAt), paired with beforeId
  beforeId: cursorId, // keyset cursor tie-break (id)
})
```

Paginate with a keyset cursor — pass the last row's `createdAt` **and** `id`. Using both is exact even when timestamps collide; `before` alone is a coarse "created before T" filter that can drop runs tied on that millisecond:

```typescript
const page1 = await engine.listRuns({ limit: 50 })
const last = page1.at(-1)
const page2 = last
  ? await engine.listRuns({ limit: 50, before: last.createdAt, beforeId: last.id })
  : []
```


## `engine.waitForRun(runId, options?)`

Waits for a run to finish and resolves with it — `completed`, `failed`, or `cancelled`; check `status`, `output`, and `error` to see which. A failed run resolves rather than rejects. Resolves at once if the run has already finished; throws [`RunNotFoundError`](/api/errors) for an unknown run.

```typescript
const { id } = await engine.enqueue('generate-report', { accountId })
const run = await engine.waitForRun(id, { signal: AbortSignal.timeout(30_000) })

if (run.status === 'completed') return run.output
throw new Error(`Report failed at ${run.error?.stepName}: ${run.error?.message}`)
```

A run finishing on this engine resolves the wait as soon as its hooks and `onFailure` have run. One finished by any other engine instance — in another process, or another engine in this one — is noticed on the next read of storage, every `pollIntervalMs` (default `1000`). `signal` stops the wait early, rejecting with its reason — `AbortSignal.timeout(ms)` gives a deadline. The wait reads storage itself, so it works on an engine that was never started, and it outlives `stop()`; bound it with a `signal` if the run might never finish. Waiting does not affect the run in any way.

## `engine.countRuns(filter?)`

Returns how many runs are in each status, optionally for one workflow — every status present, zero or not. Cheap enough to poll for a status bar or a health check.

```typescript
const counts = await engine.countRuns({ workflow: 'order-fulfillment' })
counts.failed // 3
```

## `engine.prune(options)`

Deletes finished runs — with their step results and buffered events — that finished longer ago than `olderThan` (milliseconds, or a duration string like `'30d'`). `statuses` narrows it to some of `completed` / `failed` / `cancelled` (default: all three). Runs that can still progress are never deleted. Resolves to how many runs were removed.

```typescript
await engine.prune({ olderThan: '30d' })
await engine.prune({ olderThan: '7d', statuses: ['completed'] }) // keep failures longer
```

Deletes in batches of 500, so on SQLite it never holds the write lock long enough to stall workers. See [Inspecting runs](/guide/inspecting-runs#retention).
## `engine.resume(runId)`

Re-queues a `failed` or `cancelled` run so the engine picks it up again. Completed steps are preserved and skipped on replay, so execution resumes at the step that failed; the failed step's result is discarded and re-run. Returns `true` if the run was resumable, `false` if it does not exist or is in any other state (`pending` / `running` / `completed`).

```typescript
const failed = await engine.listRuns({ status: 'failed' })
for (const run of failed) {
  await engine.resume(run.id)
}
```
