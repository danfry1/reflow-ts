# Engine methods

Methods on the [`Engine`](/api/create-engine) returned by `createEngine`.

## `engine.start(pollIntervalMs?)`

Initializes storage and starts the polling loop, running `tick()` every `pollIntervalMs` (default `1000`). Call once at startup. Returns `Promise<void>`.

## `engine.stop()`

Stops the polling loop, ends all open [streams](/api/events), aborts in-flight steps, and waits for any in-flight tick to finish. In-flight runs are left `running` so they can be reclaimed. Registered [schedules](/guide/scheduling) are deliberately left in place — they belong to the storage, not to this instance. Returns `Promise<void>`.

## `engine.tick()`

Claims up to `concurrency` pending or stale runs and executes them in parallel. Useful for CLI tools and tests. If you use `tick()` without `start()`, call `storage.initialize()` first. Returns `Promise<void>`.

## `engine.enqueue(name, input, options?)`

Submits a run. Type-safe — only accepts registered workflow names with matching input. Returns the created `WorkflowRun` (use `run.id` to track it).

| Option | Type | Description |
|---|---|---|
| `idempotencyKey` | `string` | Same key + same input returns the existing run; same key + different input throws [`IdempotencyConflictError`](/api/errors) |

Throws [`WorkflowNotFoundError`](/api/errors) for an unknown name and [`ValidationError`](/api/errors) if input fails the schema.

## `engine.stream(options?)`

Returns a pull-based [`ResultStream`](/api/events) — an `AsyncIterableIterator<EngineEvent>` (and `AsyncDisposable`) of execution events. Each call returns an independent stream.

| Option | Type | Default | Description |
|---|---|---|---|
| `bufferSize` | `number` | `Infinity` | Max events buffered before the engine pauses (backpressure). `0` = strict rendezvous; any non-negative integer allows that many buffered events. |

Invalid `bufferSize` (negative, or a non-integer that isn't `Infinity`) throws [`ConfigError`](/api/errors). Breaking out of the loop, `await using`, or `engine.stop()` unsubscribes automatically. See [Streaming Results](/guide/streaming).

## `engine.cancel(runId)`

Cancels a pending or running workflow. Returns `true` if cancelled, `false` if it already completed / failed / cancelled. Aborts the current step's `AbortSignal` immediately. See [Cancellation](/guide/cancellation).

## `engine.sendEvent(runId, eventName, payload)`

Delivers an external event to a run that is (or will be) waiting on [`waitForEvent(eventName)`](/api/workflow). The `payload` — validated against that wait's `schema`, if any — becomes the wait's result and the next step's `prev`.

```typescript
await engine.sendEvent(run.id, 'approved', { approver: 'alice' })
```

Delivery is durable and order-independent: an event sent before the run reaches the wait is buffered and consumed when it gets there. Returns `false` if the run does not exist or has already finished (completed / failed / cancelled); throws [`ConfigError`](/api/errors) if the workflow has no such event step, or [`ValidationError`](/api/errors) if the payload fails the wait's schema. See [Waiting for Events](/guide/wait-for-event).

## `engine.schedule(name, input, intervalMs, options?)`

Registers a durable recurring schedule. Returns `Promise<string>` resolving to its key. Validates `name` and `input` immediately.

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
