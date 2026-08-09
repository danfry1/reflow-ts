# Changelog

## Unreleased

### Added

- **Durable sleep** — `.sleep(name, duration)` pauses a workflow between steps for a duration that survives process exit, deploy, or crash. The run is persisted as `sleeping` with its lease released (so the process need not stay alive), and any engine instance reclaims and resumes it once the time elapses. `duration` is a number of milliseconds or a unit-suffixed string (`'500ms'`, `'30s'`, `'24h'`, `'7d'`). `prev` passes through unchanged, and the sleep is observable as a step (`sleeping` → `completed`).
- **Wait for events** — `.waitForEvent(name, { schema?, timeoutMs? })` durably suspends a workflow until `engine.sendEvent(runId, name, payload)` delivers a matching event (e.g. a webhook, an approval, a human action). The run is persisted as `waiting` with its lease released and resumes — on any engine instance — when the event arrives; the payload (validated against `schema`, if given) becomes the next step's `prev`. Delivery is durable and order-independent: an event sent before the run reaches the wait is buffered and consumed when it gets there. With `timeoutMs`, the run fails with the new `WaitTimeoutError` if no event arrives in time. New exported error: `WaitTimeoutError`.
- **Conditional steps** — `.step(name, { when, handler })` accepts a `when` predicate receiving `{ input, prev, steps }`. When it returns `false` the step is skipped: `prev` passes through unchanged, the step is recorded with the new `skipped` status, and a `stepSkipped` event fires (with a matching `onStepSkipped` hook). The decision is evaluated once and persisted, so it is never recomputed on crash-recovery replay; a throwing predicate fails the run at that step. `when` may be synchronous or `async`, and is supported on sequential `.step()` only — passing it to a `.parallel()` branch throws `ConfigError`. The builder types follow the skip: after a conditional step `prev` widens to a union with the passed-through value, and the step's entry in `steps` becomes optional, so neither can be read as though the step definitely ran. New exported types: `ConditionalStepConfig`, `StepCondition`, `StepConditionContext`. ([#21](https://github.com/danfry1/reflow-ts/issues/21))
- **Storage errors are translated at the boundary** — `better-sqlite3`, `bun:sqlite`, and `node:sqlite` report the same condition (a busy database, a failed write, a closed connection) with different error types and different properties carrying the code, and those shapes leaked straight into user code. Storage failures now surface as a `StorageError` carrying the failing `operation` and the driver's error as `cause`, so callers catch one type instead of branching per driver. The driver's message is folded into the `StorageError` message rather than left only on `cause`, since a failed step persists its error as a plain string and a cause chain does not survive that. Adapters that already throw a `ReflowError` pass through untouched, so custom adapters are never double-wrapped. New exported error: `StorageError`.
- **Error codes** — every `ReflowError` now carries a stable literal `code` (`'STEP_TIMEOUT'`, `'LEASE_EXPIRED'`, …) alongside its class. Branching on `code` is a closed union, so a `switch` over it can be checked for exhaustiveness, and unlike `instanceof` it survives bundling, duplicate copies of the package, and cross-realm boundaries. Errors also implement `toJSON()`, emitting the discriminant, their structured context, and a flattened `cause` chain, so they can be logged without a custom serializer. New exported types: `ReflowErrorCode`, `SerializedReflowError`. New exported errors: `StepFailedError`, `HookError`, `ThrownValueError`, `TestRunIncompleteError`, `InternalError`. New exported helpers: `toError`, `assertNever`.

### Fixed

- **Schedules are now durable, and no longer multiply across engine instances.** `engine.schedule()` registered an in-process `setInterval` that called `enqueue()` with no idempotency key. Two consequences, both silent: schedules vanished on restart or deploy, with nothing to re-register them; and because engine instances share storage — the deployment the concurrency guide recommends — a schedule registered on N workers produced N runs per interval. For a library whose premise is durable execution, an in-memory scheduler was the wrong default.

  Schedules are now stored. `engine.schedule()` upserts a durable record keyed by the schedule's identity and returns that key; firing happens during the engine's normal tick, claiming each due occurrence atomically and advancing its next occurrence in the same transaction, so exactly one instance enqueues it. A worker only claims schedules for workflows it has registered, so a schedule is never swallowed by an instance that cannot serve it. Re-registering preserves the existing cadence unless the interval changed, which makes calling `schedule()` unconditionally at startup on every instance the intended usage. Occurrences missed while the fleet was down are skipped rather than backfilled.

### Changed

- **Breaking:** `engine.schedule()` and `engine.unschedule()` are now `async`, since both perform storage I/O. `schedule()` resolves to the schedule's key (previously a random in-process id) and takes an optional fourth argument `{ key }` to set that identity explicitly; `unschedule()` takes the key and resolves to whether a schedule was removed. New `engine.listSchedules()` returns the registered schedules with their next firing times. New exported types: `ScheduleOptions`, `WorkflowSchedule`.
- **Breaking:** `engine.stop()` no longer clears schedules. They belong to the storage rather than to one instance, so stopping a worker must not cancel a schedule the rest of the fleet is still serving. Use `unschedule()` to remove one.
- **Breaking:** `StorageAdapter` gains `upsertSchedule`, `claimDueSchedule`, `deleteSchedule`, and `listSchedules`; the SQLite adapters add a `workflow_schedules` table, created automatically on `initialize()` with no migration needed for existing databases. Custom adapters must implement the new methods — `claimDueSchedule` in particular must claim and advance atomically, since that is what makes a firing exclusive.
- **Observer failures are reported instead of swallowed.** A lifecycle hook, stream consumer, or `onFailure` handler that throws was previously discarded by an empty `catch`, making a broken hook invisible. Those failures are now wrapped in a `HookError` (with the original throw preserved as `cause`) and delivered to the `onError` hook. Observers still cannot affect a run's outcome — only their visibility changes. Engines with no `onError` hook behave exactly as before.
- Non-`Error` throws (`throw 'boom'`) are wrapped in `ThrownValueError` rather than a bare `Error`. The message is unchanged — it is still the thrown value's string form — and the original value is now retained on both `cause` and `.value`, so a thrown object is no longer flattened beyond recovery.
- `testEngine.run()` throws `TestRunIncompleteError` (was a bare `Error`) when a run has not reached a terminal state after its single tick, and the message now names the usual cause: a workflow that suspends on `.sleep()` or `.waitForEvent()` needs a full `createEngine` to drive it.
- `StorageAdapter` gains `sleepRun(runId, leaseId, wakeAt)`, `waitRun(runId, leaseId, eventName, wakeAt)`, `deliverEvent(runId, eventName, payload)`, and `takeEvent(runId, eventName)`; `claimNextRun` now also reclaims `sleeping`/`waiting` runs whose wake time has passed. All are implemented across the built-in adapters; the SQLite adapters add a `wake_at` column (with an automatic, backward-compatible migration) and a `workflow_events` table. Custom adapters must implement the new methods.
- `RunStatus` gains `sleeping` and `waiting`; `StepStatus` gains `sleeping`, `waiting`, and `skipped`.
- `EngineEvent` / `EngineHooks` gain the additive `stepSkipped` / `onStepSkipped` variant.
- Run execution is split into one executor per execution-unit kind behind a shared `UnitExecutor` contract, replacing the single ~390-line branch-per-kind loop in the engine. Internal reorganisation with no public API change, but it fixes a step row-identity bug: because `saveStepResult` upserts by row `id` and the step path generated a fresh one on every write (unlike the sleep, wait, and parallel paths), a step re-executed after a reclaim appended a second row under the same name, surfacing as a duplicated step in `getRunStatus()`.
- The `node:sqlite` adapter now opens transactions with `BEGIN IMMEDIATE` so concurrent claims under WAL take the write lock up front instead of risking a non-retryable `SQLITE_BUSY_SNAPSHOT`.

### Fixed

- **`reflow-ts/sqlite-bun` change detection** — `bun:sqlite` reports affected-row counts on the `run()` result, not on `Database.changes` (which is `undefined`). The adapter read the latter, so `heartbeatRun`, `updateRunStatus`, `updateClaimedRunStatus`, and the `claimNextRun` double-claim guard never reflected real row counts — heartbeats always reported the lease lost and the claim guard never engaged. The adapter now reads the count from the statement result. Adds a Bun-native smoke test (`bun run test:bun`) wired into CI so the Bun adapter — which the Node-based Vitest suite cannot load — has real coverage.

## 0.5.0 — 2026-06-10

### Added

- **`engine.stream()`** — a pull-based, backpressure-aware stream of execution events. Returns an `AsyncIterableIterator<EngineEvent>` (also an `AsyncDisposable`) so you can `for await` over `runStart` / `stepStart` / `stepComplete` / `runComplete` / `runFailed` events instead of wiring up callback-to-queue plumbing. Optional `bufferSize` paces the engine against a slow consumer; breaking out of the loop, `await using`, or `engine.stop()` unsubscribes automatically. New exported types: `EngineEvent`, `EngineEventOf`, `StreamOptions`, `ResultStream`. ([#24](https://github.com/danfry1/reflow-ts/issues/24) — suggested by [@brianjenkins94](https://github.com/brianjenkins94))
- **Async hooks** — lifecycle hooks may now be `async` and are awaited before the engine proceeds, so a hook can flush a metric, persist an audit row, or apply backpressure with ordering guarantees. A throwing or rejecting hook is still fully contained and never affects engine state. ([#23](https://github.com/danfry1/reflow-ts/issues/23) — suggested by [@brianjenkins94](https://github.com/brianjenkins94))
- **`reflow-ts/sqlite-node-builtin`** — a new SQLite storage adapter for Node.js backed by the built-in `node:sqlite` module, with **zero native dependencies** (the Node equivalent of the Bun adapter). Requires Node ≥ 22.5; the existing `better-sqlite3`-based `reflow-ts/sqlite-node` adapter remains for Node ≥ 18.18.

### Changed

- Hook event objects are now aligned with `EngineEvent`: every event carries a `type` discriminator and the owning `workflow` name, `onStepStart` / `onStepComplete` now include `workflow`, and `onRunComplete` now includes the workflow's final `output`. These are additive — existing hook callbacks continue to type-check and run unchanged.
- The minimum supported Node.js version is now 18.18, the first Node 18 release with `Symbol.asyncDispose` support.

## 0.4.0

### Added

- **`.parallel({ ... })`** — concurrent step groups. Run independent steps at the same time and receive a typed merged record as `prev` in the next step. Each branch supports per-branch retry and `timeoutMs`. Fail-fast on first branch failure (siblings receive `signal.abort()`) and per-branch crash recovery — completed branches are skipped on resume, so side effects do not fire twice. `onRunFailed` and `onFailure` report the branch that actually caused the failure, not a sibling aborted by propagation. ([#20](https://github.com/danfry1/reflow-ts/issues/20) — suggested by [@brianjenkins94](https://github.com/brianjenkins94))

### Changed

- `Workflow` exposes a new `executionUnits` array (replacing the internal `steps` list) so the engine can distinguish sequential steps from parallel groups. Sequential `.step()` workflows remain fully type- and behavior-compatible.
- New `ParallelCompleteError` is exported and thrown when `complete()` is called inside a parallel branch (early completion is only meaningful in sequential context).

## 0.3.0

### Added

- **`onRunStart` / `onStepStart` hooks** — new lifecycle hooks for observability and timing; `onRunStart` fires when a run begins executing, `onStepStart` fires before each step runs ([#12](https://github.com/danfry1/reflow-ts/issues/12) — suggested by [@brianjenkins94](https://github.com/brianjenkins94))

## 0.2.0 — 2026-03-20

### Added

- **Date support** — `Date` is now a valid `PersistedValue` type, automatically serialized and deserialized through storage
- **`complete(value?)`** — step handlers can finish a workflow early, skipping remaining steps and optionally persisting a final value as the step result
- **Typed `steps` context** — each step handler receives a `steps` object with typed access to all previously completed step results by name, removing the need to forward data through `prev` across intermediate steps

### Changed

- `PersistedPrimitive` now includes `Date`
- `StepContext` has two new fields: `complete` and `steps`
- `StepStatus` has a new value: `completed-early` (used internally for crash-safe early completion)
- `executeStep` internals refactored to return a discriminated union instead of using exceptions for control flow

## 0.1.0 — 2026-03-11

Initial release.

- Durable workflow execution with per-step checkpointing to SQLite
- Typed step chaining with `prev` flowing between steps
- Per-step retry with linear and exponential backoff
- Cooperative cancellation via AbortSignal
- Idempotent enqueue with `idempotencyKey`
- Crash recovery with lease-based reclamation
- Configurable concurrency for parallel run execution
- `onFailure` handler for compensation logic (saga pattern)
- Step-level timeouts
- Recurring workflow scheduling
- Lifecycle hooks (`onStepComplete`, `onRunComplete`, `onRunFailed`)
- Standard Schema support (Zod, Valibot, ArkType, or any compatible library)
- `testEngine` helper with in-memory storage and typed step results
- SQLite storage adapter with WAL mode and transactional claiming
