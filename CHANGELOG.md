# Changelog

## Unreleased

### Added

- **Durable sleep** — `.sleep(name, duration)` pauses a workflow between steps for a duration that survives process exit, deploy, or crash. The run is persisted as `sleeping` with its lease released (so the process need not stay alive), and any engine instance reclaims and resumes it once the time elapses. `duration` is a number of milliseconds or a unit-suffixed string (`'500ms'`, `'30s'`, `'24h'`, `'7d'`). `prev` passes through unchanged, and the sleep is observable as a step (`sleeping` → `completed`).
- **Wait for events** — `.waitForEvent(name, { schema?, timeoutMs? })` durably suspends a workflow until `engine.sendEvent(runId, name, payload)` delivers a matching event (e.g. a webhook, an approval, a human action). The run is persisted as `waiting` with its lease released and resumes — on any engine instance — when the event arrives; the payload (validated against `schema`, if given) becomes the next step's `prev`. Delivery is durable and order-independent: an event sent before the run reaches the wait is buffered and consumed when it gets there. With `timeoutMs`, the run fails with the new `WaitTimeoutError` if no event arrives in time. New exported error: `WaitTimeoutError`.
- **Conditional steps** — `.step(name, { when, handler })` accepts a `when` predicate receiving `{ input, prev, steps }`. When it returns `false` the step is skipped: `prev` passes through unchanged, the step is recorded with the new `skipped` status, and a `stepSkipped` event fires (with a matching `onStepSkipped` hook). The decision is evaluated once and persisted, so it is never recomputed on crash-recovery replay; a throwing predicate fails the run at that step. `when` may be synchronous or `async`, and is supported on sequential `.step()` only — passing it to a `.parallel()` branch throws `ConfigError`. The builder types follow the skip: after a conditional step `prev` widens to a union with the passed-through value, and the step's entry in `steps` becomes optional, so neither can be read as though the step definitely ran. New exported types: `ConditionalStepConfig`, `StepCondition`, `StepConditionContext`. ([#21](https://github.com/danfry1/reflow-ts/issues/21))

- **Error codes** — every `ReflowError` now carries a stable literal `code` (`'STEP_TIMEOUT'`, `'LEASE_EXPIRED'`, …) alongside its class. Branching on `code` is a closed union, so a `switch` over it can be checked for exhaustiveness, and unlike `instanceof` it survives bundling, duplicate copies of the package, and cross-realm boundaries. Errors also implement `toJSON()`, emitting the discriminant, their structured context, and a flattened `cause` chain, so they can be logged without a custom serializer. New exported types: `ReflowErrorCode`, `SerializedReflowError`. New exported errors: `StepFailedError`, `HookError`, `ThrownValueError`, `TestRunIncompleteError`, `InternalError`. New exported helpers: `toError`, `assertNever`.

### Fixed

- **`engine.schedule()` no longer multiplies across engine instances.** Scheduled ticks were plain `setInterval` callbacks calling `enqueue()` with no idempotency key, so a schedule registered on N engines sharing one storage produced N runs per interval — silently, in exactly the multi-instance deployment the concurrency guide recommends. Ticks are now aligned to wall-clock slots of `intervalMs` and enqueued with an idempotency key derived from the schedule's identity and slot number, so every instance computes the same key for the same moment and storage keeps one run. The identity defaults to a hash of the workflow name, interval, and canonical input; `schedule()` takes a new optional fourth argument, `{ key }`, to set it explicitly. Timers remain in-memory and still do not survive a restart — only the deduplication is durable. New exported type: `ScheduleOptions`.

  The scheduling guide previously suggested giving scheduled runs an idempotency key to make them self-deduplicating, which `schedule()` had no parameter to do. That advice is now the built-in behaviour.

### Changed

- Scheduled runs fire on wall-clock boundaries of `intervalMs` rather than at a fixed offset from the `schedule()` call. This is what lets independent instances agree on which tick is which; it also removes `setInterval` drift accumulation.
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
