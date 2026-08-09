# Changelog

## Unreleased

### Added

- **Durable sleep** — `.sleep(name, duration)` pauses a workflow between steps for a duration that survives process exit, deploy, or crash. The run is persisted as `sleeping` with its lease released (so the process need not stay alive), and any engine instance reclaims and resumes it once the time elapses. `duration` is a number of milliseconds or a unit-suffixed string (`'500ms'`, `'30s'`, `'24h'`, `'7d'`). `prev` passes through unchanged, and the sleep is observable as a step (`sleeping` → `completed`).
- **Wait for events** — `.waitForEvent(name, { schema?, timeoutMs? })` durably suspends a workflow until `engine.sendEvent(runId, name, payload)` delivers a matching event (e.g. a webhook, an approval, a human action). The run is persisted as `waiting` with its lease released and resumes — on any engine instance — when the event arrives; the payload (validated against `schema`, if given) becomes the next step's `prev`. Delivery is durable and order-independent: an event sent before the run reaches the wait is buffered and consumed when it gets there. With `timeoutMs`, the run fails with the new `WaitTimeoutError` if no event arrives in time. New exported error: `WaitTimeoutError`.

### Changed

- `StorageAdapter` gains `sleepRun(runId, leaseId, wakeAt)`, `waitRun(runId, leaseId, eventName, wakeAt)`, `deliverEvent(runId, eventName, payload)`, and `takeEvent(runId, eventName)`; `claimNextRun` now also reclaims `sleeping`/`waiting` runs whose wake time has passed. All are implemented across the built-in adapters; the SQLite adapters add a `wake_at` column (with an automatic, backward-compatible migration) and a `workflow_events` table. Custom adapters must implement the new methods.
- `RunStatus` gains `sleeping` and `waiting`; `StepStatus` gains `sleeping` and `waiting`.
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
