# Changelog

## 0.3.0

### Added

- **`.parallel({ ... })`** — concurrent step groups. Run independent steps at the same time and receive a typed merged record as `prev` in the next step. Each branch supports per-branch retry and `timeoutMs`. Fail-fast on first branch failure (siblings receive `signal.abort()`) and per-branch crash recovery — completed branches are skipped on resume, so side effects do not fire twice. `onRunFailed` and `onFailure` report the branch that actually caused the failure, not a sibling aborted by propagation. ([#20](https://github.com/danfry1/reflow-ts/issues/20) — suggested by [@brianjenkins94](https://github.com/brianjenkins94))
- **`onRunStart` / `onStepStart` hooks** — new lifecycle hooks for observability and timing; `onRunStart` fires when a run begins executing, `onStepStart` fires before each step runs ([#12](https://github.com/danfry1/reflow-ts/issues/12) — suggested by [@brianjenkins94](https://github.com/brianjenkins94))

### Changed

- `Workflow` exposes a new `executionUnits` array (replacing the internal `steps` list) so the engine can distinguish sequential steps from parallel groups. Sequential `.step()` workflows remain fully type- and behavior-compatible.
- New `ParallelCompleteError` is exported and thrown when `complete()` is called inside a parallel branch (early completion is only meaningful in sequential context).

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
