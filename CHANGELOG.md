# Changelog

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
