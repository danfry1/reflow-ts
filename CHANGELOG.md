# Changelog

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
