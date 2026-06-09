# Crash Recovery

Crash recovery is the core durability guarantee. Reflow resumes workflows from the last completed step — if your process dies after step 2 of 5, a later engine instance reclaims the run and continues at step 3. Completed steps are never re-executed.

```typescript
// Process crashes here after 'charge' completed but before 'fulfill'.
// On restart, the engine claims the run and skips 'charge'.
await engine.start()
```

## How it works

1. **Per-step checkpointing.** Each step's output is persisted before the next step runs. Completed steps are reused, not re-run.
2. **Leases.** Claiming a run takes a lease valid for `runLeaseDurationMs`. While executing, the engine [heartbeats](/guide/engine#leases-and-heartbeats) the lease.
3. **Stale reclamation.** If a worker crashes, it stops heartbeating. Once the lease expires, any engine's next `tick()` can reclaim the still-`running` run and resume it.

This is why a stopped or crashed engine leaves in-flight runs in the `running` state rather than failing them — that state is what makes them reclaimable.

## Make steps idempotent

A step may run more than once across recoveries — for example, if the process dies *after* a side effect but *before* its result is persisted. Design steps so a repeat is safe:

- Use idempotency keys on external calls (Stripe, your own APIs).
- Prefer upserts over blind inserts.
- Check-then-act against external state when you can.

[Parallel branches](/guide/parallel) follow the same rule, and recovery is per-branch: already-completed branches are skipped on resume.

## Graceful shutdown

`engine.stop()` aborts in-flight steps (via their `signal`) and leaves their runs `running` so another instance — or the same one on restart — can reclaim them. It does not mark them failed.
