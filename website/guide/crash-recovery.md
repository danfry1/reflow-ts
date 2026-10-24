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

## What runs once, and what may run again

- **A finished step never runs again.** Once its result is saved, every later attempt at the run — after a crash, on another process — reads the saved result instead.
- **A step that was running when the process died runs again.** Reflow cannot know whether the work got done before the crash, only that its result was never saved, so it makes the attempt again. Steps run *at least once*.

So a step whose effect must not happen twice — charging a card, sending an email — needs a way to recognise the repeat. Every step gets `idempotencyKey`, a key that is the same each time that step runs for that run, across retries, crashes, and processes, and different for every other step and run. Pass it to any API that accepts one:

```typescript
.step('charge', async ({ input, idempotencyKey }) => {
  const charge = await stripe.paymentIntents.create(
    { amount: input.amount, currency: 'usd', customer: input.customerId, confirm: true },
    { idempotencyKey },
  )
  return { chargeId: charge.id }
})
```

The provider sees the same key on the repeat and returns the first charge rather than making a second. For your own database, the same idea: write with the key as a unique column, or upsert, so a repeat changes nothing. Where an API takes no key, check before acting — look the order up before shipping it.

Steps also get `runId`, and `attempt` — which attempt at this step this is, from 1. For a step in sequence on a started engine, the count is saved with each retry, so it carries on after a crash: an attempt cut off by one ended without a result, and is made again under the same number. A [parallel branch](/guide/parallel), or a step on an engine driven only by `tick()`, waits out its retries in memory, so after a crash its count starts again from 1. Use `idempotencyKey`, not `attempt`, to recognise a repeat.

[Parallel branches](/guide/parallel) follow the same rule, and recovery is per-branch: already-completed branches are skipped on resume.

## Deploying while runs are in flight

A run picked up after a deploy carries on under the new code, matching saved results to steps by name. Renaming a step makes it run again; see [changing workflows](/guide/changing-workflows) for what is safe to change in place and how to version the rest.

## Graceful shutdown

`engine.stop()` aborts in-flight steps (via their `signal`) and leaves their runs `running` so another instance — or the same one on restart — can reclaim them. It does not mark them failed.
