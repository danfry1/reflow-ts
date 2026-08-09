# Scheduled Workflows

Register a recurring schedule with `engine.schedule()`:

```typescript
// Enqueue a cleanup workflow every hour
const key = await engine.schedule('cleanup', { olderThanDays: 30 }, 60 * 60 * 1000)

// Stop it
await engine.unschedule(key)
```

`schedule(name, input, intervalMs, options?)` validates the input, stores the schedule, and returns its key. Each firing is a normal `enqueue()`, so everything else applies: type-safety on the name and input, durability, retries, hooks.

## Schedules are stored, not held in memory

The schedule lives in your storage, not in the process that registered it. That means it **survives a restart or a deploy** — any engine instance running that workflow picks up the next firing, even if the process that registered it is long gone.

Registering the same key again updates that schedule in place rather than creating a second one, so the intended usage is to call it unconditionally at startup on every instance:

```typescript
// Safe to run on every boot, on every worker.
await engine.start()
await engine.schedule('cleanup', { olderThanDays: 30 }, 60 * 60 * 1000)
```

Re-registering **preserves the existing cadence** unless the interval itself changed. Without that, a service that redeploys more often than its schedule fires would reset the clock every time and the schedule would never run.

## Running on more than one engine

Because [multiple engine instances](/guide/concurrency) share one storage, several of them will be polling the same schedules. Each due firing is claimed atomically — the claim and the advance of the next occurrence happen in one transaction — so exactly one instance enqueues it:

```typescript
// Registered identically on all three workers. Still one run per hour.
await engine.schedule('cleanup', { olderThanDays: 30 }, 60 * 60 * 1000)
```

A worker only claims schedules for workflows it has registered, so in a fleet where different workers run different workflows, a schedule is never swallowed by an instance that cannot serve it.

## Identity

The key defaults to a hash of the workflow name, the interval, and the canonical form of the input, so instances agree without configuration. Set `options.key` when you want to control it:

```typescript
// Keep one identity while the input changes
await engine.schedule('cleanup', { olderThanDays: days }, hourly, { key: 'nightly-cleanup' })

// Or deliberately split one workflow into two independent schedules
await engine.schedule('sync', { region: 'eu' }, hourly, { key: 'sync-eu' })
await engine.schedule('sync', { region: 'us' }, hourly, { key: 'sync-us' })
```

Because schedules are shared, `unschedule(key)` removes it for the whole fleet, and `engine.stop()` deliberately leaves schedules registered — stopping one worker must not cancel a schedule the others are still serving.

Inspect what is registered with `listSchedules()`:

```typescript
for (const schedule of await engine.listSchedules()) {
  console.log(schedule.key, new Date(schedule.nextRunAt))
}
```

## Missed occurrences are skipped, not backfilled

If every instance is down for three hours on an hourly schedule, it fires **once** when the fleet returns and then resumes its normal cadence. Backfilling three runs at once turns an outage into a thundering herd, which is almost never what a recurring job wants.

If you need every missed period processed, make that explicit in the workflow — pass a window and have the first step work out what it still owes — rather than relying on the scheduler to replay.

::: tip At most once per occurrence
A firing is claimed and advanced before the run is enqueued, so a process that dies in the moment between the two skips that occurrence rather than repeating it. Schedules are a trigger, not a ledger: if a period must never be missed, record the work itself durably and let the schedule drive a reconciliation step.
:::

Errors thrown while a scheduled enqueue runs are reported to the [`onError` hook](/guide/hooks) and do not stop the other due schedules from firing; without that hook they're swallowed.
