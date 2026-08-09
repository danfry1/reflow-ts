# Scheduled Workflows

Enqueue a workflow on a recurring interval with `engine.schedule()`:

```typescript
// Enqueue a cleanup workflow every hour
const scheduleId = engine.schedule('cleanup', { olderThanDays: 30 }, 60 * 60 * 1000)

// Stop the schedule
engine.unschedule(scheduleId)
```

`schedule(name, input, intervalMs, options?)` validates the input once and then enqueues a fresh run every `intervalMs`. It returns a `scheduleId` you can pass to `unschedule()`. Calling [`engine.stop()`](/guide/engine) also clears all schedules.

Each tick of a schedule is a normal `enqueue()`, so everything else applies: type-safety on the name and input, durability, retries, hooks.

## Running on more than one engine

Because [multiple engine instances](/guide/concurrency) can share one storage, the obvious risk is that a schedule registered on every instance fires on every instance — three workers, three runs an hour.

It doesn't. Ticks are aligned to **wall-clock slots** of `intervalMs` rather than to elapsed time since the call, and each enqueue carries an idempotency key derived from the schedule's identity and its slot number. Every instance independently computes the same key for the same moment, so the first one to reach storage creates the run and the rest resolve to it:

```typescript
// Registered identically on all three workers — still one run per hour.
engine.schedule('cleanup', { olderThanDays: 30 }, 60 * 60 * 1000)
```

The identity defaults to a hash of the workflow name, the interval, and the canonical form of the input, so instances agree without configuration. Set `options.key` when you want to control it:

```typescript
// Keep one identity while the input changes
engine.schedule('cleanup', { olderThanDays: days }, hourly, { key: 'nightly-cleanup' })

// Or deliberately split one workflow into two independent schedules
engine.schedule('sync', { region: 'eu' }, hourly, { key: 'sync-eu' })
engine.schedule('sync', { region: 'us' }, hourly, { key: 'sync-us' })
```

Deduplication relies on instances agreeing which slot "now" falls in, so it assumes their clocks are roughly in sync. Skew well under `intervalMs` is harmless; skew approaching it can let two instances land in adjacent slots and produce two runs.

::: warning Schedules are in-memory
Only the deduplication is durable. The timers themselves live in the engine instance and do not survive a restart — nothing re-registers them, and intervals missed while the process was down are not backfilled. For schedules that must outlive the process, drive `enqueue()` from a durable scheduler (system cron, a job runner) and pass your own `idempotencyKey` to get the same deduplication.
:::

Errors thrown while a scheduled enqueue runs are reported to the [`onError` hook](/guide/hooks); without that hook they're swallowed.
