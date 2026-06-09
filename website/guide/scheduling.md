# Scheduled Workflows

Enqueue a workflow on a recurring interval with `engine.schedule()`:

```typescript
// Enqueue a cleanup workflow every hour
const scheduleId = engine.schedule('cleanup', { olderThanDays: 30 }, 60 * 60 * 1000)

// Stop the schedule
engine.unschedule(scheduleId)
```

`schedule(name, input, intervalMs)` validates the input once and then enqueues a fresh run every `intervalMs`. It returns a `scheduleId` you can pass to `unschedule()`. Calling [`engine.stop()`](/guide/engine) also clears all schedules.

Each tick of a schedule is a normal `enqueue()`, so everything else applies: type-safety on the name and input, durability, retries, hooks. If you want a schedule to be self-deduplicating (only one run in flight at a time), give the enqueued runs an [idempotency key](/guide/engine#idempotency).

::: tip In-process only
Schedules are in-memory timers tied to the engine instance — they don't survive a restart. For cron-like durability across restarts, drive `enqueue()` from your own scheduler (system cron, a job runner) and rely on idempotency keys to avoid duplicates.
:::

Errors thrown while a scheduled enqueue runs are reported to the [`onError` hook](/guide/hooks); without that hook they're swallowed.
