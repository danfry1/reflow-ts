# Scheduled Workflows

Register a recurring schedule with `engine.schedule()`:

```typescript
// Enqueue a cleanup workflow every hour
const key = await engine.schedule('cleanup', { olderThanDays: 30 }, 60 * 60 * 1000)

// Stop it
await engine.unschedule(key)
```

`schedule(name, input, recurrence, options?)` validates the input, stores the schedule, and returns its key. Each firing is a normal `enqueue()`, so everything else applies: type-safety on the name and input, durability, retries, hooks.

## Intervals and cron

The recurrence is either a fixed gap or a cron expression:

```typescript
await engine.schedule('cleanup', input, 60 * 60 * 1000)   // every hour, in ms
await engine.schedule('cleanup', input, { every: '1h' })  // same, as a duration
await engine.schedule('report',  input, { cron: '0 9 * * 1-5' })  // 09:00, weekdays
```

`{ every }` takes milliseconds or a unit-suffixed string (`'500ms'`, `'30s'`, `'24h'`, `'7d'`) — the same form [`.sleep()`](/guide/sleep) accepts. A bare number is milliseconds.

`{ cron }` takes the standard five fields — `minute hour day-of-month month day-of-week` — with wildcards, ranges, lists, a `/step` suffix, three-letter month and day names, and the `@hourly` / `@daily` / `@weekly` / `@monthly` / `@yearly` aliases:

| Expression | Fires |
|---|---|
| `*/15 * * * *` | every 15 minutes |
| `0 * * * *` | hourly, on the hour |
| `0 9 * * 1-5` | 09:00, Monday to Friday |
| `30 2 1 * *` | 02:30 on the 1st of each month |
| `0 0 * * SUN` | midnight on Sundays |
| `@daily` | midnight |

A malformed expression is rejected by `schedule()` rather than on the first tick — registration is the last point a bad cadence can reach you, since after that the schedule fires unattended. So is an expression that can never occur, like `0 0 30 2 *` (the 30th of February).

Cron inherits one quirk worth knowing: when **both** day-of-month and day-of-week are narrowed, they are OR'd, not AND'd. `0 0 13 * 5` means "the 13th, and also every Friday" — not "Friday the 13th".

::: warning Cron expressions are evaluated in UTC
There is no time-zone option. Interpreting cron in a local zone means deciding what a schedule means during a DST gap (a wall-clock time that doesn't occur) and a DST overlap (one that occurs twice), and guessing there produces a scheduler that silently skips or doubles a run twice a year. UTC has no such ambiguity.

If you need a local-time schedule, convert the intended local time to UTC yourself and accept that it shifts by an hour across DST — or use an interval, which is unaffected.
:::

You can check an expression, or preview its occurrences, without registering anything:

```typescript
import { parseCron, nextCronOccurrence } from 'reflow-ts'

const cron = parseCron('0 9 * * 1-5')       // throws ConfigError if malformed
new Date(nextCronOccurrence(cron, Date.now()))
```

## Schedules are stored, not held in memory

The schedule lives in your storage, not in the process that registered it. That means it **survives a restart or a deploy** — any engine instance running that workflow picks up the next firing, even if the process that registered it is long gone.

Registering the same key again updates that schedule in place rather than creating a second one, so the intended usage is to call it unconditionally at startup on every instance:

```typescript
// Safe to run on every boot, on every worker.
await engine.start()
await engine.schedule('cleanup', { olderThanDays: 30 }, 60 * 60 * 1000)
```

Re-registering **preserves the existing cadence** unless the recurrence itself changed. Without that, a service that redeploys more often than its schedule fires would reset the clock every time and the schedule would never run.

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

The same applies to cron: a `0 9 * * *` schedule that was down for three days fires once on return, at the next 09:00, not three times.

::: tip At most once per occurrence
A firing is claimed and advanced before the run is enqueued, so a process that dies in the moment between the two skips that occurrence rather than repeating it. Schedules are a trigger, not a ledger: if a period must never be missed, record the work itself durably and let the schedule drive a reconciliation step.
:::

Errors thrown while a scheduled enqueue runs are reported to the [`onError` hook](/guide/hooks) and do not stop the other due schedules from firing; without that hook they're swallowed.
