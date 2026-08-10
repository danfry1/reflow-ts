# Migrating to 0.7

0.7 adds [cron schedules](/guide/scheduling). Everything else is additive, and there is exactly one breaking change.

**If you use the built-in storage adapters and never read `engine.listSchedules()`, you can upgrade without changing anything.** Existing `schedule()` calls keep working — a bare number is still milliseconds.

## `WorkflowSchedule.intervalMs` is now `recurrence`

A schedule can repeat on a fixed interval *or* a cron expression, so the two are modelled as a tagged union rather than two optional fields. That way a schedule cannot be stored with both, or with neither.

```typescript
// 0.6
schedule.intervalMs // 3600000

// 0.7
schedule.recurrence // { kind: 'interval', intervalMs: 3600000 }
                    // or { kind: 'cron', expression: '0 9 * * 1-5' }
```

Reading it:

```typescript
for (const schedule of await engine.listSchedules()) {
  const cadence = schedule.recurrence.kind === 'cron'
    ? schedule.recurrence.expression
    : `every ${schedule.recurrence.intervalMs}ms`

  console.log(schedule.key, cadence, new Date(schedule.nextRunAt))
}
```

### Custom storage adapters

`StorageAdapter`'s method signatures are unchanged — no new methods, nothing removed. What changes is the shape of the `WorkflowSchedule` you store and return.

If you persist the recurrence as two nullable columns the way the built-in adapters do, note that `upsertSchedule` must treat a change of **either** as a change of cadence. A schedule switching from `{ every: '1h' }` to `{ cron: '0 * * * *' }` fires at the same times but is a different recurrence, and must reset `nextRunAt` rather than inherit the old one.

The contract covers this — `reflow-ts/conformance` gained five cron cases, including that a claimed cron schedule advances to its *next occurrence* rather than by a fixed gap:

```typescript
import { storageConformanceCases } from 'reflow-ts/conformance'
```

## Databases migrate themselves

0.6 created `workflow_schedules` with `interval_ms INTEGER NOT NULL`. A cron schedule leaves that column null, and SQLite cannot drop a `NOT NULL` constraint in place — so `initialize()` rebuilds the table when it finds the old shape, carrying existing rows across.

This is automatic and idempotent. **No manual migration, no downtime step, no data loss.** Existing interval schedules keep their key, their cadence, and their next run time.

## Worth knowing

**Cron is evaluated in UTC.** There is no time-zone option. Interpreting cron in a local zone means deciding what a schedule means during a DST gap (a wall-clock time that does not occur) and a DST overlap (one that occurs twice), and guessing there produces a scheduler that silently skips or doubles a run twice a year. If you need a local-time schedule, convert to UTC yourself and accept the hour shift across DST — or use an interval, which is unaffected.

**Bad expressions fail at registration.** `schedule()` rejects a malformed expression, and also one that can never occur (`0 0 30 2 *` — the 30th of February), rather than failing on the first tick. After registration a schedule fires unattended, so that is the last point the error can reach you.

**You can check an expression without registering it:**

```typescript
import { parseCron, nextCronOccurrence } from 'reflow-ts'

const cron = parseCron('0 9 * * 1-5')   // throws ConfigError if malformed
new Date(nextCronOccurrence(cron, Date.now()))
```
