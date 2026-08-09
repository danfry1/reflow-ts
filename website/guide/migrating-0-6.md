# Migrating to 0.6

Most of 0.6 is additive — `.sleep()`, `.waitForEvent()`, conditional steps, `listRuns()`, `resume()`, and error codes all arrive without touching existing code. Three things break.

If you use the built-in storage adapters and don't call `engine.schedule()`, you can upgrade without changes.

## 1. `schedule()` and `unschedule()` are async

Schedules are now stored rather than held as in-process timers, so registering one performs I/O.

```typescript
// 0.5
const id = engine.schedule('cleanup', { olderThanDays: 30 }, 60 * 60 * 1000)
engine.unschedule(id)

// 0.6
const key = await engine.schedule('cleanup', { olderThanDays: 30 }, 60 * 60 * 1000)
await engine.unschedule(key)
```

The returned value changed meaning: it was a random per-process id, and is now the schedule's **durable key**, stable across restarts and shared between instances. If you persisted the old id anywhere, it no longer identifies anything — re-register and store the returned key instead, or pass your own with `{ key: 'nightly-cleanup' }`.

Registering the same key again updates that schedule in place and keeps its cadence, so calling `schedule()` unconditionally at startup on every instance is now the intended usage:

```typescript
await engine.start()
await engine.schedule('cleanup', { olderThanDays: 30 }, 60 * 60 * 1000)
```

## 2. `stop()` no longer clears schedules

Schedules belong to the storage, not to the instance that registered them. Stopping one worker must not cancel a schedule the rest of the fleet is still serving.

```typescript
// 0.5 — stop() cancelled every schedule this engine had registered
await engine.stop()

// 0.6 — the schedule survives; remove it explicitly if that is what you meant
await engine.unschedule(key)
await engine.stop()
```

If you relied on `stop()` to clean up in tests, either call `unschedule()` or give each test its own storage.

## 3. Custom `StorageAdapter`s need ten new methods

Only relevant if you implement `StorageAdapter` yourself. The built-in adapters already have these, and the SQLite ones create their new tables and columns automatically on `initialize()` — **existing databases need no migration**.

| Method | Backs |
|---|---|
| `sleepRun` | [`.sleep()`](/guide/sleep) |
| `waitRun`, `deliverEvent`, `takeEvent` | [`.waitForEvent()`](/guide/wait-for-event) |
| `upsertSchedule`, `claimDueSchedule`, `deleteSchedule`, `listSchedules` | [durable schedules](/guide/scheduling) |
| `listRuns`, `requeueRun` | `engine.listRuns()` / `engine.resume()` |

Two carry the durability guarantees and are worth reading the contract for:

- **`claimDueSchedule`** must claim a due schedule *and* advance its next occurrence in one transaction. That atomicity is the only thing stopping N instances firing the same occurrence N times.
- **`waitRun`** must, in the same transaction, leave the run reclaimable if a matching event is already buffered — otherwise a run can strand until its timeout when an event arrives at the wrong moment.

Rather than working from prose, run the contract:

```typescript
import { storageConformanceCases } from 'reflow-ts/conformance'

for (const testCase of storageConformanceCases) {
  it(testCase.name, async () => {
    const storage = new MyStorage(/* ... */)
    await storage.initialize()
    try {
      await testCase.run(storage)
    } finally {
      storage.close()
    }
  })
}
```

See [Storage](/guide/storage#custom-adapters).

## Worth knowing, but not breaking

**New status values.** `RunStatus` gains `sleeping` and `waiting`; `StepStatus` gains `sleeping`, `waiting`, and `skipped`. Additive, but an exhaustive `switch` over either — with an `assertNever` default or under `noFallthroughCasesInSwitch` — will now fail to compile until the new cases are handled. That is the intended behaviour: those states are observable through `getRunStatus()`, and code that renders run state should account for them.

**A throwing hook is now visible.** A lifecycle hook, stream consumer, or `onFailure` handler that threw used to be silently discarded. Those failures are now wrapped in `HookError` and delivered to the [`onError` hook](/guide/hooks). Nothing about a run's outcome changes — observers still cannot affect it — but if you have an `onError` hook and a hook that has quietly been throwing, you will start seeing it. That is the point.

**Errors carry a `code`.** Every `ReflowError` now has a stable literal discriminant, and branching on it is preferred over `instanceof`, which breaks across bundling, duplicate copies of the package, and realm boundaries. Existing `instanceof` checks keep working. See [Error Handling](/guide/error-handling).
