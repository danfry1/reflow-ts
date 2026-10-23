# Migrating to 0.8

0.8 makes runs inspectable — every run records when it started and finished, what it returned, and why it failed — and fixes running several processes against one SQLite database.

**If you use the built-in storage adapters, you can upgrade without changing any code.** The SQLite adapters migrate their tables on `initialize()`. Everything on `engine` is additive — though code that implements the `Engine` interface itself, such as a hand-written mock, gains `waitForRun`, `countRuns`, and `prune` to provide.

A few behaviours change in ways your code could notice:

- **Retry backoff suspends the run on a started engine.** Between attempts the run is `sleeping` (with `wakeAt` set to the next attempt) and the step is `retrying`, instead of the run staying `running`, and `onRunStart` fires again when it resumes. A crash mid-backoff no longer restarts the step's attempts. An engine driven only by `tick()` — and so `testEngine` — still waits out the backoff in process, as do parallel branches. A 0.7 worker that picks up a backing-off run, mid rolling deploy or after a rollback, starts that step's attempts over.
- **`StepStatus` gains `'retrying'`.** An exhaustive `switch` over step statuses needs a case for it.
- **A started engine keeps every slot busy** and starts work it enqueues at once, so runs can begin sooner than the next poll, and more of them run concurrently than before when some are slow.
- **`tick()` fills free slots** rather than skipping when another tick is in flight. On an engine that is not otherwise running — the usual way to drive one in tests — it behaves as before.

Custom storage adapters have work to do; the rest of this page is for them.

## Databases migrate themselves

`initialize()` adds the new columns and records the schema version in SQLite's `user_version`, so later upgrades apply exactly the migrations a database is missing. It is automatic, idempotent, and keeps every existing row.

Runs that finished before the upgrade get a `finishedAt` (their last update), so `prune()` can delete them. Their `startedAt`, `output`, and `error`, and their steps' attempt `history`, were never recorded and stay empty.

During a rolling deploy, workers still on 0.7 keep finishing runs after the database has been upgraded, and they do not record `finishedAt`. Those runs are never pruned. Finish the rollout before relying on `prune()`, or backfill once it is done:

```sql
UPDATE workflow_runs SET finished_at = updated_at
WHERE finished_at IS NULL AND status IN ('completed', 'failed', 'cancelled');
```

A database written by a **newer** version of Reflow is refused with a `ConfigError` rather than opened, since older code could mishandle columns it does not know. Downgrading therefore needs a database from before the upgrade.

## Custom storage adapters

### Status setters are replaced by intent

`updateRunStatus(runId, status)` and `updateClaimedRunStatus(runId, leaseId, status)` are gone. A generic setter allowed a run to be marked `completed` without an output, and made `cancel()` a read followed by a write that could race a run completing. Each transition now has its own method:

| Removed | Replacement |
|---|---|
| `updateClaimedRunStatus(id, lease, 'completed')` | `completeRun(id, lease, output)` — records `output` and `finishedAt` |
| `updateClaimedRunStatus(id, lease, 'failed')` | `failRun(id, lease, error)` — records `error` and `finishedAt` |
| `updateRunStatus(id, 'cancelled')` | `cancelRun(id)` — must refuse a run that already finished, **atomically** |

### New methods

- `countRuns(filter?)` — runs per status, every status present. Backs `engine.countRuns()`.
- `deleteRuns({ finishedBefore, statuses, limit })` — delete finished runs, oldest-finished first, with their steps and events. Backs `engine.prune()`.
- Optional: `getStepResultsForRuns(runIds)` and `runActivity(filter)`, which the dashboard uses when present to read less. See [Custom adapters](/guide/storage#custom-adapters).

### New fields

`WorkflowRun` gains `startedAt`, `finishedAt`, `output`, `error`, `wakeAt`, and `waitingFor`; `StepResult` gains `startedAt` and `history`, the step's attempts as an array to store whole and return in order. Store what the engine passes in and return it unchanged, plus:

- `claimNextRun` sets `startedAt` on a run's **first** claim only, and clears `wakeAt` and `waitingFor`.
- `waitRun` records the event name as `waitingFor`.
- `requeueRun` clears `finishedAt`, `output`, and `error`, keeping `startedAt`.
- `output` distinguishes a completed run that returned `null` from a run with no output (`undefined`).

### Claim order

`claimNextRun` must claim the run that has been claimable longest: a pending run since it became pending (its `updatedAt`), a sleeping or waiting run since its wake time, a stale run since its lease expired. An adapter that claims every pending run before any woken or stale one lets a busy queue starve those runs indefinitely.

### Check it

Every rule above is a case in the conformance suite:

```typescript
import { storageConformanceCases } from 'reflow-ts/conformance'
```
