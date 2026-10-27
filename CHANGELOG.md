# Changelog

## Unreleased

Durable functions. A workflow can now be written as one async function, with
`step.run`, `step.sleep`, and `step.waitForEvent` as its durable operations
and plain TypeScript between them: branches, loops over data, steps run
together, failures caught and recovered from. It runs on the same engine and
storage as chained workflows, which are unchanged.

**Upgrading:** nothing to change in workflow code. SQLite databases move to schema version 6 and Postgres schemas to version 3 on `initialize()`, each adding a nullable `kind` column to step rows. Open a database from before with `npx reflow-ts dashboard` and it asks for `--migrate`. On Postgres, a role allowed only to read and write rows cannot run the migration: run `initialize()` once as the schema's owner first. During a rolling deploy, workers still on 0.10 refuse the migrated database once they restart, so finish the rollout rather than rolling back. A custom storage adapter should store and return `StepResult.kind`, which the conformance suite now checks; one that does not still works, without the check it enables.

### Added

- **Workflows written as a function** (beta: the API may still change in a minor release). `createWorkflow(config, async ({ input, step }) => { ... })` defines a workflow as one async function whose durable operations are `step.run()`, `step.sleep()`, `step.sleepUntil()`, and `step.waitForEvent()`. Everything between them is ordinary code. `if` branches. A loop or `Promise.all(items.map(...))` runs a number of steps decided at run time, concurrently when started together. `try`/`catch` around a step recovers from its failure.
  - **Replay.** A run resumes by calling the function again from the top: each operation that already finished returns its saved result without running, matched by name, so a function that branches differently on resume runs new steps instead of reusing others' results. Results come back in the order the operations first finished, so `Promise.race` between operations picks the same winner on every replay.
  - **Failures.** A failed step rejects with `StepFailedError`, its `cause` the handler's error, and a failure the function caught replays as the same rejection, so it takes the same path again; `engine.resume()` keeps those caught failures and reruns only the step that ended the run. Anything that is not the step's own failure — storage failing, an output that cannot be saved, a misconfigured call — fails the run however the function catches it, so a `catch` meant for a declined card cannot take the wrong branch on a database outage.
  - **Events.** Declared on the workflow with their schemas (`events: { shipped: schema }`), which type what `step.waitForEvent` resolves to and validate payloads in `sendEvent`. `step.waitForEvent(name, { timeout })` resolves to `null` when it times out, rather than failing the run. A wait can be named apart from its event (`{ event: 'approved' }`) to wait for the same event more than once, and several waits can be pending at once. An event sent through the engine running a wait wakes it at once; one sent through another engine reaches it within a second, even while other steps are running.
  - **Concurrency.** The run suspends only once every operation in flight is sleeping, waiting, or backing off, so a sleep never cuts off a step still running beside it.
  - **Types.** Step results are typed from their handlers and the run's output from what the function returns (`InferOutput`, and `output` on `testEngine().run()`). A result typed as an interface, as SDK responses are, is accepted; one holding a function, `Map`, `Set`, or promise is a type error at the handler. Durations are written one way throughout: `timeout: '30s'`, `retry: { initialDelay: '1s' }`, `sleep(name, '7d')`.
  - **A limit on operations per run.** A run resumes by replaying every operation before the point it stopped, so one that grows without bound costs more each time; `maxOperations` (default 10,000) fails such a run with `ConfigError` rather than letting it slow down unnoticed.
  - **Names keep their kind.** Step rows record whether they are a step, a sleep, or a wait (`StepResult.kind`), so a deploy that reuses a name for another kind of operation fails the run with `ConfigError` instead of handing it the other's saved result.
  - Retries, timeouts, caching, idempotency keys, hooks, streams, `testEngine`, the dashboard, and every storage adapter work as they do for chained workflows. A [guide](https://reflow.danfry.dev/guide/functions) covers writing them, changing them safely, and how the operations map onto Inngest, Cloudflare Workflows, and Temporal.
- **`isRunControlError(error)`**, and `RunControlError` exported, so workflow code can tell the run being cancelled, losing its lease, or being suspended (`RunSuspendedError`, code `'RUN_SUSPENDED'`) from a failure, and rethrow it.
- **`testEngine().run()` returns the run's `output`.**
- **Storage adapter additions, all optional.** `requeueRun(runId, { keepFailed })` with `supportsKeepFailed` keeps the named failed steps when a run is resumed. `consumeEvent(step, leaseId, eventName)` and `waitRun` with a list of event names, with `supportsEventNames`, let a wait be named apart from its event and several waits be pending at once. The built-in adapters implement both and the conformance suite tests them. A custom adapter without them keeps the previous behaviour: resuming reruns caught failures, and a function workflow allows one pending wait at a time.
- **`ChainWorkflow`**, the type of any chained workflow. `AnyWorkflow` now also includes workflows written as functions, so code that reads `executionUnits` or chains steps on an `AnyWorkflow` narrows on `workflow.kind === 'chain'` first, or uses `ChainWorkflow`.
- **`ReflowErrorCode` gains `'RUN_SUSPENDED'`.** An exhaustive `switch` over the codes needs a case for it.

### Fixed

- **A step that resolves to `undefined` is typed `undefined`, not `never`.** The step after it received `prev: never`, and a step returning `T | undefined` lost the `undefined`, in `.step()` and `.parallel()` branches alike. Both now keep the type the handler returns, so code after such a step may now be asked by the compiler to handle `undefined` it was told could not happen. `InferSteps` of something that is not a workflow is now `Record<string, PersistedValue>` rather than `never`.
- **A step handler that throws a control-flow error itself fails the step.** A handler throwing `RunCancelledError`, say, while nothing was cancelled, left the run `running` until its lease lapsed, and then again on every reclaim. It is now an ordinary failure.
- **Thousands of steps running at once no longer slow each other down.** Each one listened for the run being cancelled on the same signal, and adding a listener there scans those already added, so a run of many concurrent steps took time growing with the square of their number. 40,000 concurrent steps now take about 0.3s rather than 8s.

## 0.10.0 — 2026-10-25

Durability, tightened where it was weakest. A waiting run's event and the step
it completes are now saved together, so a worker dying mid-wait cannot lose
the event. Postgres times every lease by the database's clock, so workers
whose clocks drift apart can no longer run the same step twice. And a step can
reuse the result an earlier run computed, so separate runs stop paying twice
for the same API or LLM call.

**Upgrading:** nothing to change. SQLite databases move to schema version 5 and Postgres schemas to version 2 on `initialize()`, each adding one table for cached steps. Open a database from before with `npx reflow-ts dashboard` and it asks for `--migrate`. On Postgres, a role allowed only to read and write rows cannot run the migration: run `initialize()` once as the schema's owner first. During a rolling deploy, workers still on 0.9 refuse the migrated database once they restart, so finish the rollout rather than rolling back.

### Added

- **Cache a step's result across runs.** `.step(name, { cache: '24h', handler })` reuses the result an earlier run computed for the same input and earlier steps' outputs, instead of running the handler again — for a paid API or an LLM call that separate runs would repeat. `cache: { ttl, key, version }` chooses what counts as the same (`key: ({ input }) => input.userId`) and discards results from older code when `version` changes. Only steps that complete are cached; a reused step fires `stepComplete` with `cached: true`. `engine.prune()` deletes expired results. Storage keeps them in a table of their own through three new optional `StorageAdapter` methods, which the built-in adapters implement and the conformance suite tests. (#21)

### Fixed

- **An event can no longer be lost when a worker dies mid-wait.** A waiting run took its event and then saved the step as two separate writes. If its process died between the two, the event was gone with no step to show for it, and the run waited for an event that had already arrived. A worker that had lost its lease in between put the event back, but behind any later events of the same name. Storage now does both in one transaction, under the run's lease: a worker that no longer holds the run takes nothing, and the event stays, in order, for the worker that does. `StorageAdapter.consumeEvent` is the new, optional method that does this; the built-in adapters implement it, and the conformance suite tests it. A custom adapter without it keeps the previous behaviour.
- **`testEngine` reports a step skipped by `when` as `skipped`.** It reported it as `failed`, with the error "Unknown error", although the run completed. Only conditional steps are typed as possibly skipped, as `SkippedStepOutput`.
- **Postgres leases are timed by the database's clock.** A run's lease was renewed with, and judged stale by, each worker's own clock, so a machine whose clock ran ahead of another's by more than the lease less the heartbeat interval (20 seconds with the defaults) took over that machine's live runs and ran their steps twice. Leases are now stamped and compared with the Postgres server's time, so worker clocks no longer affect which runs are live. Nothing to change: the storage contract is the same, and the engine's lease settings carry over. While 0.9 and newer workers share a database during a rollout, leases compare the old workers' clocks with the database's, so keep the rollout short if the database's clock may be far off.

## 0.9.0 — 2026-10-24

Room to grow, and durability stated exactly. Workflows written against a SQLite
file now run unchanged on PostgreSQL, shared by workers on many machines. Every
step gets an idempotency key that is the same however many times it runs, so a
step repeated after a crash can tell the API it calls. Runs can start later,
one workflow can be capped at so many runs at once across every process, and
the dashboard can be protected with a password in one line.

**Upgrading:** nothing to change. The built-in adapters need no migration. A
custom storage adapter keeps working; it only needs the new `limits` argument
of `claimNextRun` (and `supportsConcurrencyLimits = true`) once a workflow sets
`concurrency`.

### Added

- **PostgreSQL storage.** `reflow-ts/postgres` exports `PostgresStorage`, for running workflows from many processes on many machines against one database; install `pg` alongside. Pass a connection string or your app's own pool. Its tables live in their own Postgres schema (`reflow` by default), created and migrated by `initialize()` under an advisory lock, so every process can start at once. Claims take each due run with `FOR UPDATE SKIP LOCKED` from per-status partial indexes, so workers never take the same run or wait on one another; per-workflow concurrency limits are counted under a transaction-scoped advisory lock. It passes the full storage conformance suite, tested in CI against PostgreSQL 14 and 17. The storage guide covers moving an app from SQLite.
- **Limit how many runs of one workflow run at once.** `createWorkflow({ name, input, concurrency: 2 })` caps that workflow's running runs across every process sharing the storage — for calls to a rate-limited API, say — while other workflows carry on. Storage counts them in the same transaction that claims the next run, so two workers cannot both take the last place; sleeping and waiting runs hold no place, and a crashed run frees its place once its lease expires. Every process sharing the storage must use the same `runLeaseDurationMs`. `StorageAdapter.claimNextRun` takes the limits as an optional third argument, and an adapter declares it honours them with `supportsConcurrencyLimits`; an engine whose workflows set a limit refuses an adapter that does not. The built-in adapters support it, and the conformance suite tests it.
- **Start a run later.** `enqueue(name, input, { delay: '1h' })` or `{ startAt: date }` saves the run straight away and starts it when due. It waits in storage as a sleeping run, holding no worker, so it survives restarts and starts on time on whichever process is running then. The dashboard shows it as *Starts at …*.
- **Steps know who they are.** Every step handler receives `idempotencyKey`, `runId`, and `attempt`. `idempotencyKey` is the same each time that step runs for that run — across retries, crashes, and other processes picking the run up — and different for every other step and run, so a step that calls a payment provider or any API accepting an idempotency key can pass it along and a repeat after a crash returns the first result instead of acting twice. The crash recovery guide now states plainly that a step cut off mid-run runs again: finished steps never repeat, and steps run at least once.
- **`basicAuth({ username, password })`** from `reflow-ts/dashboard`, an `authorize` that protects the dashboard with a password through the browser's own sign-in prompt — for apps with no sign-in of their own to reuse. An empty or unset password throws a `ConfigError` at startup instead of serving the dashboard unprotected.
- **`authorize` may return a `Response`**, which is sent in place of the dashboard: a redirect to the app's login page, say, or a 403. Returning `false` still answers 401.

### Documentation

- **Changing workflows** is a new guide: what a run part-way through does when it resumes under changed code — a renamed step runs again, a step inserted earlier runs late, a removed workflow's runs are never claimed — and how to ship such changes as a new version beside the old one. Tests pin each behaviour it describes.
- The dashboard guide starts from the two ways to run it, with a complete example for an app, and shows how to open a production database over SSH without exposing a port. Its sign-in examples no longer check a bearer token, which a browser cannot send when visiting the page.

### Fixed

- `MemoryStorage` is exported from `reflow-ts/test`, as the storage guide always said it was.

## 0.8.0 — 2026-10-23

Runs become something you can see and reason about, and SQLite holds up under
real load. Every run records when it started and finished, what it returned,
why it failed, and every attempt at every step; a dashboard — in your app or
from `npx reflow-ts dashboard` — shows them, workflow by workflow. Underneath,
several processes can share one SQLite file, claims seek an index however long
the queue, the worker pool keeps every slot busy, and retry backoff survives a
crash.

**Upgrading:** nothing to change if you use the built-in storage adapters —
they migrate their tables on `initialize()`. Custom storage adapters implement
a revised contract; see [Migrating to 0.8](https://reflow.danfry.dev/guide/migrating-0-8).

### Added

- **A dashboard.** `reflow-ts/dashboard` exports `createDashboard({ storage, authorize, engine? })`, a standard Fetch API handler that serves a web view of your runs from inside your own app: each workflow with its steps and how its runs stand; a workflow's runs, each labelled by its input, with a sentence on where it stands and the steps it has reached; one run's steps with their status and timing, a retried step's attempts with each error and the wait before the next, and any stretch where nothing ran; and registered schedules. For each workflow it also shows, from its recent runs, how many reached each step, finished it, failed there, or are paused there now; their failures grouped by step and message; and how many started, completed, and failed each minute of the last hour. Pass `workflows` to list every workflow with its steps, including those a run has not reached, and to name runs by each workflow's `label`. It mounts directly in Bun, Deno, Hono, and Next.js route handlers; `toNodeHandler()` adapts it for `node:http` and Express. With `engine` it can cancel and resume runs and updates live; without, it is read-only. `authorize` is required, `allowedHosts` pins the hosts it answers to, actions need a same-host request carrying a header a cross-site page cannot send, and the page loads nothing external under a nonce-based content security policy. ([#91](https://github.com/danfry1/reflow-ts/issues/91))
- **`npx reflow-ts dashboard ./reflow.db`** serves that dashboard for a SQLite file, read-only, on `127.0.0.1`. It opens the file read-only, leaving it exactly as it was; answers only to requests addressed to the host it is bound on, so a site cannot reach it through DNS rebinding; refuses to upgrade an older database unless given `--migrate`; and never creates a database at a mistyped path.
- **`createWorkflow({ label })`** names each run for people, from its typed input — an order number, a user id — wherever runs are listed. Without it a run is shown by its idempotency key, or else its id.
- **Runs record their lifecycle.** `WorkflowRun` gains `startedAt` (first claimed, kept across crash recovery), `finishedAt`, `output` (a completed run's final output, including a `complete()` value), `error` (`{ stepName, message, code }` for a failed run), `wakeAt` (when a sleeping run wakes or a waiting run times out), and `waitingFor` (the event a waiting run awaits). `StepResult` gains `startedAt`, so a finished step's duration is `updatedAt - startedAt`, and `history` — its attempts at its handler, carried across durable backoffs and crashes, each with when it started and finished and the error it failed with, so the gaps between attempts show the backoffs it waited out. `error.code` is the error's own string code when it has one — `'STEP_TIMEOUT'`, or a system code like `'ECONNREFUSED'` — so failures can be grouped without parsing messages. New exported types: `RunError`, `TerminalRunStatus`, `StepAttempt`.
- **`engine.waitForRun(runId, { signal? })`** — wait for a run to finish and get it back, completed, failed, or cancelled. A run finishing on this engine resolves the wait immediately; one finished by another process is noticed by polling storage. Pass `AbortSignal.timeout(ms)` for a deadline. New exported error: `RunNotFoundError` (code `RUN_NOT_FOUND`). New exported type: `WaitForRunOptions`.
- **`engine.countRuns(filter?)`** — runs per status, every status present, optionally for one workflow. New exported types: `RunCounts`, `CountRunsFilter`.
- **`engine.prune({ olderThan, statuses? })`** — delete finished runs, with their steps and events, that finished longer ago than a cutoff. Runs that can still progress are never deleted. Works in batches of 500 so it never holds SQLite's write lock for long. New exported type: `PruneOptions`.
- **Read-only SQLite connections** — `new SQLiteStorage(path, { readOnly: true })` opens an existing database for reading only, on all three adapters: writes fail, and the file is left untouched, journal mode included. For inspecting a database another process owns, or a backup. New exported type: `SQLiteStorageOptions`.
- `SQLiteStorage#schemaVersion()` reports a database's schema version and the one this build writes.
- `RUN_STATUSES` and `STEP_STATUSES` — every run and step status as a readonly array; `RunStatus` and `StepStatus` are now derived from them.
- A guide to [inspecting runs](https://reflow.danfry.dev/guide/inspecting-runs) and retention.

### Fixed

- **Retry backoff no longer holds the run, and survives a crash.** A step waiting between attempts held its run's lease and a concurrency slot for the whole delay, and the attempt count lived only in memory — a crash mid-backoff restarted the step's attempts from one, so `maxAttempts` did not hold across restarts. On a started engine, backoff is now a durable suspension: the step is recorded as `retrying` with its attempts and latest error, the run sleeps until the next attempt with its lease released, and whichever engine resumes it continues at the right attempt. An engine driven only by `tick()` — and so `testEngine` — has nothing to resume a suspended run, and keeps waiting in process; so do parallel branches, whose siblings are still running. `onRunStart` fires again when a backed-off run resumes, as after a sleep. Resuming a run cancelled mid-backoff starts that step's attempts over. If a redeploy lowers `maxAttempts` below the attempts already made, the step fails with its last real error. New step status: `retrying`.
- **Short sleeps, wait timeouts, and retry backoffs resume on time.** A started engine sets a timer for runs it suspended that wake within five minutes, rather than leaving them to the next poll; previously every wake was up to `pollIntervalMs` late.
- **Several processes can share one SQLite database.** The `better-sqlite3` (`reflow-ts/sqlite-node`) and `bun:sqlite` (`reflow-ts/sqlite-bun`) adapters ran their transactions in SQLite's default deferred mode. A deferred transaction starts as a reader and upgrades to a writer on its first write; if another connection commits in between, the upgrade fails with `SQLITE_BUSY_SNAPSHOT`, which `busy_timeout` cannot retry. Claiming a run reads and then writes, so with four processes claiming from one file most claims failed outright. Every transaction now begins with `BEGIN IMMEDIATE`, taking the write lock up front, so contention becomes a short wait instead of an error. The `node:sqlite` adapter already did this.
- **Claiming no longer starves woken and crashed runs.** `claimNextRun` ranked every pending run ahead of every sleeping, waiting, or stale one, so while new runs kept arriving, a run whose `.sleep()` had elapsed, whose `.waitForEvent()` had timed out, or whose worker had crashed could wait indefinitely. Runs are now claimed in the order they became claimable: a pending run from when it became pending, a sleeping or waiting run from its wake time, a stale run from when its lease expired.
- **One slow run no longer holds the engine's other slots idle.** A started engine ran in batches: each poll claimed up to `concurrency` runs and waited for every one of them to finish before claiming more, so a single long step kept the remaining slots empty for its whole duration. Slots are now independent — when a run finishes, its slot is refilled straight away. On a workload where one run in ten takes 200ms and the rest 5ms, 300 runs at `concurrency: 10` drain in under a second instead of about six.
- **A failed claim no longer strands runs already claimed in the same batch.** When a claim threw partway through filling the slots, the runs claimed before it were never started and stayed `running` until their leases expired. They now start, and the claim failure is still reported — `tick()` rejects with it once they finish; a started engine delivers it to `onError`.

### Changed

- **Breaking for custom adapters:** `StorageAdapter.updateRunStatus` and `updateClaimedRunStatus` are replaced by `completeRun(runId, leaseId, output)`, `failRun(runId, leaseId, error)`, and `cancelRun(runId)`. A generic status setter could mark a run completed without an output, and made cancelling a read-then-write that could race a run completing; `cancelRun` refuses a finished run in the same write. Adapters also implement `countRuns` and `deleteRuns`, and store the new run and step fields. `engine.cancel()` behaves as before, without the race.
- **SQLite schema migrations are versioned.** The adapters record the schema version in `PRAGMA user_version` and apply only the migrations a database is missing, each in its own `BEGIN IMMEDIATE` transaction, so processes starting together cannot apply one twice. Existing databases upgrade in place; runs that finished before the upgrade get a `finishedAt` so they can be pruned. A database written by a newer version is refused with a `ConfigError` instead of being opened.
- Stored run and step statuses are validated when read rather than cast, so a corrupt or foreign row fails loudly.
- **SQLite claims and lists runs by seeking an index, however large the queue or history** (schema version 4). Claiming sorted every claimable run of the engine's workflows to find the oldest, so with 100k runs queued each claim took about 12ms and a worker drained at about 80 runs a second; it now seeks each workflow's oldest due run directly, in about 0.1ms, whatever the backlog, the history, or the number of workflows. The status indexes carry when runs became due; new indexes on creation time, overall and per workflow, on workflow and finish time, and on failed and cancelled runs let the newest runs be listed and the last hour counted without sorting the table. The upgrade builds them in about 9 seconds per million runs, during which other processes wait for the write lock — see [Migrating to 0.8](https://reflow.danfry.dev/guide/migrating-0-8#databases-migrate-themselves).
- `StorageAdapter` gains two optional methods, `getStepResultsForRuns` and `runActivity`, which the dashboard uses when present to read less; `reflow-ts/conformance` checks them when an adapter has them. New exported types: `RunActivity`, `RunActivityFilter`.
- `MemoryStorage.getStepResults` orders steps by creation time, as the contract states, rather than by when each was first saved.
- An error whose `message` is not a string — some libraries assign other values to it — is recorded with its message converted to one, instead of being stored as it was and leaving the run's record unreadable.
- A step that fails without making all its attempts, because the run was aborted part way, reports the attempts it actually made rather than `maxAttempts`.
- **The `StorageAdapter.claimNextRun` contract specifies claim order**, as described above. `reflow-ts/conformance` gains four cases for it; a custom adapter that ranks by status will fail them.
- The three SQLite adapters now share one implementation, each supplying only a small driver for its binding, so a fix to the SQL, the schema, or the transaction handling reaches all three at once. The `better-sqlite3` and `node:sqlite` adapters also cache prepared statements rather than preparing each query on every call; `bun:sqlite` does not, since its statements keep the column names they were prepared with. The public classes and their constructors are unchanged.
- **Work this instance creates starts without waiting for the next poll.** `enqueue()`, `resume()`, and `sendEvent()` on a started engine claim the run into a free slot at once, rather than after up to `pollIntervalMs`. Polling still picks up work the instance was not told about directly: runs enqueued by other processes, elapsed sleeps and wait timeouts, due schedules, and stale runs.
- `tick()` claims into whichever slots are free and resolves once those runs finish. Called on an idle engine it behaves as before; called while other runs are executing, it no longer skips outright but fills the remaining slots. `concurrency` is never exceeded between the two.

## 0.7.0 — 2026-08-10

Schedules gain cron. The stored next-occurrence introduced in 0.6 is what makes
this possible: interval arithmetic cannot express a cadence whose gaps vary,
but a stored next-run time can.

**Upgrading:** one breaking change, `WorkflowSchedule.intervalMs` becoming
`recurrence`. It affects code reading `engine.listSchedules()` and custom
storage adapters; the SQLite adapters migrate themselves. See
[Migrating to 0.7](https://reflow.danfry.dev/guide/migrating-0-7).

### Added

- **Cron schedules** — `engine.schedule(name, input, { cron: '0 9 * * 1-5' })` fires on a cron expression rather than a fixed gap. Supports the standard five fields (`minute hour day-of-month month day-of-week`) with wildcards, ranges, lists, a `/step` suffix, three-letter month and day names, and the `@hourly` / `@daily` / `@weekly` / `@monthly` / `@yearly` aliases. The parser is written from scratch — the package still has one runtime dependency.

  **Expressions are evaluated in UTC.** There is no time-zone option, because interpreting cron in a local zone means deciding what a schedule means during a DST gap (a wall-clock time that does not occur) and a DST overlap (one that occurs twice); guessing there produces a scheduler that silently skips or doubles a run twice a year.

  Malformed expressions are rejected by `schedule()` rather than on the first tick, as is an expression that can never occur (`0 0 30 2 *` — the 30th of February). Registration is the last point a bad cadence can reach the caller, since after that the schedule fires unattended. Missed occurrences are skipped, not backfilled, exactly as for intervals.

  `parseCron()` and `nextCronOccurrence()` are exported for validating an expression or previewing its occurrences without registering anything. New exported types: `CronExpression`, `ScheduleSpec`, `ScheduleRecurrence`.
- **Duration strings for schedule intervals** — `engine.schedule(name, input, { every: '1h' })` accepts the same unit-suffixed form as `.sleep()`. A bare number is still milliseconds, so existing callers are unaffected.

### Changed

- **Breaking:** `WorkflowSchedule.intervalMs` is replaced by `recurrence`, a tagged union of `{ kind: 'interval'; intervalMs }` and `{ kind: 'cron'; expression }`. A union rather than two optional fields, so a schedule cannot be stored with both or neither. This affects code reading `engine.listSchedules()` and anyone implementing a custom `StorageAdapter`; the `StorageAdapter` method signatures themselves are unchanged.
- The SQLite adapters store the recurrence in a nullable `interval_ms` plus a new `cron` column. 0.6.0 created that table with `interval_ms INTEGER NOT NULL`, which a cron schedule cannot satisfy, and SQLite cannot drop a `NOT NULL` in place — so `initialize()` rebuilds the table when it finds the old shape, preserving existing rows. Automatic and idempotent; **no manual migration is needed**.

## 0.6.0 — 2026-08-09

Durability beyond the step. A workflow can now pause for days, wait on a
webhook, skip work it doesn't need, and run on a schedule that outlives the
process that registered it. Every storage adapter those features rest on is
held to one shared contract, runnable against your own adapter too.

**Upgrading:** this release breaks the `schedule()` API and adds methods to
`StorageAdapter`. Everything else is additive. See [Migrating to 0.6](https://reflow.danfry.dev/guide/migrating-0-6).

### Added

- **Durable sleep** — `.sleep(name, duration)` pauses a workflow between steps for a duration that survives process exit, deploy, or crash. The run is persisted as `sleeping` with its lease released (so the process need not stay alive), and any engine instance reclaims and resumes it once the time elapses. `duration` is a number of milliseconds or a unit-suffixed string (`'500ms'`, `'30s'`, `'24h'`, `'7d'`). `prev` passes through unchanged, and the sleep is observable as a step (`sleeping` → `completed`).
- **Wait for events** — `.waitForEvent(name, { schema?, timeoutMs? })` durably suspends a workflow until `engine.sendEvent(runId, name, payload)` delivers a matching event (e.g. a webhook, an approval, a human action). The run is persisted as `waiting` with its lease released and resumes — on any engine instance — when the event arrives; the payload (validated against `schema`, if given) becomes the next step's `prev`. Delivery is durable and order-independent: an event sent before the run reaches the wait is buffered and consumed when it gets there. With `timeoutMs`, the run fails with the new `WaitTimeoutError` if no event arrives in time. New exported error: `WaitTimeoutError`.
- **Conditional steps** — `.step(name, { when, handler })` accepts a `when` predicate receiving `{ input, prev, steps }`. When it returns `false` the step is skipped: `prev` passes through unchanged, the step is recorded with the new `skipped` status, and a `stepSkipped` event fires (with a matching `onStepSkipped` hook). The decision is evaluated once and persisted, so it is never recomputed on crash-recovery replay; a throwing predicate fails the run at that step. `when` may be synchronous or `async`, and is supported on sequential `.step()` only — passing it to a `.parallel()` branch throws `ConfigError`. The builder types follow the skip: after a conditional step `prev` widens to a union with the passed-through value, and the step's entry in `steps` becomes optional, so neither can be read as though the step definitely ran. New exported types: `ConditionalStepConfig`, `StepCondition`, `StepConditionContext`. Grew out of [#21](https://github.com/danfry1/reflow-ts/issues/21) (raised by [@brianjenkins94](https://github.com/brianjenkins94)), which stays open: `when` decides whether a step runs, whereas that issue asks for a skipped step to still supply its previous result downstream.
- **Storage errors are translated at the boundary** — `better-sqlite3`, `bun:sqlite`, and `node:sqlite` report the same condition (a busy database, a failed write, a closed connection) with different error types and different properties carrying the code, and those shapes leaked straight into user code. Storage failures now surface as a `StorageError` carrying the failing `operation` and the driver's error as `cause`, so callers catch one type instead of branching per driver. The driver's message is folded into the `StorageError` message rather than left only on `cause`, since a failed step persists its error as a plain string and a cause chain does not survive that. Adapters that already throw a `ReflowError` pass through untouched, so custom adapters are never double-wrapped. New exported error: `StorageError`.
- **`reflow-ts/conformance`** — the behavioural contract the built-in storage adapters are held to, published as a list of runnable cases so a custom adapter can be validated against it rather than inferred from prose. Each case takes a freshly initialized adapter and throws on failure, with no test-framework import, so it works with any runner. This is also how the Bun adapter is covered: `bun:sqlite` cannot load under the Node-based test run, so the same list is driven against it by a Bun script — closing the gap that let `bun:sqlite`'s affected-row reporting diverge unnoticed. New exported type: `ConformanceCase`.
- **Error codes** — every `ReflowError` now carries a stable literal `code` (`'STEP_TIMEOUT'`, `'LEASE_EXPIRED'`, …) alongside its class. Branching on `code` is a closed union, so a `switch` over it can be checked for exhaustiveness, and unlike `instanceof` it survives bundling, duplicate copies of the package, and cross-realm boundaries. Errors also implement `toJSON()`, emitting the discriminant, their structured context, and a flattened `cause` chain, so they can be logged without a custom serializer. New exported types: `ReflowErrorCode`, `SerializedReflowError`. New exported errors: `StepFailedError`, `HookError`, `ThrownValueError`, `TestRunIncompleteError`, `InternalError`. New exported helpers: `toError`, `assertNever`.
- **`engine.listRuns(filter?)`** — list runs for inspection and dead-letter visibility, ordered by `createdAt` then `id` (descending). Filter by `status` and/or `workflow`, and paginate with `limit` and a keyset cursor (`before` + `beforeId`) that is exact even when runs share a `createdAt`. New exported `ListRunsFilter` type.
- **`engine.resume(runId)`** — re-queue a `failed` or `cancelled` run. Completed steps are preserved and skipped on replay, so execution resumes at the step that failed (its result is discarded and re-run). Returns `false` when the run is missing or not in a resumable state, and throws `WorkflowNotFoundError` if the run's workflow is not registered on the engine (rather than leaving an unclaimable pending run).
- **Benchmark suite** — `bun run bench` (`benchmarks/`) measures engine + storage throughput (enqueue and end-to-end execution) for the in-memory and Bun SQLite adapters, with baseline numbers in the README. Each scenario runs several timed passes and reports the best, since anything sharing the machine only ever makes a pass slower; the table prints the spread between passes and warns when it exceeds 25%, so a machine too busy to measure on says so rather than quietly producing a wrong figure.

### Changed

- **Breaking:** `engine.schedule()` and `engine.unschedule()` are now `async`, since both perform storage I/O. `schedule()` resolves to the schedule's key (previously a random in-process id) and takes an optional fourth argument `{ key }` to set that identity explicitly; `unschedule()` takes the key and resolves to whether a schedule was removed. New `engine.listSchedules()` returns the registered schedules with their next firing times. New exported types: `ScheduleOptions`, `WorkflowSchedule`.
- **Breaking:** `engine.stop()` no longer clears schedules. They belong to the storage rather than to one instance, so stopping a worker must not cancel a schedule the rest of the fleet is still serving. Use `unschedule()` to remove one.
- **Breaking:** `StorageAdapter` gains `upsertSchedule`, `claimDueSchedule`, `deleteSchedule`, and `listSchedules`; the SQLite adapters add a `workflow_schedules` table, created automatically on `initialize()` with no migration needed for existing databases. Custom adapters must implement the new methods — `claimDueSchedule` in particular must claim and advance atomically, since that is what makes a firing exclusive.
- **Breaking:** `StorageAdapter` gains `sleepRun(runId, leaseId, wakeAt)`, `waitRun(runId, leaseId, eventName, wakeAt)`, `deliverEvent(runId, eventName, payload)`, and `takeEvent(runId, eventName)`; `claimNextRun` now also reclaims `sleeping`/`waiting` runs whose wake time has passed. All are implemented across the built-in adapters; the SQLite adapters add a `wake_at` column (with an automatic, backward-compatible migration) and a `workflow_events` table. Custom adapters must implement the new methods — `waitRun` in particular must leave the run reclaimable when a matching event is already buffered, or a run can strand until its timeout.
- **Breaking:** `StorageAdapter` gains two methods, `listRuns(filter?)` and `requeueRun(runId)`, implemented across all built-in adapters (`memory`, `sqlite-node`, `sqlite-bun`, `sqlite-node-builtin`). Custom adapters must implement them. Across the three entries above, this release adds ten `StorageAdapter` methods in total; `reflow-ts/conformance` is the fastest way to check an implementation against them.
- **Observer failures are reported instead of swallowed.** A lifecycle hook, stream consumer, or `onFailure` handler that throws was previously discarded by an empty `catch`, making a broken hook invisible. Those failures are now wrapped in a `HookError` (with the original throw preserved as `cause`) and delivered to the `onError` hook. Observers still cannot affect a run's outcome — only their visibility changes. Engines with no `onError` hook behave exactly as before.
- Non-`Error` throws (`throw 'boom'`) are wrapped in `ThrownValueError` rather than a bare `Error`. The message is unchanged — it is still the thrown value's string form — and the original value is now retained on both `cause` and `.value`, so a thrown object is no longer flattened beyond recovery.
- `testEngine.run()` throws `TestRunIncompleteError` (was a bare `Error`) when a run has not reached a terminal state after its single tick, and the message now names the usual cause: a workflow that suspends on `.sleep()` or `.waitForEvent()` needs a full `createEngine` to drive it.
- `RunStatus` gains `sleeping` and `waiting`; `StepStatus` gains `sleeping`, `waiting`, and `skipped`.
- `EngineEvent` / `EngineHooks` gain the additive `stepSkipped` / `onStepSkipped` variant.
- Run execution is split into one executor per execution-unit kind behind a shared `UnitExecutor` contract, replacing the single ~390-line branch-per-kind loop in the engine. Internal reorganisation with no public API change, but it fixes a step row-identity bug: because `saveStepResult` upserts by row `id` and the step path generated a fresh one on every write (unlike the sleep, wait, and parallel paths), a step re-executed after a reclaim appended a second row under the same name, surfacing as a duplicated step in `getRunStatus()`.
- The `node:sqlite` adapter now opens transactions with `BEGIN IMMEDIATE` so concurrent claims under WAL take the write lock up front instead of risking a non-retryable `SQLITE_BUSY_SNAPSHOT`.

### Fixed

- **Schedules are now durable, and no longer multiply across engine instances.** `engine.schedule()` registered an in-process `setInterval` that called `enqueue()` with no idempotency key. Two consequences, both silent: schedules vanished on restart or deploy, with nothing to re-register them; and because engine instances share storage — the deployment the concurrency guide recommends — a schedule registered on N workers produced N runs per interval. For a library whose premise is durable execution, an in-memory scheduler was the wrong default.

  Schedules are now stored. `engine.schedule()` upserts a durable record keyed by the schedule's identity and returns that key; firing happens during the engine's normal tick, claiming each due occurrence atomically and advancing its next occurrence in the same transaction, so exactly one instance enqueues it. A worker only claims schedules for workflows it has registered, so a schedule is never swallowed by an instance that cannot serve it. Re-registering preserves the existing cadence unless the interval changed, which makes calling `schedule()` unconditionally at startup on every instance the intended usage. Occurrences missed while the fleet was down are skipped rather than backfilled.
- **`reflow-ts/sqlite-bun` change detection** — `bun:sqlite` reports affected-row counts on the `run()` result, not on `Database.changes` (which is `undefined`). The adapter read the latter, so `heartbeatRun`, `updateRunStatus`, `updateClaimedRunStatus`, and the `claimNextRun` double-claim guard never reflected real row counts — heartbeats always reported the lease lost and the claim guard never engaged. The adapter now reads the count from the statement result. Adds a Bun-native test run (`bun run test:bun`) wired into CI so the Bun adapter — which the Node-based Vitest suite cannot load — has real coverage; it now drives the full `reflow-ts/conformance` contract.

## 0.5.0 — 2026-06-10

### Added

- **`engine.stream()`** — a pull-based, backpressure-aware stream of execution events. Returns an `AsyncIterableIterator<EngineEvent>` (also an `AsyncDisposable`) so you can `for await` over `runStart` / `stepStart` / `stepComplete` / `runComplete` / `runFailed` events instead of wiring up callback-to-queue plumbing. Optional `bufferSize` paces the engine against a slow consumer; breaking out of the loop, `await using`, or `engine.stop()` unsubscribes automatically. New exported types: `EngineEvent`, `EngineEventOf`, `StreamOptions`, `ResultStream`. ([#24](https://github.com/danfry1/reflow-ts/issues/24) — suggested by [@brianjenkins94](https://github.com/brianjenkins94))
- **Async hooks** — lifecycle hooks may now be `async` and are awaited before the engine proceeds, so a hook can flush a metric, persist an audit row, or apply backpressure with ordering guarantees. A throwing or rejecting hook is still fully contained and never affects engine state. ([#23](https://github.com/danfry1/reflow-ts/issues/23) — suggested by [@brianjenkins94](https://github.com/brianjenkins94))
- **`reflow-ts/sqlite-node-builtin`** — a new SQLite storage adapter for Node.js backed by the built-in `node:sqlite` module, with **zero native dependencies** (the Node equivalent of the Bun adapter). Requires Node ≥ 22.5; the existing `better-sqlite3`-based `reflow-ts/sqlite-node` adapter remains for Node ≥ 18.18.

### Changed

- Hook event objects are now aligned with `EngineEvent`: every event carries a `type` discriminator and the owning `workflow` name, `onStepStart` / `onStepComplete` now include `workflow`, and `onRunComplete` now includes the workflow's final `output`. These are additive — existing hook callbacks continue to type-check and run unchanged.
- The minimum supported Node.js version is now 18.18, the first Node 18 release with `Symbol.asyncDispose` support.

## 0.4.0

### Added

- **`.parallel({ ... })`** — concurrent step groups. Run independent steps at the same time and receive a typed merged record as `prev` in the next step. Each branch supports per-branch retry and `timeoutMs`. Fail-fast on first branch failure (siblings receive `signal.abort()`) and per-branch crash recovery — completed branches are skipped on resume, so side effects do not fire twice. `onRunFailed` and `onFailure` report the branch that actually caused the failure, not a sibling aborted by propagation. ([#20](https://github.com/danfry1/reflow-ts/issues/20) — suggested by [@brianjenkins94](https://github.com/brianjenkins94))

### Changed

- `Workflow` exposes a new `executionUnits` array (replacing the internal `steps` list) so the engine can distinguish sequential steps from parallel groups. Sequential `.step()` workflows remain fully type- and behavior-compatible.
- New `ParallelCompleteError` is exported and thrown when `complete()` is called inside a parallel branch (early completion is only meaningful in sequential context).

## 0.3.0

### Added

- **`onRunStart` / `onStepStart` hooks** — new lifecycle hooks for observability and timing; `onRunStart` fires when a run begins executing, `onStepStart` fires before each step runs ([#12](https://github.com/danfry1/reflow-ts/issues/12) — suggested by [@brianjenkins94](https://github.com/brianjenkins94))

## 0.2.0 — 2026-03-20

### Added

- **Date support** — `Date` is now a valid `PersistedValue` type, automatically serialized and deserialized through storage
- **`complete(value?)`** — step handlers can finish a workflow early, skipping remaining steps and optionally persisting a final value as the step result
- **Typed `steps` context** — each step handler receives a `steps` object with typed access to all previously completed step results by name, removing the need to forward data through `prev` across intermediate steps

### Changed

- `PersistedPrimitive` now includes `Date`
- `StepContext` has two new fields: `complete` and `steps`
- `StepStatus` has a new value: `completed-early` (used internally for crash-safe early completion)
- `executeStep` internals refactored to return a discriminated union instead of using exceptions for control flow

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
