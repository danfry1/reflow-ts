/** Every lifecycle state of a workflow run. */
export const RUN_STATUSES = ['pending', 'running', 'sleeping', 'waiting', 'completed', 'failed', 'cancelled'] as const

/** Lifecycle state of a workflow run. */
export type RunStatus = (typeof RUN_STATUSES)[number]

/** Every lifecycle state of a single step within a run. */
export const STEP_STATUSES = [
  'pending',
  'running',
  'completed',
  'completed-early',
  'skipped',
  'sleeping',
  'waiting',
  'retrying',
  'failed',
] as const

/** Lifecycle state of a single step within a run. */
export type StepStatus = (typeof STEP_STATUSES)[number]

/** Primitive values that can be persisted to storage. */
export type PersistedPrimitive = string | number | boolean | null | undefined | Date

/** Object whose values are all persistable. */
export interface PersistedObject {
  [key: string]: PersistedValue
}

/**
 * Any value that can be stored as step input/output.
 * Supports plain objects, arrays, strings, numbers, booleans, null, undefined, and Date.
 */
export type PersistedValue = PersistedPrimitive | PersistedValue[] | PersistedObject

/** A run status from which a run never moves on its own: only `resume()` re-queues it. */
export type TerminalRunStatus = Extract<RunStatus, 'completed' | 'failed' | 'cancelled'>

/** Why a run failed: the step that failed and the error it failed with. */
export interface RunError {
  /** The step, parallel branch, or wait whose failure ended the run. */
  stepName: string
  message: string
  /**
   * The error's string `code`, when it has one — a `ReflowErrorCode` such as
   * `'STEP_TIMEOUT'`, or a system code such as `'ECONNREFUSED'` — otherwise `null`.
   */
  code: string | null
}

/** A persisted workflow run record. */
export interface WorkflowRun {
  id: string
  workflow: string
  input: PersistedValue
  idempotencyKey: string | null
  status: RunStatus
  createdAt: number
  updatedAt: number
  /** When the run was first claimed, or `null` if it has never started. Kept across reclaims. */
  startedAt: number | null
  /** When the run reached `completed`, `failed`, or `cancelled`; `null` while it can still progress. */
  finishedAt: number | null
  /** The final output of a `completed` run — the last step's output, or the `complete()` value. */
  output: PersistedValue
  /** Why a `failed` run failed; `null` otherwise. */
  error: RunError | null
  /** When a `sleeping` or `waiting` run is due to wake (a wait's timeout); `null` otherwise. */
  wakeAt: number | null
  /** The event a `waiting` run is waiting for; `null` otherwise. */
  waitingFor: string | null
}

/** A workflow run that has been claimed by an engine instance for execution. */
export interface ClaimedRun extends WorkflowRun {
  leaseId: string
}

/** The persisted result of a single step execution. */
export interface StepResult {
  id: string
  runId: string
  name: string
  status: StepStatus
  output: PersistedValue
  error: string | null
  attempts: number
  createdAt: number
  updatedAt: number
  /**
   * When the step's latest execution began. With `updatedAt`, gives how long a
   * finished step took. A step re-run after `resume()` gets a fresh value.
   */
  startedAt: number | null
  /**
   * The step's attempts at its handler, oldest first, including those made by
   * earlier executions before a durable backoff or a crash. The gap between
   * one attempt's `finishedAt` and the next one's `startedAt` is at least the
   * backoff the step waited out. Keeps the latest 100. Empty for sleeps,
   * waits, and skipped steps, and for steps recorded before 0.8.
   *
   * Attempts are saved when the step suspends or finishes, so those a step
   * retries in process — zero-delay retries, parallel branches, an engine
   * driven only by `tick()` — are lost along with the step's attempt count if
   * the process dies before it does.
   */
  history: StepAttempt[]
}

/** One attempt at running a step's handler. */
export interface StepAttempt {
  /** When the attempt began (epoch ms). */
  startedAt: number
  /** When it returned or threw (epoch ms). */
  finishedAt: number
  /** The message it failed with; `null` for the attempt that succeeded. */
  error: string | null
}

/** Configuration for automatic step retries. */
export interface RetryConfig {
  /** Maximum number of attempts before the step fails. */
  maxAttempts: number
  /** Delay growth strategy between retries. */
  backoff: 'linear' | 'exponential'
  /** Base delay in milliseconds (default: 1000). Grows according to the backoff strategy. */
  initialDelayMs?: number
  /** Timeout per attempt in milliseconds. Overridden by step-level `timeoutMs`. */
  timeoutMs?: number
}

/** A workflow run together with its step results, returned by `engine.getRunStatus()`. */
export interface RunInfo {
  run: WorkflowRun
  steps: StepResult[]
}

/**
 * How a schedule repeats.
 *
 * A tagged union rather than two optional fields, so a schedule cannot be
 * stored with both an interval and a cron expression, or neither.
 */
export type ScheduleRecurrence =
  /** A fixed gap between firings. */
  | { readonly kind: 'interval'; readonly intervalMs: number }
  /** A five-field cron expression, evaluated in UTC. */
  | { readonly kind: 'cron'; readonly expression: string }

/**
 * A durably registered recurring schedule.
 *
 * Unlike a run, a schedule is long-lived and shared: every engine instance
 * registering the same `key` addresses one row, and whichever instance claims a
 * due firing enqueues it.
 */
export interface WorkflowSchedule {
  /**
   * Stable identity of the schedule, shared across engine instances and across
   * restarts. Re-registering the same key updates that schedule rather than
   * creating a second one.
   */
  key: string
  /** Name of the workflow to enqueue. */
  workflow: string
  /** Validated input handed to each enqueued run. */
  input: PersistedValue
  /** How the schedule repeats. */
  recurrence: ScheduleRecurrence
  /** Epoch ms at which this schedule is next due to fire. */
  nextRunAt: number
  createdAt: number
  updatedAt: number
}

/** Result of `storage.createRun()`. `created` is false when an existing idempotent run was returned. */
export interface CreateRunResult {
  run: WorkflowRun
  created: boolean
}

/**
 * Filter for {@link StorageAdapter.listRuns} and `engine.listRuns()`.
 *
 * Results are ordered by `createdAt` descending, then `id` descending, so the
 * order is total and stable even when runs share a `createdAt`. For exact
 * keyset pagination pass both `before` and `beforeId` from the last row of the
 * previous page; passing `before` alone is a coarse "created before T" filter
 * that can drop runs tied on that millisecond.
 */
export interface ListRunsFilter {
  /** Only return runs with this status. */
  status?: RunStatus
  /** Only return runs of this workflow. */
  workflow?: string
  /** Maximum number of runs to return (default: 100). */
  limit?: number
  /** Keyset cursor: return runs ordered before this `createdAt` (paired with `beforeId` for the tie-break). */
  before?: number
  /** Keyset cursor tie-break: with `before`, return runs strictly after this `(createdAt, id)` position in the sort order. */
  beforeId?: string
}

/** How many runs are in each status, from {@link StorageAdapter.countRuns}. */
export type RunCounts = Record<RunStatus, number>

/**
 * A step's result kept for reuse by later runs, from
 * {@link StorageAdapter.putCachedStep}. Outlives the run that produced it.
 */
export interface CachedStep {
  /** The cache key: a hash of the workflow, step, version, and key value. */
  key: string
  workflow: string
  stepName: string
  output: PersistedValue
  /** When the result was cached (epoch ms). */
  createdAt: number
  /** When it stops being reused (epoch ms). */
  expiresAt: number
}

/** What {@link StorageAdapter.consumeEvent} did. */
export type ConsumeEventResult =
  | { readonly status: 'consumed'; readonly payload: PersistedValue }
  | { readonly status: 'none' }
  | { readonly status: 'lease-lost' }

/** Filter for {@link StorageAdapter.runActivity}. */
export interface RunActivityFilter {
  /** Only count runs of this workflow. */
  workflow?: string
  /** Start of the first bucket (epoch ms). */
  since: number
  /** Width of each bucket in milliseconds. */
  bucketMs: number
  /** How many buckets, from `since` on. */
  buckets: number
}

/**
 * Runs started, completed, and failed in each of a run of equal time buckets,
 * from {@link StorageAdapter.runActivity}. Each array has one entry per
 * bucket; a run counts as started in the bucket holding its `createdAt`, and
 * as completed or failed in the one holding its `finishedAt`.
 */
export interface RunActivity {
  started: number[]
  completed: number[]
  failed: number[]
}

/** Filter for {@link StorageAdapter.countRuns} and `engine.countRuns()`. */
export interface CountRunsFilter {
  /** Only count runs of this workflow. */
  workflow?: string
}

/** Which finished runs {@link StorageAdapter.deleteRuns} removes. */
export interface DeleteRunsFilter {
  /** Only runs that finished strictly before this time (epoch ms). */
  finishedBefore: number
  /** Only runs in these statuses. Every status listed must be terminal. */
  statuses: readonly TerminalRunStatus[]
  /** Delete at most this many runs, oldest-finished first. */
  limit: number
}

/**
 * Interface for durable workflow storage backends.
 *
 * Implement this to use a custom database. Reflow ships with `SQLiteStorage` (for both Bun and Node.js) and `MemoryStorage`.
 */
export interface StorageAdapter {
  /** Create tables/indexes. Called once by `engine.start()`. */
  initialize(): Promise<void>
  /** Persist a new run. Must handle idempotency key conflicts. */
  createRun(run: WorkflowRun): Promise<CreateRunResult>
  /**
   * Atomically claim the next runnable run for execution: a `pending` run, a
   * stale `running` run (older than `staleBefore`), or a `sleeping`/`waiting`
   * run whose wake time has passed.
   *
   * Must claim the run that has been claimable longest: a pending run counts
   * from when it became pending (its `updatedAt`), a sleeping or waiting run
   * from its wake time, and a stale run from when its lease expired. Ranking
   * by kind instead — say, every pending run first — lets a steady stream of
   * new work starve woken and crashed runs indefinitely.
   *
   * Claiming sets `startedAt` if it is still `null` (a reclaim keeps the
   * original) and clears `wakeAt` and `waitingFor`.
   *
   * The engine passes `staleBefore` as its `Date.now()` less the lease. An
   * adapter shared by many machines may instead apply that lease length to a
   * clock of its own, as PostgresStorage does with the database's.
   *
   * `limits` caps how many runs of a workflow may be running at once, across
   * every process sharing this storage. A workflow in it already at its limit
   * is skipped: count its `running` runs whose lease is live (updated after
   * `staleBefore`, or all of them when `staleBefore` is not given), in the
   * same transaction as the claim, so two workers cannot both take the last
   * place. Only adapters that set {@link StorageAdapter.supportsConcurrencyLimits}
   * are passed it.
   */
  claimNextRun(workflowNames: readonly string[], staleBefore?: number, limits?: ReadonlyMap<string, number>): Promise<ClaimedRun | null>
  /**
   * Whether `claimNextRun` honours its `limits` argument. An engine whose
   * workflows set `concurrency` refuses storage without it, rather than
   * running more at once than the limit allows.
   */
  readonly supportsConcurrencyLimits?: boolean
  /** Renew the lease on a running run. Returns false if the lease was lost. */
  heartbeatRun(runId: string, leaseId: string): Promise<boolean>
  /**
   * Suspend a running run until `wakeAt` (epoch ms), releasing its lease so it
   * can be reclaimed by `claimNextRun` once that time passes. Only succeeds if
   * the caller still holds the lease; returns false otherwise.
   */
  sleepRun(runId: string, leaseId: string, wakeAt: number): Promise<boolean>
  /**
   * Suspend a running run until the named event arrives (or `wakeAt`, if
   * non-null, for a timeout), releasing its lease. Like {@link StorageAdapter.sleepRun}
   * but the run is `waiting` and may also be woken early by {@link StorageAdapter.deliverEvent}.
   *
   * Must, in the same transaction, check for an already-buffered event matching
   * `eventName`: if one exists the run is left `pending` (reclaimable) rather than
   * `waiting`, closing the race where an event is delivered between the caller's
   * check for one ({@link StorageAdapter.consumeEvent}, or
   * {@link StorageAdapter.takeEvent} without it) and this call. Only succeeds if the
   * caller still holds the lease.
   *
   * A run left `waiting` records `eventName` as `waitingFor` and the deadline
   * as `wakeAt`.
   */
  waitRun(runId: string, leaseId: string, eventName: string, wakeAt: number | null): Promise<boolean>
  /**
   * Durably record an event for a run and wake it if it is currently `waiting`.
   * The event is buffered (it may arrive before the run reaches the wait) and
   * consumed later by {@link StorageAdapter.takeEvent}. Returns false if the run
   * does not exist.
   */
  deliverEvent(runId: string, eventName: string, payload: PersistedValue): Promise<boolean>
  /**
   * Atomically consume the oldest buffered event matching `(runId, eventName)`,
   * removing it. Returns the payload wrapper, or null when no such event exists.
   */
  takeEvent(runId: string, eventName: string): Promise<{ payload: PersistedValue } | null>
  /**
   * Optional. Consume the oldest buffered event named `step.name` for
   * `step.runId` and save `step`, with the event's payload as its output, in
   * one transaction, only while `leaseId` is still held.
   *
   * Either both happen or neither does: when the lease has been lost, or no
   * event is buffered, nothing changes, and the event stays for whichever
   * engine holds the run next. Without this method the engine falls back to
   * {@link StorageAdapter.takeEvent} then {@link StorageAdapter.saveStepResult},
   * which loses the event if the process dies between the two.
   */
  consumeEvent?(step: Omit<StepResult, 'output'>, leaseId: string): Promise<ConsumeEventResult>
  /**
   * Optional, with {@link StorageAdapter.putCachedStep} and
   * {@link StorageAdapter.deleteExpiredCachedSteps}: the cached result under
   * `key`, or null. Returns an entry whether or not it has expired; the engine
   * decides. An engine whose workflows cache a step refuses storage without
   * all three.
   */
  getCachedStep?(key: string): Promise<CachedStep | null>
  /** Optional. Store `entry`, replacing any entry under the same key. */
  putCachedStep?(entry: CachedStep): Promise<void>
  /**
   * Optional. Delete up to `limit` entries that expired at or before `now`,
   * returning how many. Backs `engine.prune()`, which calls it until a batch
   * comes back short, so no one call holds a write lock for long.
   */
  deleteExpiredCachedSteps?(now: number, limit: number): Promise<number>
  /** Fetch a run by ID, or null if not found. */
  getRun(runId: string): Promise<WorkflowRun | null>
  /** List runs in reverse-chronological order (most recent first), optionally filtered. */
  listRuns(filter?: ListRunsFilter): Promise<WorkflowRun[]>
  /**
   * Reset a `failed` or `cancelled` run to `pending` so it can be re-executed,
   * discarding any `failed` or `retrying` step results so that step runs again
   * from its first attempt.
   * Completed steps are preserved and skipped on replay. Returns false if the
   * run does not exist or is not in a resumable state.
   *
   * Clears `finishedAt`, `output`, and `error`; `startedAt` is kept.
   */
  requeueRun(runId: string): Promise<boolean>
  /** Fetch all step results for a run, ordered by creation time. */
  getStepResults(runId: string): Promise<StepResult[]>
  /**
   * Optional. The step results of several runs at once, keyed by run id, each
   * list ordered as {@link StorageAdapter.getStepResults} orders it; a run
   * with no steps, or that does not exist, maps to an empty list. Readers
   * such as the dashboard fall back to one `getStepResults` call per run.
   */
  getStepResultsForRuns?(runIds: readonly string[]): Promise<Map<string, StepResult[]>>
  /**
   * Optional. How many runs started, completed, and failed in each of a run of
   * time buckets — see {@link RunActivity}. Readers such as the dashboard
   * fall back to counting a sample of recent runs, and say so.
   */
  runActivity?(filter: RunActivityFilter): Promise<RunActivity>
  /** Persist a step result. If `leaseId` is provided, fails when the lease is no longer held. */
  saveStepResult(result: StepResult, leaseId?: string): Promise<boolean>
  /**
   * Mark a held run `completed`, recording its final `output` and `finishedAt`
   * and releasing the lease. Returns false, changing nothing, if the caller no
   * longer holds the lease.
   */
  completeRun(runId: string, leaseId: string, output: PersistedValue): Promise<boolean>
  /**
   * Mark a held run `failed`, recording `error` and `finishedAt` and releasing
   * the lease. Returns false, changing nothing, if the caller no longer holds
   * the lease.
   */
  failRun(runId: string, leaseId: string, error: RunError): Promise<boolean>
  /**
   * Mark a run `cancelled` and record `finishedAt`, whoever holds it, unless it
   * has already finished. The check and the write must be atomic, so a run
   * completing concurrently is never reported cancelled. Returns false if the
   * run does not exist or has already finished.
   */
  cancelRun(runId: string): Promise<boolean>
  /** How many runs are in each status, optionally for one workflow. Every status is present, zero or not. */
  countRuns(filter?: CountRunsFilter): Promise<RunCounts>
  /**
   * Delete finished runs matching `filter`, oldest-finished first, together
   * with their step results and buffered events. Runs that can still progress
   * are never deleted. Returns how many runs were removed.
   */
  deleteRuns(filter: DeleteRunsFilter): Promise<number>
  /**
   * Register a recurring schedule, or update the existing one with the same
   * `key`. Returns the stored schedule.
   *
   * Re-registering must **preserve `nextRunAt`** when the recurrence is
   * unchanged, so a restarting process rejoins the existing cadence instead of
   * pushing the next firing out by a full interval every deploy. When the
   * recurrence changes, the supplied `nextRunAt` takes effect.
   */
  upsertSchedule(schedule: WorkflowSchedule): Promise<WorkflowSchedule>
  /**
   * Atomically claim the next schedule due at or before `now`, advancing its
   * `nextRunAt` to the first occurrence after `now` in the same transaction.
   *
   * The returned schedule carries the `nextRunAt` it was claimed *for* (the slot
   * that came due), not the advanced value. Advancing as part of the claim is
   * what stops several engine instances firing the same slot. Returns null when
   * nothing is due.
   *
   * Only schedules whose workflow appears in `workflowNames` are considered.
   * Claiming advances the schedule, so claiming one this instance cannot run
   * would silently swallow that firing in a fleet where workers register
   * different workflows.
   */
  claimDueSchedule(workflowNames: readonly string[], now: number): Promise<WorkflowSchedule | null>
  /** Remove a schedule by key. Returns false if no such schedule existed. */
  deleteSchedule(key: string): Promise<boolean>
  /** All registered schedules, ordered by key. */
  listSchedules(): Promise<WorkflowSchedule[]>
  /** Release resources (e.g. close the database connection). */
  close(): void
}
