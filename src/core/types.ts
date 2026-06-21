/** Lifecycle state of a workflow run. */
export type RunStatus = 'pending' | 'running' | 'sleeping' | 'waiting' | 'completed' | 'failed' | 'cancelled'

/** Lifecycle state of a single step within a run. */
export type StepStatus =
  | 'pending'
  | 'running'
  | 'completed'
  | 'completed-early'
  | 'skipped'
  | 'sleeping'
  | 'waiting'
  | 'failed'

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

/** A persisted workflow run record. */
export interface WorkflowRun {
  id: string
  workflow: string
  input: PersistedValue
  idempotencyKey: string | null
  status: RunStatus
  createdAt: number
  updatedAt: number
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
  /** Gap between firings, in milliseconds. */
  intervalMs: number
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
   */
  claimNextRun(workflowNames: readonly string[], staleBefore?: number): Promise<ClaimedRun | null>
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
   * {@link StorageAdapter.takeEvent} check and this call. Only succeeds if the
   * caller still holds the lease.
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
  /** Fetch a run by ID, or null if not found. */
  getRun(runId: string): Promise<WorkflowRun | null>
  /** List runs in reverse-chronological order (most recent first), optionally filtered. */
  listRuns(filter?: ListRunsFilter): Promise<WorkflowRun[]>
  /**
   * Reset a `failed` or `cancelled` run to `pending` so it can be re-executed,
   * discarding any `failed` step results so the failed step runs again.
   * Completed steps are preserved and skipped on replay. Returns false if the
   * run does not exist or is not in a resumable state.
   */
  requeueRun(runId: string): Promise<boolean>
  /** Fetch all step results for a run, ordered by creation time. */
  getStepResults(runId: string): Promise<StepResult[]>
  /** Persist a step result. If `leaseId` is provided, fails when the lease is no longer held. */
  saveStepResult(result: StepResult, leaseId?: string): Promise<boolean>
  /** Update run status without a lease check (used for cancellation). Returns false if the run does not exist. */
  updateRunStatus(runId: string, status: RunStatus): Promise<boolean>
  /** Update run status only if the caller still holds the lease. */
  updateClaimedRunStatus(runId: string, leaseId: string, status: RunStatus): Promise<boolean>
  /**
   * Register a recurring schedule, or update the existing one with the same
   * `key`. Returns the stored schedule.
   *
   * Re-registering must **preserve `nextRunAt`** when the interval is unchanged,
   * so a restarting process rejoins the existing cadence instead of pushing the
   * next firing out by a full interval every deploy. When the interval does
   * change, the supplied `nextRunAt` takes effect.
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
