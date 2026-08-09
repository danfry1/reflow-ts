/** Lifecycle state of a workflow run. */
export type RunStatus = 'pending' | 'running' | 'sleeping' | 'waiting' | 'completed' | 'failed' | 'cancelled'

/** Lifecycle state of a single step within a run. */
export type StepStatus = 'pending' | 'running' | 'completed' | 'completed-early' | 'sleeping' | 'waiting' | 'failed'

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

/** Result of `storage.createRun()`. `created` is false when an existing idempotent run was returned. */
export interface CreateRunResult {
  run: WorkflowRun
  created: boolean
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
  /** Fetch all step results for a run, ordered by creation time. */
  getStepResults(runId: string): Promise<StepResult[]>
  /** Persist a step result. If `leaseId` is provided, fails when the lease is no longer held. */
  saveStepResult(result: StepResult, leaseId?: string): Promise<boolean>
  /** Update run status without a lease check (used for cancellation). Returns false if the run does not exist. */
  updateRunStatus(runId: string, status: RunStatus): Promise<boolean>
  /** Update run status only if the caller still holds the lease. */
  updateClaimedRunStatus(runId: string, leaseId: string, status: RunStatus): Promise<boolean>
  /** Release resources (e.g. close the database connection). */
  close(): void
}
