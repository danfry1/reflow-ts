import type { EngineEvent } from '../events'
import type { ClaimedRun, PersistedValue, StepResult, StepStatus, StorageAdapter } from '../types'
import type { AnyWorkflow, ExecutionUnit } from '../workflow'

/**
 * Signal controllers for an in-flight run.
 *
 * `run` aborts the workflow's own work (handlers, retry backoff) on
 * cancellation, engine stop, or lease loss. `observer` aborts only on
 * control-flow events, and gates event delivery to hooks and streams — a
 * backpressured consumer must not be able to wedge a run that is shutting down.
 */
export interface RunSignals {
  readonly run: AbortController
  readonly observer: AbortController
}

/** The fields an executor supplies when persisting a step result. */
export interface StepRecord {
  /** Step name. Also selects the row to reuse on replay. */
  readonly name: string
  readonly status: StepStatus
  readonly output: PersistedValue
  readonly error?: string | null
  readonly attempts?: number
}

/**
 * Everything a {@link UnitExecutor} needs to run one execution unit.
 *
 * Built once per claimed run by the engine and shared across every unit. The
 * only mutable member is {@link ExecutionContext.steps}, which executors extend
 * as they produce output; `prev` is threaded through {@link UnitExecutor.execute}
 * so the orchestration loop stays the single owner of the chaining value.
 */
export interface ExecutionContext {
  readonly run: ClaimedRun
  readonly workflow: AnyWorkflow
  readonly storage: StorageAdapter
  readonly signals: RunSignals

  /**
   * Step results persisted by earlier executions of this run, keyed by step
   * name. Empty on a first execution; populated when a suspended or reclaimed
   * run resumes. Executors consult it to replay rather than repeat work.
   */
  readonly replay: ReadonlyMap<string, StepResult>

  /**
   * Outputs of the steps completed so far in this run, keyed by step name.
   * Executors write their own results here; the engine passes a frozen deep
   * clone to handlers so a handler cannot mutate another step's output.
   */
  readonly steps: Record<string, PersistedValue>

  /**
   * Deliver a lifecycle event to hooks and streams. Rejects only when the
   * observer signal aborts, which the orchestration loop treats as a halt.
   */
  emit(event: EngineEvent): Promise<void>

  /**
   * Persist a step result under the run's lease, reusing the replayed row's
   * `id` and `createdAt` when one exists so a re-execution updates in place
   * instead of appending a second row for the same step name.
   *
   * Throws {@link LeaseExpiredError} if the lease was lost. Use
   * {@link ExecutionContext.trySaveStep} where losing the lease needs handling
   * other than unwinding.
   */
  saveStep(record: StepRecord): Promise<void>

  /** As {@link ExecutionContext.saveStep}, but returns `false` on lease loss instead of throwing. */
  trySaveStep(record: StepRecord): Promise<boolean>

  /** A frozen deep clone of {@link ExecutionContext.steps}, safe to hand to a handler. */
  snapshotSteps(): Readonly<Record<string, PersistedValue>>
}

/**
 * What the orchestration loop should do once a unit finishes.
 *
 * Every execution unit — step, parallel group, sleep, wait — reduces to one of
 * these, which is what keeps `executeRun` a flat loop rather than a tree of
 * per-kind special cases.
 */
export type UnitOutcome =
  /** Continue to the next unit, with `output` becoming the new `prev`. */
  | { readonly kind: 'advance'; readonly output: PersistedValue }
  /** Continue to the next unit, leaving `prev` untouched (a sleep, or a skipped step). */
  | { readonly kind: 'passthrough' }
  /** The run is durably suspended and its lease released. Stop; a later claim resumes it. */
  | { readonly kind: 'suspend' }
  /** A handler called `complete()`. Finish the run as completed with `output`. */
  | { readonly kind: 'complete'; readonly output: PersistedValue }
  /** The unit failed. Mark the run failed and report `stepName` / `error`. */
  | { readonly kind: 'failed'; readonly stepName: string; readonly error: Error }
  /**
   * A control-flow abort landed mid-unit (cancellation, engine stop, lease
   * loss). The run's status is deliberately left as-is so a stopped or
   * lease-lost run stays reclaimable rather than being recorded as failed.
   */
  | { readonly kind: 'halt' }

/** Executes one kind of {@link ExecutionUnit}, reducing it to a {@link UnitOutcome}. */
export interface UnitExecutor<U extends ExecutionUnit> {
  execute(unit: U, ctx: ExecutionContext, prev: PersistedValue): Promise<UnitOutcome>
}
