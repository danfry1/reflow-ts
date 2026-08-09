import type { PersistedValue } from './types'

/**
 * Typed error hierarchy for Reflow.
 *
 * Every error Reflow throws extends {@link ReflowError}, so a single
 * `instanceof ReflowError` catch-all is always available, and carries a stable
 * literal {@link ReflowErrorCode} in `code`. Prefer switching on `code` over
 * `instanceof` or message matching: it is a closed union, so a `switch` over it
 * can be checked for exhaustiveness and survives bundling, `instanceof` across
 * realms, and message rewording.
 *
 * Structured context lives in typed fields (`workflowName`, `stepName`,
 * `runId`, …) rather than being interpolated into the message, so callers never
 * need to parse strings.
 */

/**
 * Stable discriminant for every {@link ReflowError}.
 *
 * These strings are part of the public API: they are safe to persist, compare,
 * and branch on, and will not change without a major version bump.
 */
export type ReflowErrorCode =
  | 'CONFIG'
  | 'WORKFLOW_NOT_FOUND'
  | 'DUPLICATE_WORKFLOW'
  | 'DUPLICATE_STEP'
  | 'PARALLEL_COMPLETE'
  | 'VALIDATION'
  | 'IDEMPOTENCY_CONFLICT'
  | 'SERIALIZATION'
  | 'STEP_TIMEOUT'
  | 'WAIT_TIMEOUT'
  | 'STEP_FAILED'
  | 'BRANCH_FAILED'
  | 'HOOK'
  | 'THROWN_VALUE'
  | 'TEST_RUN_INCOMPLETE'
  | 'INTERNAL'
  | 'RUN_CONTROL'
  | 'EARLY_COMPLETE'
  | 'RUN_CANCELLED'
  | 'LEASE_EXPIRED'

/** The JSON shape produced by {@link ReflowError.toJSON}. */
export interface SerializedReflowError {
  readonly name: string
  readonly code: ReflowErrorCode
  readonly message: string
  readonly context: Readonly<Record<string, unknown>>
  readonly cause?: SerializedReflowError | { readonly name: string; readonly message: string }
}

// ---------------------------------------------------------------------------
// Base
// ---------------------------------------------------------------------------

/** Base class for all Reflow errors. */
export class ReflowError extends Error {
  /** Stable, machine-readable discriminant. See {@link ReflowErrorCode}. */
  readonly code: ReflowErrorCode

  constructor(code: ReflowErrorCode, message: string, options?: { cause?: unknown }) {
    super(message, options)
    this.code = code
    this.name = 'ReflowError'
    // Restore the prototype chain so `instanceof` holds for subclasses even
    // when a consumer downlevels this code below ES2015.
    Object.setPrototypeOf(this, new.target.prototype)
  }

  /**
   * The structured context this error carries, as a plain object.
   *
   * Subclasses override this to expose their typed fields. Used by
   * {@link ReflowError.toJSON}; keep it free of secrets, since the result is
   * intended to be logged.
   */
  protected context(): Readonly<Record<string, unknown>> {
    return {}
  }

  /** Render the error as plain JSON — discriminant, context, and flattened cause. */
  toJSON(): SerializedReflowError {
    const serialized: SerializedReflowError = {
      name: this.name,
      code: this.code,
      message: this.message,
      context: this.context(),
    }

    const { cause } = this
    if (cause === undefined) {
      return serialized
    }

    return {
      ...serialized,
      cause: cause instanceof ReflowError
        ? cause.toJSON()
        : cause instanceof Error
          ? { name: cause.name, message: cause.message }
          : { name: 'UnknownError', message: String(cause) },
    }
  }
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/** Thrown when engine, retry, or schedule configuration is invalid. */
export class ConfigError extends ReflowError {
  constructor(message: string, options?: { cause?: unknown }) {
    super('CONFIG', message, options)
    this.name = 'ConfigError'
  }
}

// ---------------------------------------------------------------------------
// Workflow definition
// ---------------------------------------------------------------------------

/** Thrown when `enqueue()` or `schedule()` references an unregistered workflow name. */
export class WorkflowNotFoundError extends ReflowError {
  constructor(public readonly workflowName: string) {
    super('WORKFLOW_NOT_FOUND', `Workflow "${workflowName}" not found`)
    this.name = 'WorkflowNotFoundError'
  }

  protected override context() {
    return { workflowName: this.workflowName }
  }
}

/** Thrown when `createEngine()` receives the same workflow name twice. */
export class DuplicateWorkflowError extends ReflowError {
  constructor(public readonly workflowName: string) {
    super('DUPLICATE_WORKFLOW', `Workflow "${workflowName}" is registered more than once`)
    this.name = 'DuplicateWorkflowError'
  }

  protected override context() {
    return { workflowName: this.workflowName }
  }
}

/** Thrown when `.step()` reuses a name that already exists in the workflow. */
export class DuplicateStepError extends ReflowError {
  constructor(
    public readonly workflowName: string,
    public readonly stepName: string,
  ) {
    super('DUPLICATE_STEP', `Step "${stepName}" is already defined in workflow "${workflowName}"`)
    this.name = 'DuplicateStepError'
  }

  protected override context() {
    return { workflowName: this.workflowName, stepName: this.stepName }
  }
}

/** Thrown when `complete()` is called inside a parallel step handler. */
export class ParallelCompleteError extends ReflowError {
  constructor(public readonly stepName: string) {
    super('PARALLEL_COMPLETE', `complete() cannot be called inside parallel step "${stepName}"`)
    this.name = 'ParallelCompleteError'
  }

  protected override context() {
    return { stepName: this.stepName }
  }
}

// ---------------------------------------------------------------------------
// Input validation
// ---------------------------------------------------------------------------

/** A single validation issue from the input schema. */
export interface ValidationIssue {
  readonly message: string
  readonly path?: ReadonlyArray<PropertyKey | { readonly key: PropertyKey }>
}

/** Thrown when workflow input fails schema validation. */
export class ValidationError extends ReflowError {
  constructor(
    message: string,
    public readonly issues: readonly ValidationIssue[],
  ) {
    super('VALIDATION', message)
    this.name = 'ValidationError'
  }

  protected override context() {
    return { issues: this.issues }
  }
}

// ---------------------------------------------------------------------------
// Idempotency
// ---------------------------------------------------------------------------

/** Thrown when `enqueue()` reuses an idempotency key with different input. */
export class IdempotencyConflictError extends ReflowError {
  constructor(
    public readonly workflowName: string,
    public readonly idempotencyKey: string,
  ) {
    super(
      'IDEMPOTENCY_CONFLICT',
      `Idempotency key "${idempotencyKey}" for workflow "${workflowName}" is already associated with different input`,
    )
    this.name = 'IdempotencyConflictError'
  }

  protected override context() {
    return { workflowName: this.workflowName, idempotencyKey: this.idempotencyKey }
  }
}

// ---------------------------------------------------------------------------
// Serialization
// ---------------------------------------------------------------------------

/** Thrown when a step output or workflow input contains non-JSON-compatible data. */
export class SerializationError extends ReflowError {
  constructor(
    message: string,
    public readonly path: string,
    options?: { cause?: unknown },
  ) {
    super('SERIALIZATION', message, options)
    this.name = 'SerializationError'
  }

  protected override context() {
    return { path: this.path }
  }
}

// ---------------------------------------------------------------------------
// Run lifecycle
// ---------------------------------------------------------------------------

/** Thrown when a step exceeds its `timeoutMs`. Reaches `onRunFailed`. */
export class StepTimeoutError extends ReflowError {
  constructor(public readonly timeoutMs: number) {
    super('STEP_TIMEOUT', `Step timed out after ${timeoutMs}ms`)
    this.name = 'StepTimeoutError'
  }

  protected override context() {
    return { timeoutMs: this.timeoutMs }
  }
}

/** Thrown when a `waitForEvent` step's `timeoutMs` elapses before the event is delivered. Reaches `onRunFailed`. */
export class WaitTimeoutError extends ReflowError {
  constructor(
    public readonly eventName: string,
    public readonly timeoutMs: number,
  ) {
    super('WAIT_TIMEOUT', `Timed out after ${timeoutMs}ms waiting for event "${eventName}"`)
    this.name = 'WaitTimeoutError'
  }

  protected override context() {
    return { eventName: this.eventName, timeoutMs: this.timeoutMs }
  }
}

/**
 * Thrown when a step exhausts its retries without the handler surfacing an
 * error of its own — the run signal aborted before any attempt could run.
 * Reaches `onRunFailed`.
 */
export class StepFailedError extends ReflowError {
  constructor(
    public readonly stepName: string,
    public readonly attempts: number,
    options?: { cause?: unknown },
  ) {
    super('STEP_FAILED', `Step "${stepName}" failed after ${attempts} attempt(s)`, options)
    this.name = 'StepFailedError'
  }

  protected override context() {
    return { stepName: this.stepName, attempts: this.attempts }
  }
}

/**
 * Carries branch metadata out through a rejected branch promise so a parallel
 * group can report which branch failed.
 *
 * @internal Not exported from the public API.
 */
export class BranchFailedError extends ReflowError {
  constructor(
    public readonly branchName: string,
    public readonly branchError: Error,
    public readonly attempts: number,
  ) {
    super('BRANCH_FAILED', branchError.message, { cause: branchError })
    this.name = 'BranchFailedError'
  }

  protected override context() {
    return { branchName: this.branchName, attempts: this.attempts }
  }
}

/**
 * Wraps a non-`Error` value thrown by user code (`throw 'boom'`, `throw 42`).
 *
 * The message is the value's `String()` form, so it reads exactly as thrown,
 * and the original value is retained on both `cause` and {@link ThrownValueError.value}
 * so a thrown object is not flattened to `[object Object]` beyond recovery.
 */
export class ThrownValueError extends ReflowError {
  constructor(public readonly value: unknown) {
    super('THROWN_VALUE', String(value), { cause: value })
    this.name = 'ThrownValueError'
  }

  protected override context() {
    return { value: this.value }
  }
}

/**
 * Wraps an error thrown by user-supplied observer code — a lifecycle hook, a
 * stream consumer, or an `onFailure` handler.
 *
 * Observers must not be able to change a run's outcome, so these never
 * propagate into engine state. They are delivered to `onError` instead, with
 * the original throw preserved as `cause`, so a broken hook is diagnosable
 * rather than silently dropped.
 */
export class HookError extends ReflowError {
  constructor(
    public readonly source: string,
    options?: { cause?: unknown },
  ) {
    super('HOOK', `${source} threw`, options)
    this.name = 'HookError'
  }

  protected override context() {
    return { source: this.source }
  }
}

/**
 * Thrown by the `testEngine` helper when a run does not reach a terminal state
 * within its single `tick()`.
 *
 * The usual cause is a workflow that suspends — `.sleep()` or
 * `.waitForEvent()` release the lease and park the run, which by design needs a
 * later tick (and, for an event, a `sendEvent`) to resume. Drive those with a
 * full `createEngine` instead.
 */
export class TestRunIncompleteError extends ReflowError {
  constructor(
    public readonly runId: string,
    public readonly status: string,
  ) {
    super(
      'TEST_RUN_INCOMPLETE',
      `Run "${runId}" is "${status}" after tick() rather than completed or failed. ` +
        'Workflows that suspend (.sleep() / .waitForEvent()) need more than the single tick ' +
        'testEngine performs — drive them with createEngine instead.',
    )
    this.name = 'TestRunIncompleteError'
  }

  protected override context() {
    return { runId: this.runId, status: this.status }
  }
}

/**
 * Thrown when an internal invariant is violated. Always a bug in Reflow itself
 * rather than a condition user code can provoke or recover from.
 */
export class InternalError extends ReflowError {
  constructor(message: string, options?: { cause?: unknown }) {
    super('INTERNAL', `${message} — this is a bug in reflow-ts, please report it`, options)
    this.name = 'InternalError'
  }
}

/**
 * Internal base class for errors that represent control-flow signals
 * (cancellation, lease loss) rather than real failures. These do NOT
 * reach `onRunFailed`.
 *
 * @internal Not exported from the public API.
 */
export class RunControlError extends ReflowError {
  constructor(message: string, code: ReflowErrorCode = 'RUN_CONTROL') {
    super(code, message)
    this.name = 'RunControlError'
  }
}

/** Thrown when a step calls `complete()` to finish the workflow early. */
export class EarlyCompleteError extends RunControlError {
  constructor(public readonly value?: PersistedValue) {
    super('Workflow completed early', 'EARLY_COMPLETE')
    this.name = 'EarlyCompleteError'
  }
}

/** Thrown when a run is cancelled via `engine.cancel()`. */
export class RunCancelledError extends RunControlError {
  constructor(public readonly runId: string) {
    super(`Run "${runId}" was cancelled`, 'RUN_CANCELLED')
    this.name = 'RunCancelledError'
  }

  protected override context() {
    return { runId: this.runId }
  }
}

/** Thrown when the engine loses its lease on a run (another worker reclaimed it). */
export class LeaseExpiredError extends RunControlError {
  constructor(public readonly runId: string) {
    super(`Run "${runId}" lease expired`, 'LEASE_EXPIRED')
    this.name = 'LeaseExpiredError'
  }

  protected override context() {
    return { runId: this.runId }
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Normalize an unknown throwable into an `Error`.
 *
 * The single place non-`Error` throwables are converted, so the original value
 * is always retained on `cause` rather than being reduced to a string and lost.
 * The message is left as the value's own `String()` form — throwing a non-Error
 * is legal in user code, so this reports what was thrown rather than
 * editorialising about it.
 */
export function toError(value: unknown): Error {
  if (value instanceof Error) {
    return value
  }

  return new ThrownValueError(value)
}

/**
 * Assert that a value is `never`, proving a switch or union is exhaustive.
 *
 * Adding a variant to the union turns every call site into a compile error,
 * which is the point — the runtime throw is only reachable if the type was
 * subverted at a boundary.
 */
export function assertNever(value: never, context: string): never {
  throw new InternalError(`Unhandled ${context}: ${JSON.stringify(value)}`)
}
