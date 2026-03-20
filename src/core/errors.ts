import type { PersistedValue } from './types'

/**
 * Typed error hierarchy for Reflow.
 *
 * Every error Reflow throws extends `ReflowError`, so a single
 * `instanceof ReflowError` catch-all is always available. More specific
 * subclasses carry structured context (workflow name, step name, run ID, etc.)
 * so callers never need to parse error messages.
 */

// ---------------------------------------------------------------------------
// Base
// ---------------------------------------------------------------------------

/** Base class for all Reflow errors. */
export class ReflowError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'ReflowError'
  }
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/** Thrown when engine, retry, or schedule configuration is invalid. */
export class ConfigError extends ReflowError {
  constructor(message: string) {
    super(message)
    this.name = 'ConfigError'
  }
}

// ---------------------------------------------------------------------------
// Workflow definition
// ---------------------------------------------------------------------------

/** Thrown when `enqueue()` or `schedule()` references an unregistered workflow name. */
export class WorkflowNotFoundError extends ReflowError {
  constructor(public readonly workflowName: string) {
    super(`Workflow "${workflowName}" not found`)
    this.name = 'WorkflowNotFoundError'
  }
}

/** Thrown when `createEngine()` receives the same workflow name twice. */
export class DuplicateWorkflowError extends ReflowError {
  constructor(public readonly workflowName: string) {
    super(`Workflow "${workflowName}" is registered more than once`)
    this.name = 'DuplicateWorkflowError'
  }
}

/** Thrown when `.step()` reuses a name that already exists in the workflow. */
export class DuplicateStepError extends ReflowError {
  constructor(
    public readonly workflowName: string,
    public readonly stepName: string,
  ) {
    super(`Step "${stepName}" is already defined in workflow "${workflowName}"`)
    this.name = 'DuplicateStepError'
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
    super(message)
    this.name = 'ValidationError'
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
      `Idempotency key "${idempotencyKey}" for workflow "${workflowName}" is already associated with different input`,
    )
    this.name = 'IdempotencyConflictError'
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
  ) {
    super(message)
    this.name = 'SerializationError'
  }
}

// ---------------------------------------------------------------------------
// Run lifecycle
// ---------------------------------------------------------------------------

/** Thrown when a step exceeds its `timeoutMs`. Reaches `onRunFailed`. */
export class StepTimeoutError extends ReflowError {
  constructor(public readonly timeoutMs: number) {
    super(`Step timed out after ${timeoutMs}ms`)
    this.name = 'StepTimeoutError'
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
  constructor(message: string) {
    super(message)
    this.name = 'RunControlError'
  }
}

/** Thrown when a step calls `complete()` to finish the workflow early. */
export class EarlyCompleteError extends RunControlError {
  constructor(public readonly value?: PersistedValue) {
    super('Workflow completed early')
    this.name = 'EarlyCompleteError'
  }
}

/** Thrown when a run is cancelled via `engine.cancel()`. */
export class RunCancelledError extends RunControlError {
  constructor(public readonly runId: string) {
    super(`Run "${runId}" was cancelled`)
    this.name = 'RunCancelledError'
  }
}

/** Thrown when the engine loses its lease on a run (another worker reclaimed it). */
export class LeaseExpiredError extends RunControlError {
  constructor(public readonly runId: string) {
    super(`Run "${runId}" lease expired`)
    this.name = 'LeaseExpiredError'
  }
}
