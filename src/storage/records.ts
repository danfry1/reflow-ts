import { InternalError } from '../core/errors'
import {
  RUN_STATUSES,
  STEP_STATUSES,
  type RunCounts,
  type RunError,
  type RunStatus,
  type StepStatus,
} from '../core/types'

function isOneOf<T extends string>(values: readonly T[], value: string): value is T {
  return (values as readonly string[]).includes(value)
}

/** Narrow a stored run status, refusing anything this version does not know. */
export function parseRunStatus(value: string): RunStatus {
  if (!isOneOf(RUN_STATUSES, value)) {
    throw new InternalError(`Stored run has unknown status "${value}"`)
  }
  return value
}

/** Narrow a stored step status, refusing anything this version does not know. */
export function parseStepStatus(value: string): StepStatus {
  if (!isOneOf(STEP_STATUSES, value)) {
    throw new InternalError(`Stored step has unknown status "${value}"`)
  }
  return value
}

/** Parse a run error stored as JSON, checking its shape rather than trusting it. */
export function parseRunError(serialized: string): RunError {
  const value: unknown = JSON.parse(serialized)
  if (
    typeof value === 'object'
    && value !== null
    && 'stepName' in value
    && typeof value.stepName === 'string'
    && 'message' in value
    && typeof value.message === 'string'
    && 'code' in value
    && (typeof value.code === 'string' || value.code === null)
  ) {
    return { stepName: value.stepName, message: value.message, code: value.code }
  }
  throw new InternalError('Stored run error is malformed')
}

/** A count of zero for every run status. */
export function emptyRunCounts(): RunCounts {
  // Spelled out so the compiler checks it covers every status.
  return { pending: 0, running: 0, sleeping: 0, waiting: 0, completed: 0, failed: 0, cancelled: 0 }
}
