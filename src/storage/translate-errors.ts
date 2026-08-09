import { ReflowError, StorageError } from '../core/errors'
import type { StorageAdapter } from '../core/types'

/**
 * Wrap a {@link StorageAdapter} so failures surface as {@link StorageError}.
 *
 * Storage drivers are a trust boundary: `better-sqlite3`, `bun:sqlite`, and
 * `node:sqlite` each report the same condition — a busy database, a failed
 * write, a closed connection — with a different error type and a different
 * property carrying the code. Left untranslated, that shape leaks into user
 * code, and callers end up branching per driver to handle one logical failure.
 *
 * Wrapping here rather than inside each adapter means custom adapters get the
 * same guarantee for free. Errors that are already {@link ReflowError}s pass
 * through untouched, so an adapter that reports its own typed failures is not
 * double-wrapped, and control-flow signals are never mistaken for I/O faults.
 *
 * Methods are delegated explicitly rather than via a `Proxy`: the adapter sits
 * on the hot path of every run, and an explicit list means adding a method to
 * `StorageAdapter` is a compile error here instead of a silently untranslated
 * call.
 */
export function translateStorageErrors(storage: StorageAdapter): StorageAdapter {
  return {
    initialize: () => guard('initialize', () => storage.initialize()),
    createRun: (run) => guard('createRun', () => storage.createRun(run)),
    claimNextRun: (names, staleBefore) =>
      guard('claimNextRun', () => storage.claimNextRun(names, staleBefore)),
    heartbeatRun: (runId, leaseId) =>
      guard('heartbeatRun', () => storage.heartbeatRun(runId, leaseId)),
    sleepRun: (runId, leaseId, wakeAt) =>
      guard('sleepRun', () => storage.sleepRun(runId, leaseId, wakeAt)),
    waitRun: (runId, leaseId, eventName, wakeAt) =>
      guard('waitRun', () => storage.waitRun(runId, leaseId, eventName, wakeAt)),
    deliverEvent: (runId, eventName, payload) =>
      guard('deliverEvent', () => storage.deliverEvent(runId, eventName, payload)),
    takeEvent: (runId, eventName) =>
      guard('takeEvent', () => storage.takeEvent(runId, eventName)),
    getRun: (runId) => guard('getRun', () => storage.getRun(runId)),
    getStepResults: (runId) => guard('getStepResults', () => storage.getStepResults(runId)),
    saveStepResult: (result, leaseId) =>
      guard('saveStepResult', () => storage.saveStepResult(result, leaseId)),
    updateRunStatus: (runId, status) =>
      guard('updateRunStatus', () => storage.updateRunStatus(runId, status)),
    updateClaimedRunStatus: (runId, leaseId, status) =>
      guard('updateClaimedRunStatus', () => storage.updateClaimedRunStatus(runId, leaseId, status)),
    upsertSchedule: (schedule) => guard('upsertSchedule', () => storage.upsertSchedule(schedule)),
    claimDueSchedule: (names, now) =>
      guard('claimDueSchedule', () => storage.claimDueSchedule(names, now)),
    deleteSchedule: (key) => guard('deleteSchedule', () => storage.deleteSchedule(key)),
    listSchedules: () => guard('listSchedules', () => storage.listSchedules()),
    close: () => guardSync('close', () => storage.close()),
  }
}

/** Run an async storage call, translating a foreign throw or rejection. */
async function guard<T>(operation: string, call: () => Promise<T>): Promise<T> {
  try {
    return await call()
  } catch (error) {
    throw translate(operation, error)
  }
}

/** Run a synchronous storage call, translating a foreign throw. */
function guardSync<T>(operation: string, call: () => T): T {
  try {
    return call()
  } catch (error) {
    throw translate(operation, error)
  }
}

function translate(operation: string, error: unknown): unknown {
  // An adapter reporting its own typed failure already speaks the caller's
  // language — re-wrapping would bury the discriminant it chose.
  if (error instanceof ReflowError) {
    return error
  }

  return new StorageError(operation, { cause: error })
}
