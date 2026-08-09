/**
 * Shared assertions helpers for the test suite. Not part of the published
 * package — `tsdown` builds only the declared entry points, and this file is
 * excluded from coverage along with the rest of `__tests__`.
 */

import type { StorageAdapter } from '../core/types'

/**
 * Read `items[index]`, failing loudly if it is not there.
 *
 * Index access is `T | undefined` under `noUncheckedIndexedAccess`, and tests
 * read positionally all the time (`steps[0].status`). This narrows with a real
 * runtime check rather than an assertion or a cast, so a wrong-length result
 * fails with the actual length instead of `Cannot read properties of undefined`.
 */
export function at<T>(items: readonly T[], index: number): T {
  const item = items[index]

  if (item === undefined) {
    throw new Error(
      `Expected an element at index ${index}, but the collection has ${items.length}`,
    )
  }

  return item
}

/**
 * A complete `StorageAdapter` delegating to `delegate`, with `overrides` applied.
 *
 * Tests that need one flaky or instrumented method should not have to restate
 * the other thirteen — and cannot simply spread the instance, because
 * `MemoryStorage`'s methods live on the prototype, so `{ ...instance }` would
 * copy its internal maps and none of its behaviour.
 */
export function delegatingAdapter(
  delegate: StorageAdapter,
  overrides: Partial<StorageAdapter> = {},
): StorageAdapter {
  const base: StorageAdapter = {
    initialize: () => delegate.initialize(),
    createRun: (run) => delegate.createRun(run),
    claimNextRun: (names, staleBefore) => delegate.claimNextRun(names, staleBefore),
    heartbeatRun: (runId, leaseId) => delegate.heartbeatRun(runId, leaseId),
    sleepRun: (runId, leaseId, wakeAt) => delegate.sleepRun(runId, leaseId, wakeAt),
    waitRun: (runId, leaseId, name, wakeAt) => delegate.waitRun(runId, leaseId, name, wakeAt),
    deliverEvent: (runId, name, payload) => delegate.deliverEvent(runId, name, payload),
    takeEvent: (runId, name) => delegate.takeEvent(runId, name),
    getRun: (runId) => delegate.getRun(runId),
    listRuns: (filter) => delegate.listRuns(filter),
    requeueRun: (runId) => delegate.requeueRun(runId),
    getStepResults: (runId) => delegate.getStepResults(runId),
    saveStepResult: (result, leaseId) => delegate.saveStepResult(result, leaseId),
    updateRunStatus: (runId, status) => delegate.updateRunStatus(runId, status),
    updateClaimedRunStatus: (runId, leaseId, status) =>
      delegate.updateClaimedRunStatus(runId, leaseId, status),
    upsertSchedule: (schedule) => delegate.upsertSchedule(schedule),
    claimDueSchedule: (names, now) => delegate.claimDueSchedule(names, now),
    deleteSchedule: (key) => delegate.deleteSchedule(key),
    listSchedules: () => delegate.listSchedules(),
    close: () => delegate.close(),
  }

  return { ...base, ...overrides }
}
