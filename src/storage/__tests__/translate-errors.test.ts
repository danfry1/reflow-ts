import { describe, it, expect } from 'vitest'
import { z } from 'zod'
import { createWorkflow, createEngine, StorageError, LeaseExpiredError } from '../../index'
import type { StorageAdapter } from '../../index'
import { MemoryStorage } from '../memory'
import { translateStorageErrors } from '../translate-errors'

/**
 * A full adapter delegating to `delegate`, with `overrides` applied.
 *
 * Spread cannot be used on `MemoryStorage` directly — its methods live on the
 * prototype, so `{ ...instance }` would copy the internal maps and none of the
 * behaviour, producing an adapter that silently does nothing.
 */
function adapterOver(delegate: MemoryStorage, overrides: Partial<StorageAdapter>): StorageAdapter {
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
    getStepResults: (runId) => delegate.getStepResults(runId),
    saveStepResult: (result, leaseId) => delegate.saveStepResult(result, leaseId),
    updateRunStatus: (runId, status) => delegate.updateRunStatus(runId, status),
    updateClaimedRunStatus: (runId, leaseId, status) =>
      delegate.updateClaimedRunStatus(runId, leaseId, status),
    close: () => delegate.close(),
  }

  return { ...base, ...overrides }
}

/** Capture the error a promise rejects with, failing the test if it resolves. */
async function rejection(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise
  } catch (error) {
    return error
  }
  throw new Error('Expected the promise to reject, but it resolved')
}

// Shaped like a better-sqlite3 throw: a plain Error carrying a driver code.
const driverError = () => Object.assign(new Error('database is locked'), { code: 'SQLITE_BUSY' })

describe('translateStorageErrors', () => {
  it('wraps a foreign driver error, naming the operation', async () => {
    const failure = driverError()
    const delegate = new MemoryStorage()
    const storage = translateStorageErrors(
      adapterOver(delegate, { getRun: () => Promise.reject(failure) }),
    )

    const error = await rejection(storage.getRun('run-1'))

    expect(error).toBeInstanceOf(StorageError)
    expect((error as StorageError).code).toBe('STORAGE')
    expect((error as StorageError).operation).toBe('getRun')
    expect((error as StorageError).cause).toBe(failure)
  })

  it('keeps the driver message, which is what actually gets persisted', () => {
    // A failed step records `error` as a plain string, so a cause chain does not
    // survive storage. The reason has to be in the message or it is lost.
    const error = new StorageError('saveStepResult', { cause: new Error('disk I/O error') })

    expect(error.message).toBe('Storage operation "saveStepResult" failed: disk I/O error')
  })

  it('omits the detail suffix when there is no cause to describe', () => {
    expect(new StorageError('close').message).toBe('Storage operation "close" failed')
  })

  it('passes ReflowErrors through rather than double-wrapping them', async () => {
    const original = new LeaseExpiredError('run-1')
    const delegate = new MemoryStorage()
    const storage = translateStorageErrors(
      adapterOver(delegate, { heartbeatRun: () => Promise.reject(original) }),
    )

    expect(await rejection(storage.heartbeatRun('run-1', 'lease'))).toBe(original)
  })

  it('translates a synchronous throw from close()', () => {
    const delegate = new MemoryStorage()
    const storage = translateStorageErrors(
      adapterOver(delegate, {
        close: () => {
          throw new Error('connection already closed')
        },
      }),
    )

    expect(() => storage.close()).toThrow(StorageError)
    expect(() => storage.close()).toThrow(/connection already closed/)
  })

  it('translates a synchronous throw from an async method', async () => {
    const failure = driverError()
    const delegate = new MemoryStorage()
    const storage = translateStorageErrors(
      adapterOver(delegate, {
        // A driver that throws before returning a promise must still surface as
        // a rejection, not a synchronous throw into the engine's call site.
        createRun: () => {
          throw failure
        },
      }),
    )

    expect(await rejection(storage.createRun({
      id: 'run-1',
      workflow: 'wf',
      input: {},
      idempotencyKey: null,
      status: 'pending',
      createdAt: 0,
      updatedAt: 0,
    }))).toBeInstanceOf(StorageError)
  })

  it('leaves successful calls untouched', async () => {
    const storage = translateStorageErrors(new MemoryStorage())
    await storage.initialize()

    const now = Date.now()
    const { run, created } = await storage.createRun({
      id: 'run-1',
      workflow: 'wf',
      input: { x: 1 },
      idempotencyKey: null,
      status: 'pending',
      createdAt: now,
      updatedAt: now,
    })

    expect(created).toBe(true)
    expect(run.id).toBe('run-1')
    expect(await storage.getRun('run-1')).toMatchObject({ id: 'run-1', status: 'pending' })
  })

  it('surfaces a driver failure through the engine as a StorageError', async () => {
    const delegate = new MemoryStorage()
    await delegate.initialize()

    const wf = createWorkflow({ name: 'wf', input: z.object({}) })
      .step('a', async () => ({ ok: true }))

    const engine = createEngine({
      storage: adapterOver(delegate, { createRun: () => Promise.reject(driverError()) }),
      workflows: [wf],
    })

    // Callers get one catchable type regardless of which SQLite binding is used.
    const error = await rejection(engine.enqueue('wf', {}))

    expect(error).toBeInstanceOf(StorageError)
    expect((error as StorageError).operation).toBe('createRun')
  })
})
