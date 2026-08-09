import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { z } from 'zod'
import { createWorkflow, createEngine } from '../../index'
import { MemoryStorage } from '../../storage/memory'
import { at } from '../../__tests__/helpers'

/**
 * `saveStepResult` upserts by row `id`, so a re-executed step must reuse the
 * row it already persisted. Writing a fresh id instead appends a second row
 * under the same step name, which surfaces to users as a duplicated step in
 * `getRunStatus()`.
 */
describe('step row identity across re-execution', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('reuses the persisted row when a step re-runs after its lease was lost', async () => {
    const wf = createWorkflow({ name: 'reclaim', input: z.object({}) })
      .step('a', async () => ({ ok: true }))

    const storage = new MemoryStorage()
    const engine = createEngine({ storage, workflows: [wf], runLeaseDurationMs: 1000 })
    await storage.initialize()

    // Seed the exact interleaving that leaves a stale `running` run carrying a
    // persisted `failed` step: the step's failure was written under the lease,
    // but the engine died before recording the run itself as failed.
    const now = Date.now()
    await storage.createRun({
      id: 'run-1',
      workflow: 'reclaim',
      input: {},
      idempotencyKey: null,
      status: 'pending',
      createdAt: now,
      updatedAt: now,
    })

    const claimed = await storage.claimNextRun(['reclaim'])
    expect(claimed?.id).toBe('run-1')

    await storage.saveStepResult({
      id: 'step-row-1',
      runId: 'run-1',
      name: 'a',
      status: 'failed',
      output: null,
      error: 'boom',
      attempts: 1,
      createdAt: now,
      updatedAt: now,
    }, claimed?.leaseId)

    // Let the lease go stale so the engine reclaims the run and re-runs `a`.
    await vi.advanceTimersByTimeAsync(2000)
    await engine.tick()

    const info = await engine.getRunStatus('run-1')
    const rowsForA = info?.steps.filter((step) => step.name === 'a') ?? []

    expect(rowsForA).toHaveLength(1)
    expect(at(rowsForA, 0).id).toBe('step-row-1')
    expect(at(rowsForA, 0).status).toBe('completed')
    expect(at(rowsForA, 0).createdAt).toBe(now)
    expect(info?.run.status).toBe('completed')
  })
})
