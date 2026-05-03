import { describe, it, expect, vi, beforeEach } from 'vitest'
import { z } from 'zod'
import { createWorkflow } from '../workflow'
import { _createEngine as createEngine } from '../engine'
import { MemoryStorage } from '../../storage/memory'

describe('async hooks', () => {
  let storage: MemoryStorage

  beforeEach(async () => {
    storage = new MemoryStorage()
    await storage.initialize()
  })

  it('an async onStepComplete hook is awaited before tick() resolves', async () => {
    let hookCompleted = false

    const wf = createWorkflow({ name: 'async-step', input: z.object({}) })
      .step('a', async () => ({ x: 1 }))

    const engine = createEngine({
      storage,
      workflows: [wf],
      hooks: {
        onStepComplete: async () => {
          await new Promise<void>((resolve) => setTimeout(resolve, 10))
          hookCompleted = true
        },
      },
    })

    await engine.enqueue('async-step', {})
    await engine.tick()

    expect(hookCompleted).toBe(true)
  })

  it('an async onRunComplete hook is awaited before tick() resolves', async () => {
    let hookCompleted = false

    const wf = createWorkflow({ name: 'async-run', input: z.object({}) })
      .step('a', async () => ({}))

    const engine = createEngine({
      storage,
      workflows: [wf],
      hooks: {
        onRunComplete: async () => {
          await new Promise<void>((resolve) => setTimeout(resolve, 10))
          hookCompleted = true
        },
      },
    })

    await engine.enqueue('async-run', {})
    await engine.tick()

    expect(hookCompleted).toBe(true)
  })

  it('a synchronously throwing onStepComplete does not crash the engine or fail the run', async () => {
    const wf = createWorkflow({ name: 'sync-throw-step', input: z.object({}) })
      .step('a', async () => ({ ok: true }))

    const engine = createEngine({
      storage,
      workflows: [wf],
      hooks: {
        onStepComplete: () => { throw new Error('sync step hook boom') },
      },
    })

    const run = await engine.enqueue('sync-throw-step', {})
    await expect(engine.tick()).resolves.toBeUndefined()

    const info = await engine.getRunStatus(run.id)
    expect(info?.run.status).toBe('completed')
  })

  it('a rejected-promise onStepComplete does not crash the engine or fail the run', async () => {
    const wf = createWorkflow({ name: 'async-throw-step', input: z.object({}) })
      .step('a', async () => ({ ok: true }))

    const engine = createEngine({
      storage,
      workflows: [wf],
      hooks: {
        onStepComplete: async () => { throw new Error('async step hook boom') },
      },
    })

    const run = await engine.enqueue('async-throw-step', {})
    await expect(engine.tick()).resolves.toBeUndefined()

    const info = await engine.getRunStatus(run.id)
    expect(info?.run.status).toBe('completed')
  })

  it('a synchronously throwing onRunComplete does not crash the engine', async () => {
    const wf = createWorkflow({ name: 'sync-throw-run', input: z.object({}) })
      .step('a', async () => ({}))

    const engine = createEngine({
      storage,
      workflows: [wf],
      hooks: {
        onRunComplete: () => { throw new Error('sync run hook boom') },
      },
    })

    const run = await engine.enqueue('sync-throw-run', {})
    await expect(engine.tick()).resolves.toBeUndefined()

    const info = await engine.getRunStatus(run.id)
    expect(info?.run.status).toBe('completed')
  })

  it('a rejected-promise onRunFailed does not crash the engine', async () => {
    const wf = createWorkflow({ name: 'async-throw-failed', input: z.object({}) })
      .step('broken', async () => { throw new Error('step error') })

    const engine = createEngine({
      storage,
      workflows: [wf],
      hooks: {
        onRunFailed: async () => { throw new Error('async failed hook boom') },
      },
    })

    const run = await engine.enqueue('async-throw-failed', {})
    await expect(engine.tick()).resolves.toBeUndefined()

    const info = await engine.getRunStatus(run.id)
    expect(info?.run.status).toBe('failed')
  })

  it('hook errors are logged to console.error rather than silently swallowed', async () => {
    const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {})

    const wf = createWorkflow({ name: 'hook-error-logged', input: z.object({}) })
      .step('a', async () => ({}))

    const engine = createEngine({
      storage,
      workflows: [wf],
      hooks: {
        onStepComplete: () => { throw new Error('hook boom') },
      },
    })

    await engine.enqueue('hook-error-logged', {})
    await engine.tick()

    expect(consoleSpy).toHaveBeenCalledWith(
      expect.stringContaining('[reflow-ts]'),
      expect.any(Error),
    )

    consoleSpy.mockRestore()
  })

  it('async onRunStart hook is awaited before step execution begins', async () => {
    const order: string[] = []

    const wf = createWorkflow({ name: 'start-order', input: z.object({}) })
      .step('a', async () => { order.push('step'); return {} })

    const engine = createEngine({
      storage,
      workflows: [wf],
      hooks: {
        onRunStart: async () => {
          await new Promise<void>((resolve) => setTimeout(resolve, 5))
          order.push('onRunStart')
        },
      },
    })

    await engine.enqueue('start-order', {})
    await engine.tick()

    expect(order).toEqual(['onRunStart', 'step'])
  })
})
