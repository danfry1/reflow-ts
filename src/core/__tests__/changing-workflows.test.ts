import { describe, it, expect } from 'vitest'
import { z } from 'zod'
import { createWorkflow, type AnyWorkflow } from '../workflow'
import { createEngine } from '../engine'
import { MemoryStorage } from '../../storage/memory'

/**
 * What a run part-way through does when the code that resumes it defines its
 * workflow differently. Each case pins a row of the table in
 * website/guide/changing-workflows.md.
 */

/** Start a run under v1, let it reach its pause, then resume it under `v2`. */
async function deployMidRun(v2: (log: string[]) => AnyWorkflow) {
  const storage = new MemoryStorage()
  const log: string[] = []
  const v1 = createWorkflow({ name: 'order', input: z.object({ id: z.string() }) })
    .step('charge', async () => {
      log.push('v1 charge')
      return { chargeId: 'ch_1' }
    })
    .sleep('pause', 20)
    .step('ship', async () => {
      log.push('v1 ship')
      return { tracking: 't' }
    })
  const before = createEngine({ storage, workflows: [v1] })
  const run = await before.enqueue('order', { id: 'A' })
  await before.tick()
  await new Promise((resolve) => setTimeout(resolve, 40))
  await createEngine({ storage, workflows: [v2(log)] }).tick()
  return { log, status: (await storage.getRun(run.id))?.status }
}

describe('changing a workflow with runs in flight', () => {
  it('runs a renamed step again', async () => {
    const result = await deployMidRun((log) => createWorkflow({ name: 'order', input: z.object({ id: z.string() }) })
      .step('take-payment', async () => {
        log.push('v2 take-payment')
        return { chargeId: 'ch_2' }
      })
      .sleep('pause', 20)
      .step('ship', async () => {
        log.push('v2 ship')
      }))
    expect(result).toStrictEqual({ status: 'completed', log: ['v1 charge', 'v2 take-payment', 'v2 ship'] })
  })

  it('runs a step added before the point reached late, after the step it precedes', async () => {
    const result = await deployMidRun((log) => createWorkflow({ name: 'order', input: z.object({ id: z.string() }) })
      .step('validate', async () => {
        log.push('v2 validate')
      })
      .step('charge', async () => {
        log.push('v2 charge')
      })
      .sleep('pause', 20)
      .step('ship', async () => {
        log.push('v2 ship')
      }))
    expect(result).toStrictEqual({ status: 'completed', log: ['v1 charge', 'v2 validate', 'v2 ship'] })
  })

  it('uses changed code for steps not reached, and keeps finished results', async () => {
    const seen: unknown[] = []
    const result = await deployMidRun((log) => createWorkflow({ name: 'order', input: z.object({ id: z.number() }) })
      .step('charge', async () => {
        log.push('v2 charge')
        return { chargeId: 'ch_2' }
      })
      .sleep('pause', 20)
      .step('ship', async ({ prev, input }) => {
        seen.push(prev, input)
        log.push('v2 ship')
      }))
    // The saved charge is reused, and the input is not checked against the new schema.
    expect([result, seen]).toStrictEqual([{ status: 'completed', log: ['v1 charge', 'v2 ship'] }, [{ chargeId: 'ch_1' }, { id: 'A' }]])
  })

  it('skips a removed sleep', async () => {
    const result = await deployMidRun((log) => createWorkflow({ name: 'order', input: z.object({ id: z.string() }) })
      .step('charge', async () => {
        log.push('v2 charge')
      })
      .step('ship', async () => {
        log.push('v2 ship')
      }))
    expect(result).toStrictEqual({ status: 'completed', log: ['v1 charge', 'v2 ship'] })
  })

  it('leaves the runs of a workflow no longer registered where they are', async () => {
    const result = await deployMidRun(() => createWorkflow({ name: 'other', input: z.object({}) }).step('x', async () => {}))
    expect(result).toStrictEqual({ status: 'sleeping', log: ['v1 charge'] })
  })
})
