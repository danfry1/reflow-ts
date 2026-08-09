import { LeaseExpiredError } from '../errors'
import type { UnitExecutor, UnitOutcome } from './types'

/**
 * Executes a `.sleep()`.
 *
 * The wake target is persisted on the step row the first time the sleep is
 * reached, so a resumed run sleeps until the *original* deadline rather than
 * restarting the clock — a run that crashes and is reclaimed ten times still
 * waits the duration the workflow asked for, not ten times it.
 */
export const sleepExecutor: UnitExecutor<{ kind: 'sleep'; name: string; durationMs: number }> = {
  async execute(unit, ctx): Promise<UnitOutcome> {
    const existing = ctx.replay.get(unit.name)

    // Already slept on a previous execution — `prev` passes through untouched.
    if (existing?.status === 'completed') {
      return { kind: 'passthrough' }
    }

    const resuming = existing?.status === 'sleeping'
    const wakeAt = resuming ? Number(existing.output) : Date.now() + unit.durationMs

    // A resumed sleep already emitted `stepStart` on the instance that began it.
    if (!resuming) {
      await ctx.emit({
        type: 'stepStart',
        runId: ctx.run.id,
        workflow: ctx.run.workflow,
        stepName: unit.name,
      })
    }

    // The deadline has passed (or the sleep was zero-length) — record it and continue
    // inline rather than making a round trip through storage to wake immediately.
    if (Date.now() >= wakeAt) {
      await ctx.saveStep({ name: unit.name, status: 'completed', output: null, attempts: 0 })
      await ctx.emit({
        type: 'stepComplete',
        runId: ctx.run.id,
        workflow: ctx.run.workflow,
        stepName: unit.name,
        output: null,
        attempts: 0,
      })
      return { kind: 'passthrough' }
    }

    // Not yet time: persist the wake target, then durably suspend and release
    // the lease so the process can exit during the wait.
    await ctx.saveStep({ name: unit.name, status: 'sleeping', output: wakeAt, attempts: 0 })

    const slept = await ctx.storage.sleepRun(ctx.run.id, ctx.run.leaseId, wakeAt)
    if (!slept) {
      throw new LeaseExpiredError(ctx.run.id)
    }

    return { kind: 'suspend' }
  },
}
