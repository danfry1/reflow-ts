import type { StandardSchemaV1 } from '@standard-schema/spec'
import { LeaseExpiredError, WaitTimeoutError } from '../errors'
import type { PersistedValue } from '../types'
import type { UnitExecutor, UnitOutcome } from './types'

type WaitUnit = {
  kind: 'waitForEvent'
  name: string
  schema?: StandardSchemaV1<PersistedValue>
  timeoutMs?: number
}

/**
 * Executes a `.waitForEvent()`.
 *
 * Delivery is order-independent: `engine.sendEvent()` buffers the payload
 * durably, so an event that arrives before the run reaches the wait is consumed
 * here rather than lost. The buffered-event check runs before the suspend, and
 * `storage.waitRun()` re-checks it inside the same transaction that parks the
 * run — together those close the window where an event lands between the two.
 */
export const waitForEventExecutor: UnitExecutor<WaitUnit> = {
  async execute(unit, ctx): Promise<UnitOutcome> {
    const existing = ctx.replay.get(unit.name)

    if (existing?.status === 'completed') {
      ctx.steps[unit.name] = structuredClone(existing.output)
      return { kind: 'advance', output: existing.output }
    }

    const waiting = existing?.status === 'waiting'

    // Consume a buffered event if one has been delivered. The payload was
    // validated against the schema in `sendEvent`, so it is used as-is here —
    // re-validating could fail a non-idempotent transform.
    const delivered = await ctx.storage.takeEvent(ctx.run.id, unit.name)
    if (delivered) {
      const payload = delivered.payload

      if (!waiting) {
        await ctx.emit({
          type: 'stepStart',
          runId: ctx.run.id,
          workflow: ctx.run.workflow,
          stepName: unit.name,
        })
      }

      const saved = await ctx.trySaveStep({
        name: unit.name,
        status: 'completed',
        output: payload,
        attempts: 0,
      })

      if (!saved) {
        // Lost the lease after consuming the event — put it back so whichever
        // engine reclaims the run can consume it, then unwind.
        await ctx.storage.deliverEvent(ctx.run.id, unit.name, payload)
        throw new LeaseExpiredError(ctx.run.id)
      }

      await ctx.emit({
        type: 'stepComplete',
        runId: ctx.run.id,
        workflow: ctx.run.workflow,
        stepName: unit.name,
        output: payload,
        attempts: 0,
      })

      ctx.steps[unit.name] = structuredClone(payload)
      return { kind: 'advance', output: payload }
    }

    // No event yet. Resolve the timeout deadline, pinning it to the value
    // persisted on the first wait so reclaiming the run cannot extend it.
    const { timeoutMs } = unit
    const deadline = timeoutMs === undefined
      ? null
      : waiting && typeof existing?.output === 'number'
        ? existing.output
        : Date.now() + timeoutMs

    if (waiting && deadline !== null && timeoutMs !== undefined && Date.now() >= deadline) {
      const error = new WaitTimeoutError(unit.name, timeoutMs)
      await ctx.saveStep({
        name: unit.name,
        status: 'failed',
        output: null,
        error: error.message,
        attempts: 0,
      })
      return { kind: 'failed', stepName: unit.name, error }
    }

    if (!waiting) {
      await ctx.emit({
        type: 'stepStart',
        runId: ctx.run.id,
        workflow: ctx.run.workflow,
        stepName: unit.name,
      })
    }

    await ctx.saveStep({ name: unit.name, status: 'waiting', output: deadline, attempts: 0 })

    const waited = await ctx.storage.waitRun(ctx.run.id, ctx.run.leaseId, unit.name, deadline)
    if (!waited) {
      throw new LeaseExpiredError(ctx.run.id)
    }

    return { kind: 'suspend' }
  },
}
