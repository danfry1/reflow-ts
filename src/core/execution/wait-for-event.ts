import type { StandardSchemaV1 } from '@standard-schema/spec'
import { LeaseExpiredError, WaitTimeoutError } from '../errors'
import type { PersistedValue } from '../types'
import type { ExecutionContext, UnitExecutor, UnitOutcome } from './types'

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
      ? { deadline: typeof existing.output === 'number' ? existing.output : null }
      : undefined
    const reached = await reachWait(ctx, unit.name, unit.name, unit.timeoutMs, waiting)

    switch (reached.kind) {
      case 'event':
        ctx.steps[unit.name] = structuredClone(reached.payload)
        return { kind: 'advance', output: reached.payload }
      case 'timeout':
        await ctx.saveStep({
          kind: 'wait',
          name: unit.name,
          status: 'failed',
          output: null,
          error: reached.error.message,
          attempts: 0,
        })
        return { kind: 'failed', stepName: unit.name, error: reached.error }
      case 'pending': {
        const waited = await ctx.storage.waitRun(ctx.run.id, ctx.run.leaseId, unit.name, reached.deadline)
        if (!waited) {
          throw new LeaseExpiredError(ctx.run.id)
        }
        return { kind: 'suspend', wakeAt: reached.deadline }
      }
    }
  },
}

/** How reaching a wait ended: its event arrived, it timed out, or it is still waiting until `deadline`. */
export type WaitReached =
  | { readonly kind: 'event'; readonly payload: PersistedValue }
  | { readonly kind: 'timeout'; readonly error: WaitTimeoutError }
  | { readonly kind: 'pending'; readonly deadline: number | null }

/**
 * Reach the wait `name` for the event `eventName`: consume it if it has been delivered,
 * report a timeout once the deadline has passed, or persist the wait on its
 * row and leave the suspending to the caller. `waiting` carries the deadline
 * pinned by an earlier execution that already began the wait, so reclaiming
 * the run cannot extend it. A timeout is reported, not saved: what it means
 * for the step is the caller's decision.
 */
export async function reachWait(
  ctx: ExecutionContext,
  name: string,
  eventName: string,
  timeoutMs: number | undefined,
  waiting: { readonly deadline: number | null; readonly saved?: boolean } | undefined,
): Promise<WaitReached> {
  // Consume a buffered event if one has been delivered, saving it as this
  // step's result in the same write. The payload was validated against the
  // schema in `sendEvent`, so it is used as-is here — re-validating could
  // fail a non-idempotent transform.
  const consumed = eventName === name ? await ctx.consumeEvent(name) : await ctx.consumeEvent(name, undefined, eventName)
  if (consumed.status === 'lease-lost') {
    // The event, if any, is still buffered for whichever engine holds the run next.
    throw new LeaseExpiredError(ctx.run.id)
  }
  if (consumed.status === 'consumed') {
    if (!waiting) {
      await ctx.emit({ type: 'stepStart', runId: ctx.run.id, workflow: ctx.run.workflow, stepName: name })
    }
    await ctx.emit({
      type: 'stepComplete',
      runId: ctx.run.id,
      workflow: ctx.run.workflow,
      stepName: name,
      output: consumed.payload,
      attempts: 0,
    })
    return { kind: 'event', payload: consumed.payload }
  }

  // No event yet. Resolve the timeout deadline, pinning it to the value
  // persisted on the first wait so reclaiming the run cannot extend it.
  const deadline = timeoutMs === undefined
    ? null
    : waiting && waiting.deadline !== null
      ? waiting.deadline
      : Date.now() + timeoutMs

  if (waiting && deadline !== null && timeoutMs !== undefined && Date.now() >= deadline) {
    return { kind: 'timeout', error: new WaitTimeoutError(eventName, timeoutMs) }
  }

  if (!waiting) {
    await ctx.emit({ type: 'stepStart', runId: ctx.run.id, workflow: ctx.run.workflow, stepName: name })
  }

  // Re-checking a wait already recorded as waiting, in the same execution,
  // leaves its row as it is.
  if (!waiting?.saved) {
    await ctx.saveStep({ kind: 'wait', name, status: 'waiting', output: deadline, attempts: 0 })
  }
  return { kind: 'pending', deadline }
}
