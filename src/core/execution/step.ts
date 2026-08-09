import { ConfigError, EarlyCompleteError, RunControlError } from '../errors'
import type { PersistedValue } from '../types'
import type { StepDefinition } from '../workflow'
import { createAttemptSignal, delayWithSignal, runWithSignal, toError } from './signals'
import type { ExecutionContext, UnitExecutor, UnitOutcome } from './types'

/** The result of running a step handler to exhaustion (success, early completion, or all retries spent). */
export type StepOutcome =
  | { kind: 'completed'; output: PersistedValue; attempts: number }
  | { kind: 'early-complete'; output: PersistedValue; attempts: number }
  | { kind: 'failed'; error: Error; attempts: number }

/** Everything {@link runStepHandler} needs, independent of how the step is scheduled. */
export interface StepAttemptContext {
  readonly input: PersistedValue
  readonly runSignal: AbortSignal
}

/**
 * Run a step handler, retrying per its {@link RetryConfig} until it succeeds,
 * calls `complete()`, or exhausts its attempts.
 *
 * Shared by the sequential step executor and by each branch of a parallel
 * group — the two differ in how they schedule steps and interpret the result,
 * not in how a single step's attempts are driven.
 */
export async function runStepHandler(
  stepDef: StepDefinition,
  attempt: StepAttemptContext,
  prev: PersistedValue,
  steps: Readonly<Record<string, PersistedValue>>,
): Promise<StepOutcome> {
  const maxAttempts = stepDef.retry?.maxAttempts ?? 1
  if (maxAttempts < 1) {
    throw new ConfigError(`Step "${stepDef.name}" retry maxAttempts must be at least 1`)
  }

  const backoff = stepDef.retry?.backoff ?? 'linear'
  const initialDelay = stepDef.retry?.initialDelayMs ?? 1000
  const timeoutMs = stepDef.timeoutMs ?? stepDef.retry?.timeoutMs

  let lastError: Error | null = null

  for (let attemptNumber = 1; attemptNumber <= maxAttempts; attemptNumber++) {
    // Stop retrying once the run signal is aborted. A handler is not re-entered
    // (runWithSignal short-circuits), but iterating just to reject again wastes
    // work and inflates the reported attempt count.
    if (attempt.runSignal.aborted) {
      break
    }

    const attemptSignal = createAttemptSignal(attempt.runSignal, timeoutMs)

    try {
      const complete = (value?: PersistedValue): never => {
        throw new EarlyCompleteError(value)
      }

      const rawOutput = await runWithSignal(
        () => stepDef.handler({
          input: attempt.input,
          prev,
          signal: attemptSignal.signal,
          complete,
          steps,
        }),
        attemptSignal.signal,
      )

      // `void` and `undefined` coincide at runtime; the cast narrows the
      // handler's `PersistedValue | void` return without touching the value, so
      // a handler that returns `null` still records `null` rather than `undefined`.
      return { kind: 'completed', output: rawOutput as PersistedValue, attempts: attemptNumber }
    } catch (error) {
      if (error instanceof EarlyCompleteError) {
        return { kind: 'early-complete', output: error.value, attempts: attemptNumber }
      }

      const err = toError(error)

      // Control-flow aborts are not failures and must not be retried.
      if (err instanceof RunControlError) {
        throw err
      }

      lastError = err

      if (attemptNumber < maxAttempts) {
        const delay = backoff === 'exponential'
          ? initialDelay * Math.pow(2, attemptNumber - 1)
          : initialDelay * attemptNumber

        if (delay > 0) {
          await delayWithSignal(delay, attempt.runSignal)
        }
      }
    } finally {
      attemptSignal.cleanup()
    }
  }

  // RunControlError is rethrown inside the loop, so reaching here means a plain
  // failure — or a null `lastError` when the signal aborted before any attempt.
  return { kind: 'failed', error: lastError ?? new Error('Unknown error'), attempts: maxAttempts }
}

/** Executes a sequential `.step()`. */
export const stepExecutor: UnitExecutor<{ kind: 'step'; definition: StepDefinition }> = {
  async execute(unit, ctx, prev): Promise<UnitOutcome> {
    const stepDef = unit.definition
    const existing = ctx.replay.get(stepDef.name)

    // This step called complete() on a previous execution — finish the run
    // without re-running anything after it.
    if (existing?.status === 'completed-early') {
      await ctx.emit({
        type: 'stepComplete',
        runId: ctx.run.id,
        workflow: ctx.run.workflow,
        stepName: stepDef.name,
        output: existing.output,
        attempts: existing.attempts,
      })
      return { kind: 'complete', output: existing.output }
    }

    if (existing?.status === 'completed') {
      ctx.steps[stepDef.name] = structuredClone(existing.output)
      return { kind: 'advance', output: existing.output }
    }

    // Skipped on a previous execution — the decision is persisted, so `when` is
    // not re-evaluated (it may read state that has since changed).
    if (existing?.status === 'skipped') {
      return { kind: 'passthrough' }
    }

    const frozenSteps = ctx.snapshotSteps()

    try {
      if (stepDef.when) {
        const shouldRun = await stepDef.when({ input: ctx.run.input, prev, steps: frozenSteps })

        if (!shouldRun) {
          // Persist the skip before announcing it, so a crash between the two
          // replays as a skip rather than re-evaluating the predicate.
          await ctx.saveStep({ name: stepDef.name, status: 'skipped', output: null, attempts: 0 })

          await ctx.emit({
            type: 'stepSkipped',
            runId: ctx.run.id,
            workflow: ctx.run.workflow,
            stepName: stepDef.name,
          })

          return { kind: 'passthrough' }
        }
      }

      await ctx.emit({
        type: 'stepStart',
        runId: ctx.run.id,
        workflow: ctx.run.workflow,
        stepName: stepDef.name,
      })

      const outcome = await runStepHandler(
        stepDef,
        { input: ctx.run.input, runSignal: ctx.signals.run.signal },
        prev,
        frozenSteps,
      )

      if (outcome.kind === 'failed') {
        // A control-flow abort (cancel / engine stop / lease loss) can land on
        // an await point between steps — including a backpressured stream emit.
        // When it does, the handler reports a synthetic failure for a step that
        // never really ran. That is a side effect of the abort, not a real
        // failure, so leave the run untouched and reclaimable. A non-control
        // abort reason (e.g. a heartbeat storage error) is a genuine failure and
        // still falls through.
        if (isControlAbort(ctx)) {
          return { kind: 'halt' }
        }

        await ctx.saveStep({
          name: stepDef.name,
          status: 'failed',
          output: null,
          error: outcome.error.message,
          attempts: outcome.attempts,
        })

        return { kind: 'failed', stepName: stepDef.name, error: outcome.error }
      }

      await ctx.saveStep({
        name: stepDef.name,
        status: outcome.kind === 'early-complete' ? 'completed-early' : 'completed',
        output: outcome.output,
        attempts: outcome.attempts,
      })

      await ctx.emit({
        type: 'stepComplete',
        runId: ctx.run.id,
        workflow: ctx.run.workflow,
        stepName: stepDef.name,
        output: outcome.output,
        attempts: outcome.attempts,
      })

      if (outcome.kind === 'early-complete') {
        return { kind: 'complete', output: outcome.output }
      }

      ctx.steps[stepDef.name] = structuredClone(outcome.output)
      return { kind: 'advance', output: outcome.output }
    } catch (error) {
      const err = toError(error)

      if (err instanceof EarlyCompleteError) {
        throw new Error(`EarlyCompleteError escaped runStepHandler for step "${stepDef.name}"`)
      }

      // Cancellation, engine stop, lease loss (including a failed lease-checked
      // write): leave the run's status alone so it stays reclaimable.
      if (err instanceof RunControlError) {
        return { kind: 'halt' }
      }

      if (ctx.signals.run.signal.aborted) {
        const latest = await ctx.storage.getRun(ctx.run.id)
        if (!latest || latest.status === 'cancelled') {
          return { kind: 'halt' }
        }
      }

      return { kind: 'failed', stepName: stepDef.name, error: err }
    }
  },
}

/** True when the run signal aborted for a control-flow reason rather than a genuine error. */
function isControlAbort(ctx: ExecutionContext): boolean {
  return (
    ctx.signals.run.signal.aborted &&
    ctx.signals.run.signal.reason instanceof RunControlError
  )
}
