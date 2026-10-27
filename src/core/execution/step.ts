import {
  ConfigError,
  EarlyCompleteError,
  errorMessage,
  InternalError,
  LeaseExpiredError,
  RunControlError,
  StepFailedError,
} from '../errors'
import { createHash } from 'node:crypto'
import { canonicalizePersistedValue } from '../../storage/codec'
import type { CachedStep, PersistedValue, StepAttempt } from '../types'
import type { StepCacheDefinition, StepDefinition } from '../workflow'
import { createAttemptSignal, delayWithSignal, runWithSignal, toError } from './signals'
import type { ExecutionContext, UnitExecutor, UnitOutcome } from './types'

/**
 * How a run of a step handler ended: success, early completion, all attempts
 * spent, or a failure with attempts left and a backoff to wait out first.
 * `history` holds the attempts this call made, not those of earlier executions.
 */
export type StepOutcome =
  | { kind: 'completed'; output: PersistedValue; attempts: number; history: StepAttempt[] }
  | { kind: 'early-complete'; output: PersistedValue; attempts: number; history: StepAttempt[] }
  | { kind: 'failed'; error: Error; attempts: number; history: StepAttempt[] }
  /** Attempt `attempts` failed; the next may start at `retryAt` (epoch ms). */
  | { kind: 'retry'; error: Error; attempts: number; retryAt: number; history: StepAttempt[] }

/** Everything {@link runStepHandler} needs, independent of how the step is scheduled. */
export interface StepAttemptContext {
  readonly runId: string
  readonly input: PersistedValue
  readonly runSignal: AbortSignal
  /** Attempts earlier executions already spent on this step. Numbering continues from here. */
  readonly priorAttempts?: number
  /** The error the last of those attempts failed with, as persisted. */
  readonly priorError?: string
}

/**
 * Run a step handler's attempts until it succeeds, calls `complete()`,
 * exhausts its attempts, or fails with a backoff delay ahead of the next one.
 *
 * It never waits out a backoff itself: it returns a `retry` outcome and the
 * caller decides how to wait. The sequential executor suspends the run
 * durably, so the wait holds no lease or slot and survives a crash; a parallel
 * branch, whose siblings are still running, waits in process. Zero-delay
 * retries run straight through.
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
  const priorAttempts = attempt.priorAttempts ?? 0
  const history: StepAttempt[] = []

  for (let attemptNumber = priorAttempts + 1; attemptNumber <= maxAttempts; attemptNumber++) {
    // Stop retrying once the run signal is aborted. A handler is not re-entered
    // (runWithSignal short-circuits), but iterating just to reject again wastes
    // work and inflates the reported attempt count.
    if (attempt.runSignal.aborted) {
      break
    }

    const attemptSignal = createAttemptSignal(attempt.runSignal, timeoutMs)
    const startedAt = Date.now()

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
          runId: attempt.runId,
          attempt: attemptNumber,
          idempotencyKey: `${attempt.runId}:${stepDef.name}`,
        }),
        attemptSignal.signal,
      )

      // `void` and `undefined` coincide at runtime; the cast narrows the
      // handler's `PersistedValue | void` return without touching the value, so
      // a handler that returns `null` still records `null` rather than `undefined`.
      history.push({ startedAt, finishedAt: Date.now(), error: null })
      return { kind: 'completed', output: rawOutput as PersistedValue, attempts: attemptNumber, history }
    } catch (error) {
      if (error instanceof EarlyCompleteError) {
        history.push({ startedAt, finishedAt: Date.now(), error: null })
        return { kind: 'early-complete', output: error.value, attempts: attemptNumber, history }
      }

      const err = toError(error)

      // Control-flow aborts are not failures and must not be retried. One the
      // handler threw of its own accord, with nothing aborted, is a failure.
      if (err instanceof RunControlError && attemptSignal.signal.aborted) {
        throw err
      }

      lastError = err
      history.push({ startedAt, finishedAt: Date.now(), error: errorMessage(err) })

      if (attemptNumber < maxAttempts) {
        const delay = backoff === 'exponential'
          ? initialDelay * Math.pow(2, attemptNumber - 1)
          : initialDelay * attemptNumber

        if (delay > 0) {
          return { kind: 'retry', error: err, attempts: attemptNumber, retryAt: Date.now() + delay, history }
        }
      }
    } finally {
      attemptSignal.cleanup()
    }
  }

  // RunControlError is rethrown inside the loop, so reaching here means a plain
  // failure — or a null `lastError` when the signal aborted before any attempt
  // could run, which needs an error of its own to report.
  // No attempt ran: either the signal aborted first, or earlier executions had
  // already spent every attempt (the workflow now allows fewer). In the second
  // case the persisted error is the one that explains the failure. Either
  // way the count is the attempts actually made, not the ones allowed.
  const attempts = priorAttempts + history.length
  return {
    kind: 'failed',
    error: lastError ?? new StepFailedError(stepDef.name, attempts, { lastError: attempt.priorError }),
    attempts,
    history,
  }
}

/**
 * Run a step handler to its end, waiting out each backoff in process.
 *
 * For a parallel branch, which cannot suspend the run on its own while its
 * siblings are still executing.
 */
export async function runStepHandlerInProcess(
  stepDef: StepDefinition,
  attempt: StepAttemptContext,
  prev: PersistedValue,
  steps: Readonly<Record<string, PersistedValue>>,
): Promise<Exclude<StepOutcome, { kind: 'retry' }>> {
  let priorAttempts = attempt.priorAttempts ?? 0
  const history: StepAttempt[] = []
  for (;;) {
    const outcome = await runStepHandler(stepDef, { ...attempt, priorAttempts }, prev, steps)
    history.push(...outcome.history)
    if (outcome.kind !== 'retry') {
      return { ...outcome, history }
    }

    // An abort mid-backoff rejects with the abort's reason, which the caller
    // already treats as a cancellation, a stop, or a sibling's failure.
    await delayWithSignal(Math.max(0, outcome.retryAt - Date.now()), attempt.runSignal)
    priorAttempts = outcome.attempts
  }
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

    // Waiting out a backoff from an earlier execution: carry on from the next
    // attempt rather than starting the step over.
    const retrying = existing?.status === 'retrying' ? existing : undefined
    const frozenSteps = ctx.snapshotSteps()

    try {
      // A retrying step already passed its `when`; re-evaluating it could now
      // skip a step whose side effects have partly happened.
      if (stepDef.when && !retrying) {
        const shouldRun = await stepDef.when({ input: ctx.run.input, prev, steps: frozenSteps })

        if (!shouldRun) {
          // Persist the skip before announcing it, so a crash between the two
          // replays as a skip rather than re-evaluating the predicate.
          await ctx.saveStep({ kind: 'step', name: stepDef.name, status: 'skipped', output: null, attempts: 0 })

          await ctx.emit({
            type: 'stepSkipped',
            runId: ctx.run.id,
            workflow: ctx.run.workflow,
            stepName: stepDef.name,
          })

          return { kind: 'passthrough' }
        }
      }

      // A step resuming a backoff has started running, so it finishes rather
      // than switching to a cached result; its result is still cached after.
      const cacheKey = stepDef.cache
        ? cacheKeyOf(ctx.run.workflow, stepDef.name, stepDef.cache, ctx.run.input, prev, frozenSteps)
        : undefined
      if (cacheKey !== undefined && stepDef.cache && !retrying) {
        const cached = await cachedOutput(ctx, cacheKey, stepDef.cache.ttlMs)
        if (cached) {
          await reuseCached(ctx, stepDef.name, cached.output)
          return { kind: 'advance', output: cached.output }
        }
      }

      // A retry keeps the time the step first began, so its duration covers
      // every attempt and backoff. `stepStart` fired on that first execution.
      const startedAt = retrying?.startedAt ?? Date.now()
      if (retrying) {
        const retryAt = Number(retrying.output)
        if (Date.now() < retryAt) {
          if (ctx.durableBackoff) {
            return suspendForRetry(ctx, stepDef.name, retrying.error ?? '', retrying.attempts, retryAt, startedAt, retrying.history)
          }
          await delayWithSignal(retryAt - Date.now(), ctx.signals.run.signal)
        }
      } else {
        await ctx.emit({
          type: 'stepStart',
          runId: ctx.run.id,
          workflow: ctx.run.workflow,
          stepName: stepDef.name,
        })
      }

      const attempt: StepAttemptContext = {
        runId: ctx.run.id,
        input: ctx.run.input,
        runSignal: ctx.signals.run.signal,
        priorAttempts: retrying?.attempts ?? 0,
        priorError: retrying?.error ?? undefined,
      }
      // Suspending is only worth it when something will come back for the run.
      // A tick()-driven engine has no poller to, so it waits in process and the
      // tick still finishes the step.
      const outcome = ctx.durableBackoff
        ? await runStepHandler(stepDef, attempt, prev, frozenSteps)
        : await runStepHandlerInProcess(stepDef, attempt, prev, frozenSteps)

      // Attempts made by earlier executions come first, so the record spans
      // every attempt and the backoffs between them.
      const history = recent([...(retrying?.history ?? []), ...outcome.history])

      if (outcome.kind === 'retry') {
        if (isControlAbort(ctx)) {
          return { kind: 'halt' }
        }
        return suspendForRetry(ctx, stepDef.name, errorMessage(outcome.error), outcome.attempts, outcome.retryAt, startedAt, history)
      }

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
          kind: 'step',
          name: stepDef.name,
          status: 'failed',
          output: null,
          error: errorMessage(outcome.error),
          attempts: outcome.attempts,
          startedAt,
          history,
        })

        return { kind: 'failed', stepName: stepDef.name, error: outcome.error }
      }

      await ctx.saveStep({
        kind: 'step',
        name: stepDef.name,
        status: outcome.kind === 'early-complete' ? 'completed-early' : 'completed',
        output: outcome.output,
        attempts: outcome.attempts,
        startedAt,
        history,
      })

      // complete() ends the run, which a cached result replayed elsewhere
      // would not, so only a plain completion is cached.
      if (cacheKey !== undefined && stepDef.cache && outcome.kind === 'completed') {
        await cacheOutput(ctx, cacheKey, stepDef.name, stepDef.cache.ttlMs, outcome.output)
      }

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
        throw new InternalError(`EarlyCompleteError escaped runStepHandler for step "${stepDef.name}"`)
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

/**
 * The key a step's result is cached under: a hash of the workflow, the step,
 * its cache version, and its key value — `{ input, prev, steps }` unless the
 * step names its own, everything the handler is given that can differ between
 * runs. Hashed so any key value fits a fixed-size column.
 */
export function cacheKeyOf(
  workflow: string,
  stepName: string,
  cache: StepCacheDefinition,
  input: PersistedValue,
  prev: PersistedValue,
  steps: Readonly<Record<string, PersistedValue>>,
): string {
  const value = withoutUndefinedFields(cache.key ? cache.key({ input, prev, steps }) : { input, prev, steps })
  // Encoded on its own first, so an error points into the key, not this tuple.
  const encodedValue = canonicalizePersistedValue(value, `Step "${stepName}" cache key`)
  const identity = JSON.stringify([workflow, stepName, cache.version ?? null, encodedValue])
  return createHash('sha256').update(identity).digest('hex')
}

/**
 * `value` with object fields set to `undefined` left out, so `{ page: undefined }`
 * keys the same as `{}` — an optional field passed as undefined is not a
 * different run.
 */
function withoutUndefinedFields(value: PersistedValue): PersistedValue {
  if (Array.isArray(value)) return value.map(withoutUndefinedFields)
  // Only plain objects are rebuilt: anything else is left for the encoder to
  // refuse, rather than flattened into a plain object that would key wrongly.
  if (value === null || typeof value !== 'object' || ![Object.prototype, null].includes(Object.getPrototypeOf(value))) return value
  const fields: Record<string, PersistedValue> = {}
  for (const [field, fieldValue] of Object.entries(value)) {
    if (fieldValue !== undefined) fields[field] = withoutUndefinedFields(fieldValue)
  }
  return fields
}

/**
 * The cached output under `key`, if it is still within its ttl and within
 * the step's current one — so shortening a step's ttl takes effect at once.
 * A failed read is reported and treated as a miss: the step can still run.
 */
export async function cachedOutput(
  ctx: ExecutionContext,
  key: string,
  ttlMs: number,
): Promise<{ output: PersistedValue } | null> {
  let entry: CachedStep | null | undefined
  try {
    entry = await ctx.storage.getCachedStep?.(key)
  } catch (error) {
    ctx.reportError(error)
    return null
  }
  const now = Date.now()
  return entry && entry.expiresAt > now && entry.createdAt > now - ttlMs ? { output: entry.output } : null
}

/** Complete a step with a cached output, without running its handler. */
export async function reuseCached(ctx: ExecutionContext, stepName: string, output: PersistedValue): Promise<void> {
  await ctx.emit({ type: 'stepStart', runId: ctx.run.id, workflow: ctx.run.workflow, stepName })
  await ctx.saveStep({ kind: 'step', name: stepName, status: 'completed', output, attempts: 0, startedAt: Date.now(), history: [] })
  await ctx.emit({
    type: 'stepComplete',
    runId: ctx.run.id,
    workflow: ctx.run.workflow,
    stepName,
    output,
    attempts: 0,
    cached: true,
  })
  ctx.steps[stepName] = structuredClone(output)
}

/**
 * Cache a step's output for later runs. The step has already completed and
 * been saved, so a failed write is reported rather than failing the run.
 */
export async function cacheOutput(
  ctx: ExecutionContext,
  key: string,
  stepName: string,
  ttlMs: number,
  output: PersistedValue,
): Promise<void> {
  const now = Date.now()
  try {
    await ctx.storage.putCachedStep?.({ key, workflow: ctx.run.workflow, stepName, output, createdAt: now, expiresAt: now + ttlMs })
  } catch (error) {
    ctx.reportError(error)
  }
}

/**
 * Record a step as `retrying` and suspend the run until its next attempt.
 *
 * The backoff becomes a durable sleep: the lease and the concurrency slot are
 * released for its whole length, and the attempt count is persisted, so a
 * crash mid-backoff resumes at the right attempt instead of starting the
 * step's attempts over.
 */
async function suspendForRetry(
  ctx: ExecutionContext,
  stepName: string,
  error: string,
  attempts: number,
  retryAt: number,
  startedAt: number,
  history: readonly StepAttempt[],
): Promise<UnitOutcome> {
  await ctx.saveStep({ kind: 'step', name: stepName, status: 'retrying', output: retryAt, error, attempts, startedAt, history })

  if (!(await ctx.storage.sleepRun(ctx.run.id, ctx.run.leaseId, retryAt))) {
    throw new LeaseExpiredError(ctx.run.id)
  }

  return { kind: 'suspend', wakeAt: retryAt }
}

/**
 * The most attempts a step's history keeps. A step retried thousands of times
 * — retry used as polling — would otherwise rewrite an ever longer record on
 * every backoff.
 */
export const MAX_STEP_HISTORY = 100

/** The latest {@link MAX_STEP_HISTORY} attempts. */
export function recent(history: StepAttempt[]): StepAttempt[] {
  return history.length > MAX_STEP_HISTORY ? history.slice(-MAX_STEP_HISTORY) : history
}

/** True when the run signal aborted for a control-flow reason rather than a genuine error. */
function isControlAbort(ctx: ExecutionContext): boolean {
  return (
    ctx.signals.run.signal.aborted &&
    ctx.signals.run.signal.reason instanceof RunControlError
  )
}
