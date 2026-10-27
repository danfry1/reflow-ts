import { AsyncLocalStorage } from 'node:async_hooks'
import { setImmediate as afterPendingCallbacks, setTimeout as afterDelay } from 'node:timers'
import { canonicalizePersistedValue } from '../../storage/codec'
import { parseDuration } from '../duration'
import {
  ConfigError,
  DuplicateStepError,
  errorMessage,
  InternalError,
  RunControlError,
  RunSuspendedError,
  StepFailedError,
} from '../errors'
import type { PersistedValue, StepAttempt, StepResult, StepStatus } from '../types'
import {
  resolveCache,
  type FunctionStepConfig,
  type FunctionWorkflow,
  type StepDefinition,
  type StepRunContext,
  type WorkflowSteps,
} from '../workflow'
import { MinHeap } from './min-heap'
import { reachSleep } from './sleep'
import { toError } from './signals'
import { cachedOutput, cacheKeyOf, cacheOutput, recent, reuseCached, runStepHandler } from './step'
import type { ExecutionContext, UnitOutcome } from './types'
import { reachWait } from './wait-for-event'

/** The parts of a workflow written as a function that {@link executeFunction} runs. */
export type FunctionBody = Pick<FunctionWorkflow, 'name' | '~handler' | 'events' | 'maxOperations'>

/** One durable operation the function has started and not yet finished. */
interface Operation {
  readonly name: string
  readonly kind: 'step' | 'sleep' | 'wait'
  state: OperationState
  /** The function has awaited, or otherwise used, what the operation returned. */
  used: boolean
  /** Finished, and waiting its turn to be handed to the function. */
  releasing: boolean
}

/**
 * Whether an operation is doing work, or is blocked until a time or an event.
 * A blocked operation wakes in process at `wakeAt`, or when its event is sent;
 * when every operation is blocked, and each could be resumed from storage
 * (`durable`), the run is suspended instead and the operations rejected so
 * the function unwinds.
 */
type OperationState =
  | { readonly kind: 'running' }
  | {
      readonly kind: 'blocked'
      readonly wakeAt: number | null
      readonly durable: boolean
      readonly event: string | null
      timer: ReturnType<typeof setTimeout> | undefined
      readonly poll: ReturnType<typeof afterDelay> | undefined
      readonly wake: () => void
      readonly reject: (error: Error) => void
    }

/** How a run ended before its function finished: suspended, halted by a control-flow abort, or failed by an error no step answers for. */
type Stop = Extract<UnitOutcome, { kind: 'suspend' } | { kind: 'halt' } | { kind: 'failed' }>

/** What an operation ended with, before it is handed to the function in order. */
type Settled<T> = { readonly ok: true; readonly value: T } | { readonly ok: false; readonly error: Error }

/** The longest delay `setTimeout` honours; anything longer fires at once. */
const MAX_TIMER_MS = 2_147_483_647

/**
 * How long nothing may start or finish before a run whose operations are all
 * blocked is suspended. Covers a branch awaiting something other than a step
 * between two steps — a log flush, a quick read — so it is not cut off by a
 * sibling's sleep.
 */
const SUSPEND_GRACE_MS = 25

/**
 * How often a wait blocked in process, beside other work, checks storage for
 * its event: one sent from another engine reaches it no later than this.
 */
const EVENT_POLL_MS = 1_000

/**
 * Microtask turns between handing the function one result and the next, so
 * what it chains on one result (`.then`, an `async` wrapper) runs before the
 * next arrives, as it did when they finished apart. Microtasks rather than a
 * timer, so a test that fakes timers cannot stall them.
 */
const RELEASE_SPACING = 64

const RUNNING: OperationState = { kind: 'running' }

const NO_STEPS: Readonly<Record<string, PersistedValue>> = Object.freeze({})

/**
 * Set while a step handler runs, so an operation it starts is refused. Carries
 * the execution it belongs to: work a handler starts — an engine claiming
 * another run, say — carries the value along, and must not be refused there.
 */
const insideStep = new AsyncLocalStorage<{ readonly execution: object; readonly step: string }>()

/**
 * What a durable operation returns: a promise that notes when the function
 * first awaits it or chains on it, so one the function never waits for — a
 * `void step.sleep(...)` — cannot hold up the run.
 */
class OperationPromise<T> extends Promise<T> {
  static override get [Symbol.species](): PromiseConstructor {
    return Promise
  }

  #onUse: (() => void) | undefined

  /** A promise settling as `inner` does, calling `onUse` the first time it is awaited or chained on. */
  static tracking<T>(inner: Promise<T>, onUse: () => void): OperationPromise<T> {
    const promise = new OperationPromise<T>((resolve, reject) => {
      inner.then(resolve, reject)
    })
    // Handled here without counting as a use, so one the function ignores
    // cannot surface as an unhandled rejection.
    Promise.prototype.then.call(promise, undefined, noop)
    promise.#onUse = onUse
    return promise
  }

  override then<TResult1 = T, TResult2 = never>(
    onFulfilled?: ((value: T) => TResult1 | PromiseLike<TResult1>) | null,
    onRejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null,
  ): Promise<TResult1 | TResult2> {
    const onUse = this.#onUse
    this.#onUse = undefined
    onUse?.()
    return super.then(onFulfilled, onRejected)
  }
}

/**
 * Run a workflow written as a function, reducing it to a {@link UnitOutcome}.
 *
 * The function runs from the top on every execution of the run. Each durable
 * operation it calls — `step.run`, `step.sleep`, `step.waitForEvent` — is saved
 * under its name, and one already finished on an earlier execution returns its
 * saved result at once, so replay costs a lookup per operation and repeats no
 * work. Because operations are matched by name rather than position, code that
 * takes a different branch on replay starts new steps instead of being handed
 * another step's result.
 *
 * Results are handed to the function in the order the operations finished,
 * on the first execution and on every replay alike: each save is stamped,
 * strictly increasing, and a result waits for every earlier stamp still
 * unresolved. So `Promise.race` between operations picks the same winner
 * every time.
 *
 * Operations may run concurrently (`Promise.all`). The run suspends only when
 * every operation in flight is blocked — sleeping, waiting for an event, or
 * backing off before a retry — and nothing has started or finished for a
 * moment, then until the earliest of their wake times. A step still running
 * is never cut off by another's sleep.
 *
 * A step's failure is the function's to handle: `step.run` rejects with a
 * `StepFailedError`. Anything else an operation hits — storage failing, an
 * output that cannot be saved, a misconfigured call — is not, and fails the
 * run however the function catches it, as it would a chained workflow.
 */
export async function executeFunction(ctx: ExecutionContext, body: FunctionBody): Promise<UnitOutcome> {
  const runSignal = ctx.signals.run.signal
  // This execution's identity, for telling its own step handlers apart.
  const self = {}
  const multipleWaits = ctx.storage.supportsEventNames === true
  const operations = new Set<Operation>()
  const unfinished = new Set<Promise<unknown>>()
  const names = new Set<string>()
  // Each operation's kind, recorded on every row it saves.
  const kinds = new Map<string, Operation['kind']>()
  // The step each error thrown out of an operation came from, to name the
  // failed step if it ends the run.
  const origins = new WeakMap<Error, string>()
  // The failures `step.run` answers with, which the function may catch. Any
  // other error out of an operation ends the run.
  const stepFailures = new WeakSet<Error>()
  // Control-flow errors the engine itself raised. One the function throws of
  // its own accord is a failure like any other.
  const engineStops = new WeakSet<Error>()
  // Caught failures and timed-out waits: what later code may branch on
  // besides completed outputs, so part of a cached step's default key.
  const outcomes: Record<string, PersistedValue> = {}
  // Events sent to this engine for the run while it executes, so a wait
  // that has not blocked yet still sees one that arrived after it looked.
  const delivered = new Set<string>()

  // Aborted when the run aborts, and when the function fails, so steps still
  // running unwind with it rather than carrying on detached.
  const operationAbort = new AbortController()
  // Each running step's own controller, aborted with `operationAbort`. One
  // listener fans out to them all: a listener per step on one signal would
  // cost a fan-out of thousands time that grows with the square of their number.
  const stepAborts = new Set<AbortController>()
  operationAbort.signal.addEventListener('abort', () => {
    for (const stepAbort of stepAborts) stepAbort.abort(operationAbort.signal.reason)
  }, { once: true })
  // Set once this execution is ending in failure: steps cut off by it save nothing.
  let ending = false
  // The function's own error, once it threw: steps cut off reject with it, and
  // must not be blamed for it.
  let cutOffBy: Error | undefined

  let stopping = false
  let stop: Stop | null = null
  let settled = false
  let checkQueued = false
  // Operations started or finished, so a suspend check can tell the run has been quiet.
  let activity = 0
  let activitySeen = -1
  let parking: Promise<void> | null = null
  // Operations the function never waited for, cut off once it settled.
  const abandoned = new Set<Operation>()
  let resolveStop: (stop: Stop) => void = noop
  let rejectStop: (error: unknown) => void = noop
  const stopped = new Promise<Stop>((resolve, reject) => {
    resolveStop = resolve
    rejectStop = reject
  })
  // Awaited only while the run is live; a failure to park it after that is
  // surfaced by whichever await sees it.
  stopped.catch(noop)

  // --- Ordering results ---------------------------------------------------

  let lastStamp = 0
  for (const row of ctx.replay.values()) lastStamp = Math.max(lastStamp, row.updatedAt)
  // Stamps not yet resolved: a save in flight, or one whose operation has
  // not yet been handed its result or blocked. No later result passes them.
  const unresolved = new Set<number>()
  const unresolvedHeap = new MinHeap<number>((a, b) => a < b)
  // The stamp each operation's last save holds, until it is released.
  const holding = new Map<string, number>()
  /** Hold stamp `at` for operation `name`. */
  const hold = (name: string, at: number): void => {
    holding.set(name, at)
    unresolved.add(at)
    unresolvedHeap.push(at)
  }
  // Operations an earlier execution finished hold their place in the order
  // until the function reaches them again — so one it reaches late, after
  // other work, still comes before those that finished after it — or until
  // the run goes quiet without reaching them, as after a deploy removed them.
  const replayHeld = new Set<string>()
  for (const row of ctx.replay.values()) {
    if (row.status === 'completed' || row.status === 'failed' || row.status === 'skipped') {
      hold(row.name, row.updatedAt)
      replayHeld.add(row.name)
    }
  }
  /** Stop holding places for finished operations the function has not reached. */
  const dropReplayHolds = (): void => {
    for (const name of [...replayHeld]) letGo(name)
  }
  // The stamp of the last save each operation made: the order it finished in.
  const finishedAt = new Map<string, number>()
  const awaitingRelease = new MinHeap<{ readonly at: number; readonly release: () => void }>((a, b) => a.at < b.at)
  let releasing = false
  let releaseWanted = false

  /** Stamp a save of operation `name`, holding the stamp until the operation lets it go. */
  const stamped = async <T>(name: string, save: (updatedAt: number) => Promise<T>): Promise<T> => {
    const at = lastStamp = Math.max(Date.now(), lastStamp + 1)
    finishedAt.set(name, at)
    letGo(name)
    hold(name, at)
    return await save(at)
  }

  /** Release the stamp operation `name` holds, so later results may pass it. */
  const letGo = (name: string): void => {
    replayHeld.delete(name)
    const at = holding.get(name)
    if (at === undefined) return
    holding.delete(name)
    unresolved.delete(at)
    queueRelease()
  }

  /** The lowest stamp still unresolved, if any. */
  const lowestUnresolved = (): number | undefined => {
    for (let at = unresolvedHeap.peek(); at !== undefined; at = unresolvedHeap.peek()) {
      if (unresolved.has(at)) return at
      unresolvedHeap.pop()
    }
    return undefined
  }

  // Every save an operation makes goes through here, stamped.
  const opsCtx: ExecutionContext = {
    ...ctx,
    saveStep: (record) =>
      stamped(record.name, (updatedAt) => ctx.saveStep({ ...record, updatedAt, kind: record.kind ?? kinds.get(record.name) })),
    trySaveStep: (record) =>
      stamped(record.name, (updatedAt) => ctx.trySaveStep({ ...record, updatedAt, kind: record.kind ?? kinds.get(record.name) })),
    consumeEvent: (name, _updatedAt, eventName) =>
      stamped(name, (updatedAt) => ctx.consumeEvent(name, updatedAt, eventName)),
  }

  /** Resolve once every result stamped before `at` has been handed over. */
  const inOrder = (at: number): Promise<void> =>
    new Promise((release) => {
      awaitingRelease.push({ at, release })
      queueRelease()
    })

  const queueRelease = (): void => {
    releaseWanted = true
    if (releasing) return
    releasing = true
    void releaseInOrder()
  }

  const releaseInOrder = async (): Promise<void> => {
    while (releaseWanted) {
      releaseWanted = false
      // Operations replayed together are all waiting before the earliest of
      // them is handed over.
      await turns(RELEASE_SPACING)
      for (let next = awaitingRelease.peek(); next; next = awaitingRelease.peek()) {
        const lowest = lowestUnresolved()
        if (lowest !== undefined && lowest < next.at) break
        awaitingRelease.pop()
        next.release()
        await turns(RELEASE_SPACING)
      }
    }
    releasing = false
  }

  // --- Stopping -----------------------------------------------------------

  /** What an operation throws once the run has stopped: the abort's reason, or a suspension. */
  const stopError = (): Error => {
    const error = runSignal.aborted && runSignal.reason instanceof RunControlError
      ? runSignal.reason
      : new RunSuspendedError(ctx.run.id)
    engineStops.add(error)
    return error
  }

  /** Stop the run for good: reject every blocked operation, and report `outcome`. */
  const finish = (outcome: Stop): void => {
    if (stop) return
    stopping = true
    stop = outcome
    rejectBlocked()
    resolveStop(outcome)
  }

  /** End this execution as failed by `error`, which no step answers for. */
  const fail = (error: Error, stepName: string): void => {
    if (stop) return
    ending = true
    if (!operationAbort.signal.aborted) operationAbort.abort(error)
    finish({ kind: 'failed', stepName, error })
  }

  /** Reject every blocked operation, returning them. */
  const rejectBlocked = (): Operation[] => {
    const rejected: Operation[] = []
    for (const operation of operations) {
      if (operation.state.kind === 'blocked') {
        clearTimeout(operation.state.timer)
        clearTimeout(operation.state.poll)
        operation.state.reject(stopError())
        rejected.push(operation)
      }
    }
    return rejected
  }

  const onRunAbort = (): void => {
    const reason = runSignal.reason
    if (reason instanceof RunControlError) {
      engineStops.add(reason)
      if (!operationAbort.signal.aborted) operationAbort.abort(reason)
      finish({ kind: 'halt' })
      return
    }
    // A genuine error, such as the heartbeat failing to reach storage: the
    // run cannot carry on safely, so it fails at the step it was running.
    const running = [...operations].find((operation) => operation.state.kind === 'running')
    fail(toError(reason), running?.name ?? '')
  }
  if (runSignal.aborted) {
    onRunAbort()
  } else {
    runSignal.addEventListener('abort', onRunAbort, { once: true })
  }

  // An event sent to this engine wakes a wait blocked in process for it, which
  // then takes the event from storage as it would on any other wake.
  const stopListening = ctx.onEvent((eventName) => {
    if (stopping) return
    delivered.add(eventName)
    for (const operation of operations) {
      if (operation.state.kind === 'blocked' && operation.state.event === eventName) {
        operation.state.wake()
      }
    }
  })

  // --- Suspending ---------------------------------------------------------

  /** Check, once pending callbacks have run, whether every operation is blocked. */
  const queueCheck = (delay = 0): void => {
    if (checkQueued || stopping) return
    checkQueued = true
    const check = (): void => {
      checkQueued = false
      const attempt = suspendIfBlocked()
      if (stopping) parking = attempt
      attempt.catch((error: unknown) => {
        stopping = true
        rejectBlocked()
        rejectStop(error)
      })
    }
    if (delay > 0) {
      afterDelay(check, delay)
    } else {
      afterPendingCallbacks(check)
    }
  }

  /**
   * Suspend the run durably if nothing in it can make progress: every
   * operation is blocked, each can be resumed from storage, and nothing has
   * started or finished for {@link SUSPEND_GRACE_MS}. It sleeps until the
   * earliest wake time among them, or waits for the pending events with
   * that time as its deadline.
   */
  const suspendIfBlocked = async (): Promise<void> => {
    if (stopping || settled || operations.size === 0) return
    let wakeAt: number | null = null
    const events = new Set<string>()
    let allBlocked = true
    let waitedOn = false
    for (const operation of operations) {
      const state = operation.state
      if (state.kind !== 'blocked') {
        // Doing work: nothing to do yet. Waiting its turn: the run may be
        // quiet, with a place held for an operation it will not reach.
        if (!operation.releasing) return
        allBlocked = false
        continue
      }
      if (!state.durable) return
      if (operation.used) waitedOn = true
      if (state.wakeAt !== null && (wakeAt === null || state.wakeAt < wakeAt)) wakeAt = state.wakeAt
      if (state.event !== null) events.add(state.event)
    }
    if (activity !== activitySeen) {
      activitySeen = activity
      queueCheck(SUSPEND_GRACE_MS)
      return
    }
    // Quiet with results waiting their turn: the places still held are for
    // operations the function is not going to reach.
    if (!allBlocked) {
      dropReplayHolds()
      return
    }
    // Only operations the function never waited for are blocked: it is busy
    // with something else, and suspending would cut that off.
    if (!waitedOn) return

    // Decided: from here no operation may start or wake in process.
    stopping = true
    for (const operation of operations) {
      if (operation.state.kind === 'blocked') {
        clearTimeout(operation.state.timer)
        clearTimeout(operation.state.poll)
      }
    }

    let parked: boolean
    const [onlyEvent] = events
    if (onlyEvent !== undefined) {
      parked = await ctx.storage.waitRun(ctx.run.id, ctx.run.leaseId, events.size === 1 ? onlyEvent : [...events], wakeAt)
    } else if (wakeAt !== null) {
      parked = await ctx.storage.sleepRun(ctx.run.id, ctx.run.leaseId, wakeAt)
    } else {
      throw new InternalError(`Run "${ctx.run.id}" has blocked operations with neither a wake time nor an event`)
    }
    finish(parked ? { kind: 'suspend', wakeAt } : { kind: 'halt' })
  }

  // --- Operations ---------------------------------------------------------

  /** Register an operation, refusing it once the run has stopped, inside a step, or when its name is taken. */
  const begin = (name: string, kind: Operation['kind']): Operation => {
    if (stopping) throw stopError()
    const outer = insideStep.getStore()
    if (outer?.execution === self) {
      throw new ConfigError(`Step "${outer.step}" in workflow "${body.name}" starts "${name}" from inside its handler; call step operations from the workflow function only`)
    }
    if (typeof name !== 'string' || name.length === 0) {
      throw new ConfigError(`Every step in workflow "${body.name}" needs a non-empty name`)
    }
    if (names.has(name)) throw new DuplicateStepError(body.name, name)
    if (names.size >= body.maxOperations) {
      throw new ConfigError(
        `Workflow "${body.name}" started more than ${body.maxOperations} operations in one run, at "${name}". ` +
        'A run replays every operation before the point it stopped each time it resumes; raise maxOperations, or split the work across runs.',
      )
    }
    names.add(name)
    kinds.set(name, kind)
    const operation: Operation = { name, kind, state: RUNNING, used: false, releasing: false }
    operations.add(operation)
    activity++
    return operation
  }

  /**
   * Block `operation` until `wakeAt` (epoch ms, or `null` for no time), or
   * until `event` is sent, then resolve. Rejects instead if the run stops
   * first. `durable` says whether storage could resume it, which a backoff on
   * an engine driven by `tick()` cannot: nothing would come back for the run.
   */
  const block = (operation: Operation, wakeAt: number | null, durable: boolean, event: string | null): Promise<void> => {
    if (stopping) return Promise.reject(stopError())
    // Once the function has settled, nothing would suspend the run or wake a
    // block, so an operation still going cannot wait.
    if (settled) {
      abandoned.add(operation)
      return Promise.reject(toError(operationAbort.signal.reason ?? stopError()))
    }
    // A blocked operation's result is not next; later ones may pass it.
    letGo(operation.name)
    // The event arrived between the wait's last look and now: look again.
    if (event !== null && delivered.has(event)) return Promise.resolve()
    return new Promise<void>((resolve, reject) => {
      const wake = (): void => {
        if (stopping || operation.state.kind !== 'blocked') return
        clearTimeout(operation.state.timer)
        clearTimeout(operation.state.poll)
        operation.state = RUNNING
        resolve()
      }
      // A delay past what `setTimeout` can hold is waited out in steps.
      const arm = (): ReturnType<typeof setTimeout> | undefined => {
        if (wakeAt === null) return undefined
        const delay = Math.max(0, wakeAt - Date.now())
        if (delay <= MAX_TIMER_MS) return setTimeout(wake, delay)
        return setTimeout(() => {
          if (operation.state.kind === 'blocked') operation.state.timer = arm()
        }, MAX_TIMER_MS)
      }
      // A wait beside other work looks for its event now and then, for one
      // sent from another engine, which cannot wake it here.
      const poll = event === null ? undefined : afterDelay(wake, EVENT_POLL_MS)
      operation.state = { kind: 'blocked', wakeAt, durable, event, timer: undefined, poll, wake, reject }
      operation.state.timer = arm()
      queueCheck()
    })
  }

  /** The step an error came from: its own origin, or the first along its `cause` chain. */
  const originOf = (error: Error): string | undefined => {
    let current: unknown = error
    for (let depth = 0; current instanceof Error && depth < 16; depth++) {
      const origin = origins.get(current)
      if (origin !== undefined) return origin
      current = current.cause
    }
    return undefined
  }

  /**
   * Decide what an error out of an operation means. A control-flow error
   * stops the run for good, so a function that swallows one cannot carry on.
   * A step's own failure goes to the function. Anything else fails the run.
   */
  const settleError = (error: Error, name: string): Error => {
    // The function's own error, which cut this operation off: already its to report.
    if (error === cutOffBy) throw error
    if (error instanceof RunControlError) {
      engineStops.add(error)
      if (!(error instanceof RunSuspendedError)) finish({ kind: 'halt' })
      throw error
    }
    // A step cut off by the function's failure rejects with that same error,
    // which must keep naming the step it came from.
    if (!origins.has(error)) origins.set(error, name)
    if (!stepFailures.has(error)) {
      fail(error, name)
      throw error
    }
    return error
  }

  /**
   * Run one operation: register it, track it until it settles, and hand its
   * result to the function in the order operations finished. A step's or a
   * wait's result is recorded for later cache keys when it is handed over,
   * so what those keys see is the same on every replay.
   */
  const operate = <T extends PersistedValue | void>(
    name: string,
    kind: Operation['kind'],
    work: (operation: Operation, existing: StepResult | undefined) => Promise<T>,
  ): Promise<T> => {
    let operation: Operation | undefined
    let used = false
    const promise = (async () => {
      try {
        let settledWith: Settled<T>
        try {
          operation = begin(name, kind)
          operation.used = used
          const existing = ctx.replay.get(name)
          // A row records its kind, so a name now used for another kind of
          // operation — after a deploy — is caught rather than handed the
          // other's result. Rows saved before kinds were kept carry none.
          if (existing?.kind && existing.kind !== kind) {
            throw new ConfigError(
              `"${name}" in workflow "${body.name}" was saved as a ${existing.kind} by an earlier execution of this run, ` +
              `but is now a ${kind}. Give each operation a name of its own.`,
            )
          }
          settledWith = { ok: true, value: await work(operation, existing) }
        } catch (error) {
          settledWith = { ok: false, error: settleError(toError(error), name) }
        }
        const at = operation ? finishedAt.get(name) ?? ctx.replay.get(name)?.updatedAt : undefined
        if (operation) {
          letGo(name)
          operation.releasing = true
        }
        if (at !== undefined) await inOrder(at)
        if (!settledWith.ok) throw settledWith.error
        if (kind !== 'sleep') ctx.steps[name] = structuredClone(settledWith.value as PersistedValue)
        return settledWith.value
      } finally {
        if (operation) {
          letGo(name)
          operations.delete(operation)
          activity++
          queueCheck()
        }
      }
    })()
    unfinished.add(promise)
    promise.then(() => unfinished.delete(promise), () => unfinished.delete(promise))
    // A function may leave an operation unawaited, or stop awaiting it once
    // the run is suspended; neither surfaces as an unhandled rejection.
    return OperationPromise.tracking(promise, () => {
      used = true
      if (operation) operation.used = true
      queueCheck()
    })
  }

  /** Throw the run's control-flow abort, if that is why an attempt failed. */
  const throwIfControlAbort = (): void => {
    if (runSignal.aborted && runSignal.reason instanceof RunControlError) throw runSignal.reason
  }

  /** Fail with a mismatch when a saved row is not the kind of operation that now has its name. */
  const mismatch = (name: string, status: StepStatus, kind: Operation['kind']): ConfigError =>
    new ConfigError(
      `"${name}" in workflow "${body.name}" was saved as a ${status} operation by an earlier execution of this run, ` +
      `but is now a ${kind}. Give each operation a name of its own.`,
    )

  /** The rejection for a step that failed, which the function may catch. */
  const stepFailure = (name: string, attempts: number, message: string, cause: Error): StepFailedError => {
    const failure = new StepFailedError(name, attempts, { cause, lastError: message })
    stepFailures.add(failure)
    outcomes[name] = { failed: message }
    return failure
  }

  /** A saved failure's error, rebuilt with the name and code it had. */
  const savedCause = (row: StepResult): Error => {
    const cause = new Error(row.error ?? 'Step failed')
    const saved = row.output
    if (saved !== null && typeof saved === 'object' && !Array.isArray(saved) && !(saved instanceof Date)) {
      if (typeof saved['name'] === 'string') cause.name = saved['name']
      if (typeof saved['code'] === 'string') Object.assign(cause, { code: saved['code'] })
    }
    return cause
  }

  const runStep = (name: string, config: FunctionStepConfig): Promise<PersistedValue> =>
    operate(name, 'step', async (operation, existing) => {
      if (existing) {
        switch (existing.status) {
          case 'retrying':
            break
          case 'completed':
            return existing.output
          case 'failed':
            // A failure the function caught on an earlier execution is replayed
            // as one, so the function takes the same path again.
            throw stepFailure(name, existing.attempts, existing.error ?? 'Step failed', savedCause(existing))
          case 'skipped':
            // The function never waited for it on an earlier execution, so it
            // never finished: say so, should the function now wait for it.
            throw stepFailure(name, existing.attempts, 'The step did not finish on an earlier execution', new Error('Not awaited'))
          default:
            throw mismatch(name, existing.status, 'step')
        }
      }

      const definition: StepDefinition = {
        name,
        // The types checked the handler resolves to data that can be saved,
        // and saving it checks again, so the narrowing here cannot let
        // anything else through.
        handler: (handlerCtx) => insideStep.run({ execution: self, step: name }, () => config.handler({
          signal: handlerCtx.signal,
          runId: handlerCtx.runId,
          attempt: handlerCtx.attempt,
          idempotencyKey: handlerCtx.idempotencyKey,
        } satisfies StepRunContext)) as Promise<PersistedValue | void>,
        retry: config.retry && {
          maxAttempts: config.retry.maxAttempts,
          backoff: config.retry.backoff ?? 'linear',
          initialDelayMs: config.retry.initialDelay === undefined ? undefined : parseDuration(config.retry.initialDelay),
        },
        timeoutMs: config.timeout === undefined ? undefined : parseDuration(config.timeout),
        cache: resolveCache(body.name, name, config.cache),
      }
      if (definition.cache && (!ctx.storage.getCachedStep || !ctx.storage.putCachedStep)) {
        throw new ConfigError(`Step "${name}" in workflow "${body.name}" is cached, but this storage adapter does not store cached steps`)
      }

      const retrying = existing?.status === 'retrying' ? existing : undefined
      // Everything the step could depend on has resolved before it was called:
      // results handed to the function so far, and failures or timeouts it
      // branched on. Only a cached step needs them; copying them for every
      // step would cost a run of many steps time that grows with the square
      // of their number.
      const frozenSteps = definition.cache ? ctx.snapshotSteps() : NO_STEPS
      const cacheKey = definition.cache
        ? cacheKeyOf(ctx.run.workflow, name, definition.cache, ctx.run.input, structuredClone(outcomes), frozenSteps)
        : undefined
      if (cacheKey !== undefined && definition.cache && !retrying) {
        const cached = await cachedOutput(opsCtx, cacheKey, definition.cache.ttlMs)
        if (cached) {
          await reuseCached(opsCtx, name, cached.output)
          return cached.output
        }
      }

      const startedAt = retrying?.startedAt ?? Date.now()
      let attempts = retrying?.attempts ?? 0
      let lastError = retrying?.error ?? undefined
      let history: StepAttempt[] = [...(retrying?.history ?? [])]

      if (retrying) {
        const retryAt = Number(retrying.output)
        if (Date.now() < retryAt) await block(operation, retryAt, ctx.durableBackoff, null)
      } else {
        await ctx.emit({ type: 'stepStart', runId: ctx.run.id, workflow: ctx.run.workflow, stepName: name })
      }

      const stepAbort = new AbortController()
      if (operationAbort.signal.aborted) {
        stepAbort.abort(operationAbort.signal.reason)
      } else {
        stepAborts.add(stepAbort)
      }
      try {
        return await attemptStep(operation, definition, stepAbort.signal, { startedAt, attempts, lastError, history, cacheKey, frozenSteps })
      } finally {
        stepAborts.delete(stepAbort)
      }
    })

  /** Run a step's attempts, backing off between them, to its result or failure. */
  const attemptStep = async (
    operation: Operation,
    definition: StepDefinition,
    signal: AbortSignal,
    // Where the step stands: its attempts so far, carried over from a backoff
    // an earlier execution began, and what caching it needs.
    progress: {
      readonly startedAt: number
      readonly attempts: number
      readonly lastError: string | undefined
      readonly history: StepAttempt[]
      readonly cacheKey: string | undefined
      readonly frozenSteps: Readonly<Record<string, PersistedValue>>
    },
  ): Promise<PersistedValue> => {
    const { name } = definition
    const { startedAt, cacheKey, frozenSteps } = progress
    let { attempts, lastError, history } = progress
    for (;;) {
      const outcome = await runStepHandler(
        definition,
        {
          runId: ctx.run.id,
          input: ctx.run.input,
          runSignal: signal,
          priorAttempts: attempts,
          priorError: lastError,
        },
        undefined,
        frozenSteps,
      )
      history = recent([...history, ...outcome.history])

      switch (outcome.kind) {
        case 'retry': {
          throwIfControlAbort()
          // Cut off because the run is failing elsewhere: not this step's
          // failure, so it neither backs off nor saves a retry.
          if (ending) throw outcome.error
          lastError = errorMessage(outcome.error)
          attempts = outcome.attempts
          await opsCtx.saveStep({
            name,
            status: 'retrying',
            output: outcome.retryAt,
            error: lastError,
            attempts,
            startedAt,
            history,
          })
          await block(operation, outcome.retryAt, ctx.durableBackoff, null)
          continue
        }
        case 'failed': {
          throwIfControlAbort()
          // Cut off because the run is failing elsewhere: not this step's
          // failure, so nothing is saved and resuming the run runs it again.
          if (ending) throw outcome.error
          const message = errorMessage(outcome.error)
          const code = 'code' in outcome.error && typeof outcome.error.code === 'string' ? outcome.error.code : null
          await opsCtx.saveStep({
            name,
            status: 'failed',
            // What the error was, besides its message, so a replay rebuilds it alike.
            output: { name: outcome.error.name, code },
            error: message,
            attempts: outcome.attempts,
            startedAt,
            history,
          })
          throw stepFailure(name, outcome.attempts, message, outcome.error)
        }
        case 'early-complete':
          throw new InternalError(`Step "${name}" completed the run early, which a function's steps cannot do`)
        case 'completed': {
          await opsCtx.saveStep({
            name,
            status: 'completed',
            output: outcome.output,
            attempts: outcome.attempts,
            startedAt,
            history,
          })
          if (cacheKey !== undefined && definition.cache) {
            await cacheOutput(opsCtx, cacheKey, name, definition.cache.ttlMs, outcome.output)
          }
          await ctx.emit({
            type: 'stepComplete',
            runId: ctx.run.id,
            workflow: ctx.run.workflow,
            stepName: name,
            output: outcome.output,
            attempts: outcome.attempts,
          })
          return outcome.output
        }
      }
    }
  }

  /** Sleep until the time `wakeAtFor` gives, fixed the first time the sleep is reached. */
  const sleepOperation = (name: string, wakeAtFor: () => number): Promise<void> =>
    operate(name, 'sleep', async (operation, existing) => {
      // Completed, or never waited for on an earlier execution: either way the
      // sleep is over.
      if (existing?.status === 'completed' || existing?.status === 'skipped') return
      if (existing && existing.status !== 'sleeping') throw mismatch(name, existing.status, 'sleep')

      const wakeAt = existing ? Number(existing.output) : wakeAtFor()
      let resuming = existing !== undefined
      let saved = false
      while ((await reachSleep(opsCtx, name, wakeAt, resuming, saved)) === 'pending') {
        await block(operation, wakeAt, true, null)
        resuming = true
        saved = true
      }
    })

  const waitOperation = (name: string, options: { event?: string; timeout?: number | string } | undefined): Promise<PersistedValue> =>
    operate(name, 'wait', async (operation, existing) => {
      const eventName = options?.event ?? name
      if (!Object.hasOwn(body.events, eventName)) {
        throw new ConfigError(`Workflow "${body.name}" waits for the event "${eventName}", which is not declared in its events`)
      }
      const timeoutMs = options?.timeout === undefined ? undefined : parseDuration(options.timeout)
      if (timeoutMs === 0) {
        throw new ConfigError(`waitForEvent "${name}" timeout must be above zero`)
      }
      if (existing) {
        switch (existing.status) {
          case 'waiting':
            break
          case 'completed':
            return existing.output
          case 'skipped':
            outcomes[name] = { timedOut: true }
            return null
          default:
            throw mismatch(name, existing.status, 'wait')
        }
      }

      let waiting: { deadline: number | null; saved?: boolean } | undefined = existing
        ? { deadline: typeof existing.output === 'number' ? existing.output : null }
        : undefined
      for (;;) {
        delivered.delete(eventName)
        const reached = await reachWait(opsCtx, name, eventName, timeoutMs, waiting)
        switch (reached.kind) {
          case 'event':
            return reached.payload
          case 'timeout':
            // Timing out is an answer the function handles, not a failure:
            // saved as skipped, it replays as `null` and survives a resume.
            await opsCtx.saveStep({ name, status: 'skipped', output: null, attempts: 0 })
            await ctx.emit({ type: 'stepSkipped', runId: ctx.run.id, workflow: ctx.run.workflow, stepName: name })
            outcomes[name] = { timedOut: true }
            return null
          case 'pending':
            // Without storage that parks a run on several events at once, two
            // pending waits could miss the second's delivery. Refuse rather
            // than risk it.
            if (!multipleWaits) {
              for (const other of operations) {
                if (other !== operation && other.state.kind === 'blocked' && other.state.event !== null) {
                  throw new ConfigError(
                    `Workflow "${body.name}" waits for "${name}" while still waiting for "${other.name}"; this storage adapter allows one pending wait at a time`,
                  )
                }
              }
            }
            await block(operation, reached.deadline, true, eventName)
            waiting = { deadline: reached.deadline, saved: true }
        }
      }
    })

  const handlerOf = (
    handlerOrConfig: FunctionStepConfig | FunctionStepConfig['handler'],
  ): FunctionStepConfig =>
    typeof handlerOrConfig === 'function' ? { handler: handlerOrConfig } : handlerOrConfig

  const steps = {
    run: (name: string, handlerOrConfig: Parameters<typeof handlerOf>[0]) => runStep(name, handlerOf(handlerOrConfig)),
    sleep: (name: string, duration: number | string) =>
      sleepOperation(name, () => Date.now() + parseDuration(duration)),
    sleepUntil: (name: string, time: Date | number) =>
      sleepOperation(name, () => {
        const wakeAt = time instanceof Date ? time.getTime() : time
        if (!Number.isFinite(wakeAt)) {
          throw new ConfigError(`sleepUntil "${name}" needs a valid time`)
        }
        return wakeAt
      }),
    waitForEvent: (name: string, options?: { event?: string; timeout?: number | string }) => waitOperation(name, options),
  }

  const functionDone = Promise.resolve()
    .then(() => body['~handler']({
      input: ctx.run.input,
      // The overloads only narrow what `run` and `waitForEvent` resolve to;
      // at runtime each is the one implementation above.
      step: steps as unknown as WorkflowSteps<Record<string, PersistedValue>>,
      runId: ctx.run.id,
    }))
    .then(
      (value) => ({ kind: 'returned' as const, value }),
      (error: unknown) => ({ kind: 'threw' as const, error: toError(error) }),
    )

  /** Wait for every operation still in flight to settle, and for a suspend under way to land. */
  const drain = async (): Promise<void> => {
    while (unfinished.size > 0) {
      await Promise.allSettled([...unfinished])
    }
    await parking?.catch(noop)
  }

  try {
    const first = await Promise.race([functionDone, stopped])
    if (first.kind === 'suspend' || first.kind === 'halt' || first.kind === 'failed') {
      await drain()
      return first
    }

    settled = true
    // Nothing more will be reached: results waiting behind a held place go.
    dropReplayHolds()
    // The run stopped while the function was finishing, or the function
    // caught the rejection that stopping it threw: the stop stands.
    if (stopping) {
      const outcome = await stopped
      await drain()
      return outcome
    }
    // An operation the function did not wait for cannot block the run now.
    for (const operation of rejectBlocked()) abandoned.add(operation)

    if (first.kind === 'threw') {
      const error = first.error
      // Named before the steps it cuts off reject with it.
      const stepName = originOf(error)
      ending = true
      cutOffBy = error
      if (!operationAbort.signal.aborted) operationAbort.abort(error)
      await drain()
      if (stop) return stop
      // A control-flow error the engine raised unwinds the run; one the
      // function threw itself is a failure like any other.
      if (error instanceof RunControlError && engineStops.has(error)) return { kind: 'halt' }

      // A step's failure is reported by what its handler threw, as a chained
      // workflow's is; the StepFailedError is only the function's view of it.
      const reported = error instanceof StepFailedError && stepFailures.has(error) && error.cause instanceof Error
        ? error.cause
        : error
      return { kind: 'failed', stepName: stepName ?? '', error: reported }
    }

    await drain()
    if (stop) return stop
    // `void` and `undefined` coincide at runtime; the cast narrows the
    // function's `PersistedValue | void` return without touching the value.
    const output = first.value as PersistedValue
    try {
      canonicalizePersistedValue(output, `Workflow "${body.name}" output`)
    } catch (error) {
      return { kind: 'failed', stepName: '', error: toError(error) }
    }
    // A sleep, wait, or backoff the function never waited for did not finish:
    // record it as skipped, so a completed run leaves nothing looking under way.
    for (const operation of abandoned) {
      await opsCtx.saveStep({ name: operation.name, status: 'skipped', output: null, attempts: 0 })
    }
    return { kind: 'complete', output }
  } finally {
    runSignal.removeEventListener('abort', onRunAbort)
    stopListening()
  }
}

/** Yield `count` microtask turns. */
async function turns(count: number): Promise<void> {
  for (let turn = 0; turn < count; turn++) await undefined
}

function noop(): void {}
