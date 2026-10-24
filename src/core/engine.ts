import { createHash, randomUUID } from 'node:crypto'
import type { StandardSchemaV1 } from '@standard-schema/spec'
import { canonicalizePersistedValue, persistedValuesEqual } from '../storage/codec'
import { translateStorageErrors } from '../storage/translate-errors'
import {
  createBoundedAsyncIterator,
  type AbortableSubscriber,
} from './async-iterator'
import {
  assertNever,
  ConfigError,
  DuplicateWorkflowError,
  errorMessage,
  HookError,
  IdempotencyConflictError,
  LeaseExpiredError,
  RunCancelledError,
  RunControlError,
  RunNotFoundError,
  ValidationError,
  WorkflowNotFoundError,
} from './errors'
import { parseCron } from './cron'
import { parseDuration } from './duration'
import { firstOccurrence } from './schedule-timing'
import { cloneEngineEvent, type EngineEvent, type EngineHooks } from './events'
import { parallelExecutor } from './execution/parallel'
import { sleepExecutor } from './execution/sleep'
import { stepExecutor } from './execution/step'
import { toError } from './execution/signals'
import type { ExecutionContext, RunSignals, StepRecord, UnitOutcome } from './execution/types'
import { waitForEventExecutor } from './execution/wait-for-event'
import type {
  ClaimedRun,
  CountRunsFilter,
  ListRunsFilter,
  RunCounts,
  RunStatus,
  TerminalRunStatus,
  PersistedValue,
  RunInfo,
  StepResult,
  StorageAdapter,
  WorkflowRun,
  WorkflowSchedule,
  ScheduleRecurrence,
} from './types'
import type { AnyWorkflow, ExecutionUnit, WorkflowInputMap } from './workflow'

export type { EngineEvent, EngineEventOf, EngineHooks } from './events'

/** Options for {@link Engine.stream}. */
export interface StreamOptions {
  /**
   * Maximum number of events buffered before the engine pauses (backpressure).
   * Defaults to `Infinity` — events buffer without bound and the engine never
   * waits on the consumer. Set a finite value (e.g. `1`) to pace the engine
   * against a slow consumer; the engine will not start the next unit of work
   * until the consumer drains the buffer below this size. Set `0` for strict
   * rendezvous delivery, where every event waits for a pending pull.
   */
  bufferSize?: number
}

/**
 * A pull-based, backpressure-aware stream of {@link EngineEvent}s.
 *
 * Implements `AsyncIterableIterator`, so it works directly with `for await`,
 * and `AsyncDisposable`, so it works with `await using`. Breaking out of a
 * `for await` loop (or disposing) unsubscribes from the engine automatically.
 */
export interface ResultStream<E extends EngineEvent = EngineEvent>
  extends AsyncIterableIterator<E> {
  [Symbol.asyncDispose](): Promise<void>
}

/** Configuration for {@link createEngine}. */
export interface EngineConfig<TWorkflows extends readonly AnyWorkflow[] = readonly AnyWorkflow[]> {
  /** Storage backend for persisting runs and step results. */
  storage: StorageAdapter
  /** Workflows the engine can execute. */
  workflows: TWorkflows
  /** Optional lifecycle hooks. */
  hooks?: EngineHooks
  /** Maximum runs this instance executes at once (default: 1). */
  concurrency?: number
  /** How long a run's lease is valid before another engine can reclaim it (default: 30000). */
  runLeaseDurationMs?: number
  /** How often to renew the lease while a run is executing (default: leaseDuration / 3). */
  heartbeatIntervalMs?: number
}

/** Options for `engine.waitForRun()`. */
export interface WaitForRunOptions {
  /** Stop waiting when this aborts — `AbortSignal.timeout(ms)` for a deadline. Rejects with its reason. */
  signal?: AbortSignal
  /**
   * How often to re-read the run from storage, which is how a run finished by
   * another process is noticed (default: 1000). A run finishing on this engine
   * resolves the wait at once regardless.
   */
  pollIntervalMs?: number
}

/** Options for `engine.prune()`. */
export interface PruneOptions {
  /** Delete runs that finished longer ago than this: milliseconds, or a duration string such as `'30d'`. */
  olderThan: number | string
  /** Which finished statuses to delete. Defaults to all three: `completed`, `failed`, and `cancelled`. */
  statuses?: readonly TerminalRunStatus[]
}

/** Options for `engine.enqueue()`. */
export interface EnqueueOptions {
  /** Prevents duplicate runs. Same key + same input returns the existing run. Same key + different input throws. */
  idempotencyKey?: string
  /**
   * Start the run this long from now rather than straight away: milliseconds,
   * or a duration string such as `'10m'`. The run waits in storage, holding
   * no worker, and starts on time whichever process is running then.
   */
  delay?: number | string
  /** Start the run at this time rather than straight away. A time already past starts it now. */
  startAt?: Date | number
}

/**
 * How often a schedule fires.
 *
 * A bare number is milliseconds, so existing interval callers are unaffected.
 * `{ every }` additionally accepts a duration string (`'30s'`, `'24h'`), the
 * same form `.sleep()` takes. `{ cron }` takes a five-field expression,
 * evaluated in UTC.
 */
export type ScheduleSpec =
  | number
  | { readonly every: number | string; readonly cron?: never }
  | { readonly cron: string; readonly every?: never }

/** Options for `engine.schedule()`. */
export interface ScheduleOptions {
  /**
   * Identity of this schedule, shared across engine instances.
   *
   * Scheduled runs are enqueued with an idempotency key derived from this value
   * and the interval slot, so every instance running the same schedule
   * converges on a single run per interval. It defaults to a hash of the
   * workflow name, interval, and input, which is already stable across
   * instances — set it explicitly only to keep that identity fixed while one of
   * those changes, or to deliberately split one logical schedule into two.
   */
  key?: string
}

/** The workflow engine. Connects workflows to storage and handles execution, polling, and scheduling. */
export interface Engine<TWorkflowMap extends Record<string, PersistedValue> = Record<string, PersistedValue>> {
  /** Submit a workflow run. Type-safe: only accepts registered workflow names with matching input. */
  enqueue<TName extends string & keyof TWorkflowMap>(
    workflowName: TName,
    input: TWorkflowMap[TName],
    options?: EnqueueOptions,
  ): Promise<WorkflowRun>
  /** Get a run and its step results, or null if not found. */
  getRunStatus(runId: string): Promise<RunInfo | null>
  /**
   * List runs for inspection or dead-letter visibility, most recent first.
   * Filter by `status` and/or `workflow`, page with `limit` and `before`.
   */
  listRuns(filter?: ListRunsFilter): Promise<WorkflowRun[]>
  /**
   * Wait for a run to finish, resolving with it once it is `completed`,
   * `failed`, or `cancelled` — check `status`, `output`, and `error` to see
   * which. Resolves at once for a run that has already finished.
   *
   * A run finishing on this engine resolves the wait once its hooks and
   * `onFailure` have run; one finished by any other engine instance — in
   * another process or this one — is noticed on the next read of storage.
   * The wait polls storage itself, so it works on an engine that was never
   * started, and outlives `stop()`: pass a `signal` to bound it. Throws
   * {@link RunNotFoundError} for an unknown run.
   *
   * ```ts
   * const { id } = await engine.enqueue('report', input)
   * const run = await engine.waitForRun(id, { signal: AbortSignal.timeout(30_000) })
   * if (run.status === 'completed') send(run.output)
   * ```
   */
  waitForRun(runId: string, options?: WaitForRunOptions): Promise<WorkflowRun>
  /** How many runs are in each status, optionally for one workflow. */
  countRuns(filter?: CountRunsFilter): Promise<RunCounts>
  /**
   * Delete finished runs older than `olderThan`, with their steps and events.
   * Runs that can still progress are never touched. Resolves to how many runs
   * were deleted.
   *
   * Deletes in small batches, so on SQLite it never holds the write lock long
   * enough to stall workers sharing the database. Safe to call on a schedule.
   */
  prune(options: PruneOptions): Promise<number>
  /**
   * Re-queue a `failed` or `cancelled` run so the engine picks it up again,
   * resuming from the failed step (completed steps are skipped on replay).
   * Returns true if the run was resumable, false otherwise.
   */
  resume(runId: string): Promise<boolean>
  /** Cancel a pending or running workflow. Returns true if cancelled. */
  cancel(runId: string): Promise<boolean>
  /**
   * Deliver an external event to a run that is (or will be) waiting on
   * `waitForEvent(eventName)`. The payload — validated against that wait's
   * schema, if any — becomes the wait's result and the next step's `prev`.
   * Delivery is durable and order-independent: an event sent before the run
   * reaches the wait is buffered and consumed when it gets there. Returns false
   * if the run does not exist or has already finished (completed / failed /
   * cancelled); throws if the workflow has no such event step or the payload
   * fails schema validation.
   */
  sendEvent(runId: string, eventName: string, payload: PersistedValue): Promise<boolean>
  /**
   * Register a recurring schedule, durably. Returns its key.
   *
   * The schedule is stored, not held as an in-process timer, so it survives a
   * restart or a deploy: any engine instance running the workflow picks up the
   * next firing. Registering the same key again updates that schedule in place
   * rather than creating a second one, which makes calling this at startup on
   * every instance the intended usage.
   *
   * Each due firing is claimed atomically and its next occurrence advanced in
   * the same transaction, so a schedule shared by N instances still produces
   * one run per interval rather than N.
   *
   * Occurrences missed while every instance was down are **skipped, not
   * backfilled**: the schedule fires once on return and resumes its cadence.
   *
   * Fires on a fixed interval (`60_000`, or `{ every: '1h' }`) or a cron
   * expression (`{ cron: '0 9 * * 1-5' }`, evaluated in UTC).
   */
  schedule<TName extends string & keyof TWorkflowMap>(
    workflowName: TName,
    input: TWorkflowMap[TName],
    spec: ScheduleSpec,
    options?: ScheduleOptions,
  ): Promise<string>
  /**
   * Remove a durable schedule by key. Returns false if no such schedule
   * existed. Because schedules are shared, this stops it for every instance.
   */
  unschedule(key: string): Promise<boolean>
  /** All registered schedules, ordered by key. */
  listSchedules(): Promise<readonly WorkflowSchedule[]>
  /**
   * Subscribe to a live, pull-based stream of execution events
   * ({@link EngineEvent}). Use it to consume step/run results as they happen,
   * with optional backpressure:
   *
   * ```ts
   * for await (const event of engine.stream()) {
   *   if (event.type === 'runComplete') process(event.output)
   * }
   * ```
   *
   * Each call returns an independent stream. Breaking out of the loop, or
   * disposing the stream, unsubscribes automatically.
   */
  stream(options?: StreamOptions): ResultStream
  /**
   * Claim runs into the free `concurrency` slots, execute them, and resolve once
   * they have finished. Useful for tests and CLI tools; `start()` does this
   * continuously.
   */
  tick(): Promise<void>
  /**
   * Initialize storage and start executing runs. Call once at startup.
   *
   * Up to `concurrency` runs execute at once, and a slot is refilled as soon as
   * its run finishes. Work this instance enqueues, resumes, or sends an event to
   * starts immediately; every `pollIntervalMs` the engine also checks storage
   * for work it was not told about directly — runs from other processes,
   * elapsed sleeps and wait timeouts, due schedules, and stale runs.
   */
  start(pollIntervalMs?: number): Promise<void>
  /**
   * Stop the engine, abort in-flight runs, and wait for its in-flight claims
   * and runs to settle. Aborted runs are left `running`, to be reclaimed.
   *
   * Durable schedules are deliberately left registered — they belong to the
   * storage, not to this instance, and stopping one worker must not cancel a
   * schedule the rest of the fleet is still serving. Use `unschedule()` to
   * remove one.
   */
  stop(): Promise<void>
}

/** What one pass of claiming started, and the claim failure that ended it early, if any. */
interface ClaimBatch {
  started: Promise<void>[]
  failure: { error: unknown } | null
}

interface ActiveRunState {
  leaseId: string
  signals: RunSignals
  heartbeatTimer: ReturnType<typeof setInterval> | null
  heartbeatInFlight: boolean
}

/**
 * Create a workflow engine that connects workflows to storage and handles execution.
 *
 * @example
 * ```ts
 * const engine = createEngine({
 *   storage: new SQLiteStorage('./reflow.db'),
 *   workflows: [myWorkflow],
 * })
 * await engine.start()
 * ```
 */
export function createEngine<const TWorkflows extends readonly AnyWorkflow[]>(
  config: EngineConfig<TWorkflows>,
): Engine<WorkflowInputMap<TWorkflows>> {
  const {
    storage: rawStorage,
    workflows,
    hooks,
    concurrency = 1,
    runLeaseDurationMs = 30_000,
    heartbeatIntervalMs = defaultHeartbeatInterval(runLeaseDurationMs),
  } = config

  if (!Number.isInteger(concurrency) || concurrency < 1) {
    throw new ConfigError('Engine concurrency must be a positive integer')
  }

  if (!Number.isFinite(runLeaseDurationMs) || runLeaseDurationMs <= 0) {
    throw new ConfigError('Engine runLeaseDurationMs must be a positive number')
  }

  if (!Number.isFinite(heartbeatIntervalMs) || heartbeatIntervalMs <= 0) {
    throw new ConfigError('Engine heartbeatIntervalMs must be a positive number')
  }

  if (heartbeatIntervalMs >= runLeaseDurationMs) {
    throw new ConfigError('Engine heartbeatIntervalMs must be smaller than runLeaseDurationMs')
  }

  // Driver failures become StorageError at the boundary, so nothing downstream
  // has to know which SQLite binding raised them.
  const storage = translateStorageErrors(rawStorage)

  const registry = new Map<string, AnyWorkflow>()
  const activeRuns = new Map<string, ActiveRunState>()
  const subscribers = new Set<AbortableSubscriber<EngineEvent>>()
  let running = false
  let timer: ReturnType<typeof setInterval> | null = null
  let stopGeneration = 0
  /** Runs this instance is executing. Each holds one `concurrency` slot. */
  const executions = new Set<Promise<void>>()
  /**
   * Slots reserved by claims that are awaiting storage, or that have claimed a
   * run not yet started. Shared by every caller — the pool and any manual
   * `tick()` — so between them they never hold more than `concurrency`.
   */
  let reservedSlots = 0
  /**
   * Every background operation a `stop()` must outlast — claims, schedule
   * passes, executions — with the stop generation it began in.
   */
  const pendingWork = new Map<Promise<unknown>, number>()
  let fillInFlight = false
  let refillRequested = false
  let pollInFlight = false
  /** Wake-ups for `waitForRun()` callers, keyed by the run they wait on. */
  const finishWaiters = new Map<string, Set<() => void>>()
  /** Soon-due wake times of runs this instance suspended, ascending. See {@link scheduleWake}. */
  const wakeTimes: number[] = []
  let wakeTimer: ReturnType<typeof setTimeout> | null = null

  for (const wf of workflows) {
    if (registry.has(wf.name)) {
      throw new DuplicateWorkflowError(wf.name)
    }

    registry.set(wf.name, wf)
  }

  const workflowNames = Array.from(registry.keys())

  async function enqueue(
    workflowName: string,
    input: PersistedValue,
    options?: EnqueueOptions,
  ): Promise<WorkflowRun> {
    const wf = registry.get(workflowName)
    if (!wf) throw new WorkflowNotFoundError(workflowName)

    const idempotencyKey = normalizeIdempotencyKey(options?.idempotencyKey)
    const parsedInput = wf.parseInput(input)
    const now = Date.now()
    const startAt = startTimeOf(options, now)
    const delayed = startAt > now

    const run: WorkflowRun = {
      id: randomUUID(),
      workflow: workflowName,
      input: parsedInput,
      idempotencyKey,
      // A delayed run waits as a sleeper does, so every adapter already claims
      // it once it is due.
      status: delayed ? 'sleeping' : 'pending',
      createdAt: now,
      updatedAt: now,
      startedAt: null,
      finishedAt: null,
      output: undefined,
      error: null,
      wakeAt: delayed ? startAt : null,
      waitingFor: null,
    }

    const { run: storedRun, created } = await storage.createRun(run)

    if (!created && idempotencyKey && !persistedValuesEqual(storedRun.input, parsedInput)) {
      throw new IdempotencyConflictError(workflowName, idempotencyKey)
    }

    if (created) {
      if (delayed) scheduleWake(startAt)
      else requestFill()
    }

    return storedRun
  }

  async function getRunStatus(runId: string): Promise<RunInfo | null> {
    const run = await storage.getRun(runId)
    if (!run) return null

    const steps = await storage.getStepResults(runId)
    return { run, steps }
  }

  async function listRuns(filter?: ListRunsFilter): Promise<WorkflowRun[]> {
    if (
      filter?.limit !== undefined &&
      (!Number.isInteger(filter.limit) || filter.limit < 1)
    ) {
      throw new ConfigError('listRuns limit must be a positive integer')
    }
    return storage.listRuns(filter)
  }

  async function waitForRun(runId: string, options: WaitForRunOptions = {}): Promise<WorkflowRun> {
    const { signal } = options
    const pollIntervalMs = options.pollIntervalMs ?? 1000
    if (!isTimerDelay(pollIntervalMs)) {
      throw new ConfigError(`waitForRun pollIntervalMs must be a positive number of milliseconds up to ${MAX_TIMER_DELAY_MS}`)
    }

    for (;;) {
      if (signal?.aborted) {
        throw toError(signal.reason)
      }

      // Armed before the read, so a run finishing between the two still wakes us.
      const wake = armFinishWait(runId, pollIntervalMs, signal)
      let run: WorkflowRun | null
      try {
        run = await storage.getRun(runId)
      } catch (error) {
        wake.cancel()
        throw error
      }

      if (!run) {
        wake.cancel()
        throw new RunNotFoundError(runId)
      }
      if (isTerminalStatus(run.status)) {
        wake.cancel()
        return run
      }

      await wake.settled
    }
  }

  /**
   * A promise that resolves when `runId` finishes on this engine, after
   * `timeoutMs` (for runs finished elsewhere), or when `signal` aborts —
   * whichever comes first. `cancel()` disarms it.
   *
   * It never rejects. An abort that lands while the caller is still reading
   * storage would otherwise reject a promise nobody is waiting on yet — an
   * unhandled rejection — so the caller checks `signal.aborted` itself.
   */
  function armFinishWait(
    runId: string,
    timeoutMs: number,
    signal: AbortSignal | undefined,
  ): { settled: Promise<void>; cancel(): void } {
    let disarm = noop
    const settled = new Promise<void>((resolve) => {
      const waiters = finishWaiters.get(runId) ?? new Set<() => void>()
      finishWaiters.set(runId, waiters)

      const wake = () => {
        disarm()
        resolve()
      }
      const timer = setTimeout(wake, timeoutMs)

      disarm = () => {
        clearTimeout(timer)
        waiters.delete(wake)
        if (waiters.size === 0 && finishWaiters.get(runId) === waiters) {
          finishWaiters.delete(runId)
        }
        signal?.removeEventListener('abort', wake)
      }

      waiters.add(wake)
      signal?.addEventListener('abort', wake, { once: true })
    })

    return { settled, cancel: () => disarm() }
  }

  function notifyFinished(runId: string): void {
    const waiters = finishWaiters.get(runId)
    if (!waiters) {
      return
    }
    for (const wake of [...waiters]) {
      wake()
    }
  }

  async function prune(options: PruneOptions): Promise<number> {
    const olderThanMs = parseDuration(options.olderThan)
    const statuses = options.statuses ?? TERMINAL_STATUSES
    // The types already forbid a live status; this guards JavaScript callers,
    // for whom deleting a run mid-flight would be silent data loss.
    for (const status of statuses) {
      if (!TERMINAL_STATUSES.includes(status)) {
        throw new ConfigError(`prune() only deletes finished runs; "${status}" is not a finished status`)
      }
    }

    const finishedBefore = Date.now() - olderThanMs
    let deleted = 0
    for (;;) {
      const batch = await storage.deleteRuns({ finishedBefore, statuses, limit: PRUNE_BATCH_SIZE })
      deleted += batch
      if (batch < PRUNE_BATCH_SIZE) {
        return deleted
      }
    }
  }

  async function resume(runId: string): Promise<boolean> {
    const run = await storage.getRun(runId)
    if (!run) {
      return false
    }
    // Re-queuing a run whose workflow this engine doesn't know would leave it
    // pending but unclaimable (a zombie), since claimNextRun only asks for
    // registered workflows. Fail loudly instead.
    if (!registry.has(run.workflow)) {
      throw new WorkflowNotFoundError(run.workflow)
    }
    const requeued = await storage.requeueRun(runId)
    if (requeued) {
      requestFill()
    }
    return requeued
  }

  /**
   * Dispatch a lifecycle event to the user hook and every active stream, then
   * wait for them all to settle. Awaiting here is what lets an `async` hook or a
   * backpressured stream pace the engine. Neither a throwing hook nor a stream
   * may affect engine state, so all errors are contained — except an abort of
   * `signal`, which is a control-flow signal and propagates.
   */
  async function emit(event: EngineEvent, signal: AbortSignal): Promise<void> {
    try {
      await runWithSignal(
        () => Promise.resolve(dispatchHook(event)).then(noop),
        signal,
      )
    } catch (error) {
      if (signal.aborted) {
        throw toError(signal.reason)
      }
      // Hooks are observers and must not affect engine state — but the failure
      // is still reported, so a throwing hook is diagnosable rather than silent.
      reportError(new HookError(`${event.type} hook`, { cause: toError(error) }))
    }

    if (subscribers.size === 0) {
      return
    }

    const deliveries = Array.from(
      subscribers,
      (subscriber) => subscriber.push(cloneEngineEvent(event, 'Stream event'), signal),
    )

    try {
      await runWithSignal(
        () => Promise.allSettled(deliveries).then(noop),
        signal,
      )
    } catch (error) {
      if (signal.aborted) {
        throw toError(signal.reason)
      }
      // Stream delivery is observational and must not affect engine state.
      reportError(new HookError(`${event.type} stream delivery`, { cause: toError(error) }))
    }
  }

  function dispatchHook(event: EngineEvent): unknown {
    const copy = cloneEngineEvent(event, 'Hook event')
    switch (copy.type) {
      case 'runStart':
        return hooks?.onRunStart?.(copy)
      case 'stepStart':
        return hooks?.onStepStart?.(copy)
      case 'stepSkipped':
        return hooks?.onStepSkipped?.(copy)
      case 'stepComplete':
        return hooks?.onStepComplete?.(copy)
      case 'runComplete':
        return hooks?.onRunComplete?.(copy)
      case 'runFailed':
        return hooks?.onRunFailed?.(copy)
      default:
        return assertNever(copy, 'engine event')
    }
  }

  /**
   * Report a background error to `onError`.
   *
   * This is the terminal error handler: there is no caller left to propagate to,
   * so an `onError` that itself throws has nowhere to go and is dropped. Keeping
   * that failure contained here is deliberate — the alternative is an unhandled
   * rejection that takes down the host process.
   */
  function reportError(error: unknown): void {
    const err = toError(error)
    void (async () => {
      try {
        await hooks?.onError?.(err)
      } catch {
        // Deliberate terminal swallow — see above.
      }
    })()
  }

  function createStream(options?: StreamOptions): ResultStream {
    const capacity = options?.bufferSize ?? Number.POSITIVE_INFINITY
    if (
      capacity !== Number.POSITIVE_INFINITY &&
      (!Number.isInteger(capacity) || capacity < 0)
    ) {
      throw new ConfigError('Stream bufferSize must be a non-negative integer or Infinity')
    }

    let subscriber!: AbortableSubscriber<EngineEvent>
    const channel = createBoundedAsyncIterator<EngineEvent>(
      capacity,
      () => subscribers.delete(subscriber),
    )
    subscriber = channel.subscriber
    subscribers.add(subscriber)
    return channel.iterator
  }

  /**
   * Build the per-run context handed to every {@link UnitExecutor}.
   *
   * `saveStep` reuses the replayed row's `id` and `createdAt`, so re-executing a
   * step after a reclaim updates that step's row in place rather than appending
   * a second row under the same name.
   */
  function createExecutionContext(
    run: ClaimedRun,
    workflow: AnyWorkflow,
    activeRun: ActiveRunState,
    existingSteps: readonly StepResult[],
  ): ExecutionContext {
    const replay = new Map(existingSteps.map((step) => [step.name, step]))
    const steps: Record<string, PersistedValue> = {}
    const observerSignal = activeRun.signals.observer.signal

    const trySaveStep = (record: StepRecord): Promise<boolean> => {
      const existing = replay.get(record.name)
      const now = Date.now()

      return storage.saveStepResult({
        id: existing?.id ?? randomUUID(),
        runId: run.id,
        name: record.name,
        status: record.status,
        output: record.output,
        error: record.error ?? null,
        attempts: record.attempts ?? 0,
        createdAt: existing?.createdAt ?? now,
        updatedAt: now,
        // A suspended sleep or wait finishing on a later claim keeps the time
        // it began; an executor re-running a step passes a fresh one.
        startedAt: record.startedAt ?? existing?.startedAt ?? now,
        history: record.history ? [...record.history] : existing?.history ?? [],
      }, run.leaseId)
    }

    return {
      run,
      workflow,
      storage,
      signals: activeRun.signals,
      durableBackoff: running,
      replay,
      steps,
      emit: (event) => emit(event, observerSignal),
      trySaveStep,
      async saveStep(record) {
        const saved = await trySaveStep(record)
        if (!saved) {
          throw new LeaseExpiredError(run.id)
        }
      },
      snapshotSteps: () => deepFreeze(structuredClone(steps)),
    }
  }

  /**
   * Route one execution unit to its executor.
   *
   * Exhaustive over `ExecutionUnit['kind']`: adding a new kind is a compile
   * error here until an executor is wired up for it.
   */
  function dispatchUnit(
    unit: ExecutionUnit,
    ctx: ExecutionContext,
    prev: PersistedValue,
  ): Promise<UnitOutcome> {
    switch (unit.kind) {
      case 'step':
        return stepExecutor.execute(unit, ctx, prev)
      case 'parallel':
        return parallelExecutor.execute(unit, ctx, prev)
      case 'sleep':
        return sleepExecutor.execute(unit, ctx, prev)
      case 'waitForEvent':
        return waitForEventExecutor.execute(unit, ctx, prev)
      default:
        return assertNever(unit, 'execution unit')
    }
  }

  /**
   * Drive a claimed run through its execution units.
   *
   * Each unit reduces to a {@link UnitOutcome}, so this stays a flat loop: the
   * decisions about replay, persistence, and suspension live in the executors,
   * and the run-level transitions (complete, fail, halt) live here.
   */
  async function executeRun(run: ClaimedRun): Promise<void> {
    const wf = registry.get(run.workflow)
    if (!wf) return

    const activeRun = registerActiveRun(run)
    const runSignal = activeRun.signals.run.signal

    try {
      const currentRun = await storage.getRun(run.id)
      if (!currentRun || currentRun.status !== 'running') {
        return
      }

      await emit({ type: 'runStart', runId: run.id, workflow: run.workflow }, activeRun.signals.observer.signal)

      const existingSteps = await storage.getStepResults(run.id)
      const ctx = createExecutionContext(run, wf, activeRun, existingSteps)

      let prev: PersistedValue = undefined

      for (const unit of wf.executionUnits) {
        // A cancellation or engine stop can land on any await between units.
        // Check once here rather than in every executor.
        if (runSignal.aborted) {
          const latest = await storage.getRun(run.id)
          if (!latest || latest.status === 'cancelled') {
            return
          }
        }

        const outcome = await dispatchUnit(unit, ctx, prev)

        switch (outcome.kind) {
          case 'advance':
            prev = outcome.output
            continue
          case 'passthrough':
            continue
          case 'halt':
            return
          case 'suspend':
            scheduleWake(outcome.wakeAt)
            return
          case 'complete':
            await finishRun(run, ctx, outcome.output)
            return
          case 'failed':
            await failRun(run, wf, ctx, outcome.stepName, outcome.error)
            return
        }
      }

      const latest = await storage.getRun(run.id)
      if (!latest || latest.status === 'cancelled') {
        return
      }

      await finishRun(run, ctx, prev)
    } catch (error) {
      // Control-flow signals (cancel, stop, lease loss) unwind the run without
      // recording a failure; anything else is a genuine bug and propagates.
      if (!(error instanceof RunControlError)) {
        throw error
      }
    } finally {
      cleanupActiveRun(run.id, activeRun)
    }
  }

  /** Mark a run completed and announce it. No-ops if the lease was lost. */
  async function finishRun(run: ClaimedRun, ctx: ExecutionContext, output: PersistedValue): Promise<void> {
    const completed = await storage.completeRun(run.id, run.leaseId, output)
    if (!completed) {
      return
    }
    // Waiters wake only after the run's hooks have had their turn, so code
    // that awaits a run and then stops the engine cannot cut them short.
    try {
      await ctx.emit({ type: 'runComplete', runId: run.id, workflow: run.workflow, output })
    } finally {
      notifyFinished(run.id)
    }
  }

  /** Mark a run failed, announce it, then invoke the workflow's `onFailure`. No-ops if the lease was lost. */
  async function failRun(
    run: ClaimedRun,
    wf: AnyWorkflow,
    ctx: ExecutionContext,
    stepName: string,
    error: Error,
  ): Promise<void> {
    const failed = await storage.failRun(run.id, run.leaseId, {
      stepName,
      message: errorMessage(error),
      code: errorCode(error),
    })
    if (!failed) {
      return
    }
    // Waiters wake once `onFailure` has run, so awaiting a run and then
    // stopping the engine cannot skip its compensation.
    try {
      await ctx.emit({ type: 'runFailed', runId: run.id, workflow: run.workflow, stepName, error })

      if (wf.failureHandler) {
        try {
          await wf.failureHandler({ error, stepName, input: run.input })
        } catch (failureError) {
          // Compensation logic must not change the run's outcome — it is already
          // failed — but a broken `onFailure` is reported rather than lost.
          reportError(new HookError('onFailure handler', { cause: toError(failureError) }))
        }
      }
    } finally {
      notifyFinished(run.id)
    }
  }

  async function cancel(runId: string): Promise<boolean> {
    // One conditional write rather than read-then-write, so a run finishing
    // at the same moment is never reported as cancelled.
    const cancelled = await storage.cancelRun(runId)
    if (cancelled) {
      abortActiveRun(runId, new RunCancelledError(runId))
      notifyFinished(runId)
    }

    return cancelled
  }

  async function sendEvent(runId: string, eventName: string, payload: PersistedValue): Promise<boolean> {
    const run = await storage.getRun(runId)
    if (!run) {
      return false
    }
    // Don't buffer events for a run that can never consume them (it would leak
    // and mislead the caller into thinking delivery was meaningful).
    if (run.status === 'completed' || run.status === 'failed' || run.status === 'cancelled') {
      return false
    }

    const wf = registry.get(run.workflow)
    const unit = wf?.executionUnits.find((u) => u.kind === 'waitForEvent' && u.name === eventName)
    if (!unit || unit.kind !== 'waitForEvent') {
      throw new ConfigError(`Workflow "${run.workflow}" has no waitForEvent step named "${eventName}"`)
    }

    const validated = validateEventPayload(eventName, unit.schema, payload)
    const delivered = await storage.deliverEvent(runId, eventName, validated)
    if (delivered) {
      requestFill()
    }
    return delivered
  }

  async function schedule(
    workflowName: string,
    input: PersistedValue,
    spec: ScheduleSpec,
    options?: ScheduleOptions,
  ): Promise<string> {
    const recurrence = parseScheduleSpec(spec)

    const wf = registry.get(workflowName)
    if (!wf) throw new WorkflowNotFoundError(workflowName)

    const parsedInput = wf.parseInput(input)
    const key = options?.key ?? deriveScheduleKey(workflowName, recurrence, parsedInput)
    const now = Date.now()

    // `nextRunAt` here is only the value used when the schedule is new (or its
    // recurrence changed) — storage preserves an existing cadence otherwise.
    const stored = await storage.upsertSchedule({
      key,
      workflow: workflowName,
      input: parsedInput,
      recurrence,
      nextRunAt: firstOccurrence(recurrence, now),
      createdAt: now,
      updatedAt: now,
    })

    return stored.key
  }

  function unschedule(key: string): Promise<boolean> {
    return storage.deleteSchedule(key)
  }

  function listSchedules(): Promise<readonly WorkflowSchedule[]> {
    return storage.listSchedules()
  }

  /**
   * Enqueue every schedule that has come due.
   *
   * Each claim advances that schedule past `now` inside the same transaction,
   * so this loop terminates after at most one firing per registered schedule
   * and two instances ticking together cannot both fire the same occurrence.
   */
  async function processDueSchedules(): Promise<void> {
    const now = Date.now()

    try {
      for (;;) {
        const due = await storage.claimDueSchedule(workflowNames, now)
        if (!due) {
          return
        }

        try {
          // Keyed by the occurrence it fired for. The atomic claim already makes
          // this a single firing; the key is what keeps that true if a retry ever
          // replays the enqueue.
          await enqueue(due.workflow, due.input, {
            idempotencyKey: `reflow.schedule:${due.key}:${due.nextRunAt}`,
          })
        } catch (error) {
          // One bad schedule must not stop the others from firing.
          reportError(error)
        }
      }
    } catch (error) {
      // Scheduling is auxiliary, and this runs first in every tick. A failure
      // reading the schedule table must not stop the engine from claiming and
      // executing runs, which is its actual job.
      reportError(error)
    }
  }

  /**
   * Track a background operation so `stop()` can wait for it to settle.
   * Returns the same promise, so callers keep its result and its rejection.
   */
  function track<T>(work: Promise<T>): Promise<T> {
    pendingWork.set(work, stopGeneration)
    const forget = () => {
      pendingWork.delete(work)
    }
    void work.then(forget, forget)
    return work
  }

  /**
   * Claim runs into this instance's free slots, then start executing them
   * without waiting for any to finish.
   *
   * Slots are reserved before each claim is awaited, so callers racing each
   * other — the pool refilling while a manual `tick()` runs — can never hold
   * more than `concurrency` runs between them. The whole batch is claimed
   * before any of it starts, so the claims run back to back instead of
   * interleaving with the executions they start.
   *
   * A failed claim ends the batch but is returned rather than thrown: the runs
   * already claimed must still start, or they would sit stranded until their
   * leases expired.
   */
  async function claimIntoFreeSlots(): Promise<ClaimBatch> {
    const generation = stopGeneration
    const claimed: ClaimedRun[] = []
    let failure: ClaimBatch['failure'] = null

    try {
      // Stops claiming the moment stop() lands, rather than filling every slot
      // with runs that would only sit out their leases.
      while (registry.size > 0 && generation === stopGeneration && executions.size + reservedSlots < concurrency) {
        // The slot stays reserved from here until the run starts executing (or
        // the claim comes back empty), so a concurrent caller cannot count it
        // as free while this batch is still being claimed.
        reservedSlots++
        let run: ClaimedRun | null
        try {
          run = await track(storage.claimNextRun(workflowNames, Date.now() - runLeaseDurationMs))
        } catch (error) {
          reservedSlots--
          throw error
        }

        if (!run) {
          reservedSlots--
          break
        }

        claimed.push(run)
      }
    } catch (error) {
      failure = { error }
    }

    reservedSlots -= claimed.length
    // Runs claimed as stop() lands are left to their leases, like every other
    // run a stopped instance holds, and reclaimed once those expire.
    const started = generation === stopGeneration ? claimed.map(execute) : []
    return { started, failure }
  }

  /** Execute a claimed run in its slot, freeing the slot and refilling it when the run ends. */
  function execute(run: ClaimedRun): Promise<void> {
    const execution: Promise<void> = executeRun(run).finally(() => {
      executions.delete(execution)
      requestFill()
    })
    executions.add(execution)
    return track(execution)
  }

  /**
   * Top up free slots from storage, while the polling loop is running.
   *
   * Requested on every poll, whenever a run ends, and whenever this instance
   * enqueues, resumes, or delivers an event to a run — so a free slot is filled
   * as soon as there is work for it, not at the next poll. Requests that land
   * mid-fill are folded into one more pass rather than claiming concurrently.
   */
  function requestFill(): void {
    if (!running) {
      return
    }

    if (fillInFlight) {
      refillRequested = true
      return
    }

    fillInFlight = true
    refillRequested = false
    void claimIntoFreeSlots()
      .then(({ started, failure }) => {
        // Nothing awaits the pool, so its failures are reported here.
        for (const execution of started) {
          execution.catch(reportError)
        }
        if (failure) {
          reportError(failure.error)
        }
      })
      .finally(() => {
        fillInFlight = false
        if (refillRequested) {
          requestFill()
        }
      })
  }

  /**
   * Refill slots when a run this instance suspended is due to wake, rather
   * than leaving it for the next poll.
   *
   * Polling already finds every due run eventually; this only makes a short
   * sleep, wait timeout, or retry backoff resume on time instead of up to
   * `pollIntervalMs` late. Wakes further out than {@link LOCAL_WAKE_HORIZON_MS}
   * are left to polling, where that delay is negligible, so the list stays
   * small however many long sleeps are outstanding.
   */
  function scheduleWake(wakeAt: number | null): void {
    if (wakeAt === null || !running || wakeAt - Date.now() > LOCAL_WAKE_HORIZON_MS) {
      return
    }

    const index = sortedIndex(wakeTimes, wakeAt)
    wakeTimes.splice(index, 0, wakeAt)
    if (index === 0) {
      armWakeTimer()
    }
  }

  function armWakeTimer(): void {
    if (wakeTimer) {
      clearTimeout(wakeTimer)
      wakeTimer = null
    }

    const [next] = wakeTimes
    if (next === undefined) {
      return
    }

    // A millisecond late rather than early: a timer can fire just before its
    // target, when the run would not yet be claimable.
    wakeTimer = setTimeout(() => {
      wakeTimer = null
      const now = Date.now()
      while (wakeTimes[0] !== undefined && wakeTimes[0] <= now) {
        wakeTimes.shift()
      }
      requestFill()
      armWakeTimer()
    }, Math.max(0, next - Date.now()) + 1)
  }

  /** One polling cycle: fire due schedules, then fill free slots. Cycles never overlap. */
  async function poll(): Promise<void> {
    if (pollInFlight || !running) {
      return
    }

    pollInFlight = true
    try {
      await track(processDueSchedules())
    } finally {
      pollInFlight = false
    }

    requestFill()
  }

  async function tick(): Promise<void> {
    if (registry.size === 0) {
      return
    }

    await track(processDueSchedules())
    const { started, failure } = await claimIntoFreeSlots()
    await Promise.all(started)
    if (failure) {
      throw failure.error
    }
  }

  async function start(pollIntervalMs = 1000): Promise<void> {
    if (!isTimerDelay(pollIntervalMs)) {
      throw new ConfigError(`Engine pollIntervalMs must be a positive number of milliseconds up to ${MAX_TIMER_DELAY_MS}`)
    }

    await storage.initialize()
    if (running) {
      return
    }

    running = true

    const triggerPoll = () => {
      void poll().catch(reportError)
    }

    triggerPoll()
    timer = setInterval(triggerPoll, pollIntervalMs)
  }

  async function stop(): Promise<void> {
    const stopping = stopGeneration++
    running = false

    if (timer) {
      clearInterval(timer)
      timer = null
    }

    if (wakeTimer) {
      clearTimeout(wakeTimer)
      wakeTimer = null
    }
    wakeTimes.length = 0

    for (const [runId, activeRun] of activeRuns) {
      abortActiveRun(runId, new RunControlError('Engine stopped'))
      cleanupActiveRun(runId, activeRun)
    }

    // End all streams and unblock any producer paused on backpressure, so a
    // pending run can settle instead of deadlocking on a consumer that is gone.
    for (const subscriber of subscribers) {
      subscriber.close()
    }
    subscribers.clear()

    // Settling one operation can start another (a run ending requests a fill,
    // which no-ops once stopped but may already be claiming), so drain until
    // nothing is left rather than snapshotting once. Only work begun before
    // this stop counts: an engine started again meanwhile must not keep a
    // stop() pending for as long as it stays busy.
    for (;;) {
      const outstanding = [...pendingWork].filter(([, generation]) => generation <= stopping).map(([work]) => work)
      if (outstanding.length === 0) {
        return
      }
      await Promise.allSettled(outstanding)
    }
  }

  function registerActiveRun(run: ClaimedRun): ActiveRunState {
    // This instance reclaimed a run it was still executing: the earlier
    // execution's lease has lapsed (its heartbeats stalled), so it can no
    // longer write anything, and must stop before the new one runs alongside it.
    const existing = activeRuns.get(run.id)
    if (existing) {
      abortRunState(existing, new LeaseExpiredError(run.id))
      cleanupActiveRun(run.id, existing)
    }

    const activeRun: ActiveRunState = {
      leaseId: run.leaseId,
      signals: { run: new AbortController(), observer: new AbortController() },
      heartbeatTimer: null,
      heartbeatInFlight: false,
    }

    activeRuns.set(run.id, activeRun)
    startHeartbeat(run, activeRun)
    return activeRun
  }

  function startHeartbeat(run: ClaimedRun, activeRun: ActiveRunState): void {
    const sendHeartbeat = async () => {
      if (activeRun.heartbeatInFlight || activeRun.signals.run.signal.aborted) {
        return
      }

      activeRun.heartbeatInFlight = true

      try {
        const ok = await storage.heartbeatRun(run.id, activeRun.leaseId)
        if (!ok) {
          abortRunState(activeRun, new LeaseExpiredError(run.id))
        }
      } catch (error) {
        abortRunState(activeRun, toError(error))
      } finally {
        activeRun.heartbeatInFlight = false
      }
    }

    activeRun.heartbeatTimer = setInterval(() => {
      void sendHeartbeat()
    }, heartbeatIntervalMs)
  }

  /**
   * Stop `activeRun`'s heartbeat and forget it — only it. Keyed by the state
   * object rather than the run id, so an execution finishing after this
   * instance has reclaimed the same run cannot tear down the new execution.
   */
  function cleanupActiveRun(runId: string, activeRun: ActiveRunState): void {
    if (activeRun.heartbeatTimer) {
      clearInterval(activeRun.heartbeatTimer)
      activeRun.heartbeatTimer = null
    }

    if (activeRuns.get(runId) === activeRun) {
      activeRuns.delete(runId)
    }
  }

  function abortActiveRun(runId: string, reason: Error): void {
    const activeRun = activeRuns.get(runId)
    if (activeRun) {
      abortRunState(activeRun, reason)
    }
  }

  function abortRunState(activeRun: ActiveRunState, reason: Error): void {
    if (!activeRun.signals.run.signal.aborted) {
      activeRun.signals.run.abort(reason)
    }

    // Only a control-flow abort tears down observers. A genuine error (e.g. a
    // heartbeat storage failure) still needs its `runFailed` event delivered.
    if (
      reason instanceof RunControlError &&
      !activeRun.signals.observer.signal.aborted
    ) {
      activeRun.signals.observer.abort(reason)
    }
  }

  return {
    enqueue,
    getRunStatus,
    listRuns,
    waitForRun,
    countRuns: (filter?: CountRunsFilter) => storage.countRuns(filter),
    prune,
    resume,
    cancel,
    sendEvent,
    schedule,
    unschedule,
    listSchedules,
    stream: createStream,
    tick,
    start,
    stop,
  } as Engine<WorkflowInputMap<TWorkflows>>
}

/** Validate an event payload against the wait's schema (if any), returning the parsed value. */
function validateEventPayload(
  eventName: string,
  schema: StandardSchemaV1<PersistedValue> | undefined,
  payload: PersistedValue,
): PersistedValue {
  if (!schema) {
    return payload
  }
  const result = schema['~standard'].validate(payload)
  if (result instanceof Promise) {
    throw new TypeError(`Async schema validation is not supported (event "${eventName}")`)
  }
  if (result.issues) {
    const messages = result.issues.map((issue) => issue.message).join(', ')
    throw new ValidationError(`Event "${eventName}" payload failed validation: ${messages}`, result.issues)
  }
  return result.value as PersistedValue
}

function runWithSignal<T>(
  promiseFactory: () => Promise<T>,
  signal: AbortSignal,
): Promise<T> {
  if (signal.aborted) {
    return Promise.reject(toError(signal.reason))
  }

  return new Promise<T>((resolve, reject) => {
    const onAbort = () => {
      cleanup()
      reject(toError(signal.reason))
    }

    const cleanup = () => {
      signal.removeEventListener('abort', onAbort)
    }

    signal.addEventListener('abort', onAbort, { once: true })

    Promise.resolve()
      .then(promiseFactory)
      .then(
        (value) => {
          cleanup()
          resolve(value)
        },
        (error) => {
          cleanup()
          reject(error)
        },
      )
  })
}

/**
 * Derive a schedule identity that every engine instance computes identically.
 *
 * Hashes the canonical (key-order-independent) form of the input together with
 * the workflow name and interval, so two processes registering the same logical
 * schedule agree without having to coordinate or configure anything.
 */
function deriveScheduleKey(
  workflowName: string,
  recurrence: ScheduleRecurrence,
  input: PersistedValue,
): string {
  const canonicalInput = canonicalizePersistedValue(input, 'Schedule input')
  const cadence = recurrence.kind === 'cron' ? `cron:${recurrence.expression}` : `every:${recurrence.intervalMs}`

  return createHash('sha256')
    .update(`${workflowName}\u0000${cadence}\u0000${canonicalInput}`)
    .digest('base64url')
    .slice(0, 22)
}


/**
 * When a run enqueued with these options should start, in epoch ms.
 *
 * @throws {ConfigError} if both `delay` and `startAt` are given, or either is invalid.
 */
function startTimeOf(options: EnqueueOptions | undefined, now: number): number {
  const { delay, startAt } = options ?? {}
  if (delay !== undefined && startAt !== undefined) {
    throw new ConfigError('enqueue: pass delay or startAt, not both')
  }
  if (delay !== undefined) {
    return now + parseDuration(delay)
  }
  if (startAt !== undefined) {
    const time = startAt instanceof Date ? startAt.getTime() : startAt
    if (!Number.isFinite(time)) {
      throw new ConfigError('enqueue: startAt must be a valid Date or epoch milliseconds')
    }
    return time
  }
  return now
}
/**
 * Normalise the caller's recurrence, validating it before anything is stored.
 *
 * Registration is the last point a bad cadence can be reported to a caller —
 * after this the schedule fires unattended — so a malformed cron expression is
 * rejected here rather than on the first tick that tries to advance it.
 */
function parseScheduleSpec(spec: ScheduleSpec): ScheduleRecurrence {
  if (typeof spec === 'number') {
    return intervalRecurrence(spec)
  }

  if (spec.every !== undefined) {
    return intervalRecurrence(typeof spec.every === 'string' ? parseDuration(spec.every) : spec.every)
  }

  if (spec.cron !== undefined) {
    // Parsed eagerly and discarded: this is a validation call, and the stored
    // form stays the expression so it round-trips through storage unchanged.
    parseCron(spec.cron)
    return { kind: 'cron', expression: spec.cron.trim() }
  }

  // Unreachable through the types, but reachable from JavaScript.
  throw new ConfigError('Schedule must specify either `every` or `cron`')
}

function intervalRecurrence(intervalMs: number): ScheduleRecurrence {
  if (!Number.isFinite(intervalMs) || intervalMs <= 0) {
    throw new ConfigError('Schedule intervalMs must be a positive number')
  }

  return { kind: 'interval', intervalMs }
}

function normalizeIdempotencyKey(idempotencyKey?: string): string | null {
  if (idempotencyKey === undefined) {
    return null
  }

  if (idempotencyKey.length === 0) {
    throw new ConfigError('Enqueue idempotencyKey must not be empty')
  }

  return idempotencyKey
}

function defaultHeartbeatInterval(runLeaseDurationMs: number): number {
  return Math.max(1, Math.min(runLeaseDurationMs - 1, Math.floor(runLeaseDurationMs / 3)))
}

/** The error's string `code` — a `ReflowErrorCode`, a Node system code like `ECONNREFUSED` — or `null`. */
function errorCode(error: Error): string | null {
  return 'code' in error && typeof error.code === 'string' ? error.code : null
}

const TERMINAL_STATUSES: readonly TerminalRunStatus[] = ['completed', 'failed', 'cancelled']

function isTerminalStatus(status: RunStatus): status is TerminalRunStatus {
  const terminal: readonly RunStatus[] = TERMINAL_STATUSES
  return terminal.includes(status)
}

/** The longest delay `setTimeout` honours; anything larger silently fires after 1ms. */
const MAX_TIMER_DELAY_MS = 2_147_483_647

function isTimerDelay(ms: number): boolean {
  return Number.isFinite(ms) && ms > 0 && ms <= MAX_TIMER_DELAY_MS
}

/** How far ahead {@link scheduleWake} sets a local timer; later wakes are left to polling. */
const LOCAL_WAKE_HORIZON_MS = 5 * 60_000

/** Where `value` belongs in the ascending `sorted`, after any equal values. */
function sortedIndex(sorted: readonly number[], value: number): number {
  let low = 0
  let high = sorted.length
  while (low < high) {
    const middle = (low + high) >>> 1
    const candidate = sorted[middle]
    if (candidate !== undefined && candidate <= value) {
      low = middle + 1
    } else {
      high = middle
    }
  }
  return low
}

/** Runs deleted per `deleteRuns` call during `prune()`: each batch is one short write transaction. */
const PRUNE_BATCH_SIZE = 500

function noop() {}

function deepFreeze<T extends Record<string, unknown>>(obj: T): T {
  Object.freeze(obj)
  for (const value of Object.values(obj)) {
    if (value !== null && typeof value === 'object' && !Object.isFrozen(value)) {
      deepFreeze(value as Record<string, unknown>)
    }
  }
  return obj
}
