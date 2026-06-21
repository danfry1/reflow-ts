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
  HookError,
  IdempotencyConflictError,
  LeaseExpiredError,
  RunCancelledError,
  RunControlError,
  ValidationError,
  WorkflowNotFoundError,
} from './errors'
import { cloneEngineEvent, type EngineEvent, type EngineHooks } from './events'
import { parallelExecutor } from './execution/parallel'
import { sleepExecutor } from './execution/sleep'
import { stepExecutor } from './execution/step'
import { toError } from './execution/signals'
import type { ExecutionContext, RunSignals, StepRecord, UnitOutcome } from './execution/types'
import { waitForEventExecutor } from './execution/wait-for-event'
import type {
  ClaimedRun,
  ListRunsFilter,
  PersistedValue,
  RunInfo,
  StepResult,
  StorageAdapter,
  WorkflowRun,
  WorkflowSchedule,
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
  /** Maximum runs to process in parallel per tick (default: 1). */
  concurrency?: number
  /** How long a run's lease is valid before another engine can reclaim it (default: 30000). */
  runLeaseDurationMs?: number
  /** How often to renew the lease while a run is executing (default: leaseDuration / 3). */
  heartbeatIntervalMs?: number
}

/** Options for `engine.enqueue()`. */
export interface EnqueueOptions {
  /** Prevents duplicate runs. Same key + same input returns the existing run. Same key + different input throws. */
  idempotencyKey?: string
}

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
   */
  schedule<TName extends string & keyof TWorkflowMap>(
    workflowName: TName,
    input: TWorkflowMap[TName],
    intervalMs: number,
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
  /** Process one batch of pending runs. Useful for tests and CLI tools. */
  tick(): Promise<void>
  /** Initialize storage and start the polling loop. Call once at startup. */
  start(pollIntervalMs?: number): Promise<void>
  /**
   * Stop the polling loop and wait for in-flight ticks to finish.
   *
   * Durable schedules are deliberately left registered — they belong to the
   * storage, not to this instance, and stopping one worker must not cancel a
   * schedule the rest of the fleet is still serving. Use `unschedule()` to
   * remove one.
   */
  stop(): Promise<void>
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
  let tickInFlight = false
  let tickPromise: Promise<void> | null = null
  let stopGeneration = 0

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

    const run: WorkflowRun = {
      id: randomUUID(),
      workflow: workflowName,
      input: parsedInput,
      idempotencyKey,
      status: 'pending',
      createdAt: now,
      updatedAt: now,
    }

    const { run: storedRun, created } = await storage.createRun(run)

    if (!created && idempotencyKey && !persistedValuesEqual(storedRun.input, parsedInput)) {
      throw new IdempotencyConflictError(workflowName, idempotencyKey)
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

  async function resume(runId: string): Promise<boolean> {
    return storage.requeueRun(runId)
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
      }, run.leaseId)
    }

    return {
      run,
      workflow,
      storage,
      signals: activeRun.signals,
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
          case 'suspend':
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
      cleanupActiveRun(run.id)
    }
  }

  /** Mark a run completed and announce it. No-ops if the lease was lost. */
  async function finishRun(run: ClaimedRun, ctx: ExecutionContext, output: PersistedValue): Promise<void> {
    const completed = await storage.updateClaimedRunStatus(run.id, run.leaseId, 'completed')
    if (!completed) {
      return
    }

    await ctx.emit({ type: 'runComplete', runId: run.id, workflow: run.workflow, output })
  }

  /** Mark a run failed, announce it, then invoke the workflow's `onFailure`. No-ops if the lease was lost. */
  async function failRun(
    run: ClaimedRun,
    wf: AnyWorkflow,
    ctx: ExecutionContext,
    stepName: string,
    error: Error,
  ): Promise<void> {
    const failed = await storage.updateClaimedRunStatus(run.id, run.leaseId, 'failed')
    if (!failed) {
      return
    }

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
  }

  async function cancel(runId: string): Promise<boolean> {
    const run = await storage.getRun(runId)
    if (!run || run.status === 'completed' || run.status === 'failed' || run.status === 'cancelled') {
      return false
    }

    const cancelled = await storage.updateRunStatus(runId, 'cancelled')
    if (cancelled) {
      abortActiveRun(runId, new RunCancelledError(runId))
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
    return storage.deliverEvent(runId, eventName, validated)
  }

  async function schedule(
    workflowName: string,
    input: PersistedValue,
    intervalMs: number,
    options?: ScheduleOptions,
  ): Promise<string> {
    if (!Number.isFinite(intervalMs) || intervalMs <= 0) {
      throw new ConfigError('Schedule intervalMs must be a positive number')
    }

    const wf = registry.get(workflowName)
    if (!wf) throw new WorkflowNotFoundError(workflowName)

    const parsedInput = wf.parseInput(input)
    const key = options?.key ?? deriveScheduleKey(workflowName, intervalMs, parsedInput)
    const now = Date.now()

    // `nextRunAt` here is only the value used when the schedule is new (or its
    // interval changed) — storage preserves an existing cadence otherwise.
    const stored = await storage.upsertSchedule({
      key,
      workflow: workflowName,
      input: parsedInput,
      intervalMs,
      nextRunAt: now + intervalMs,
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

  async function tick(): Promise<void> {
    if (registry.size === 0 || tickInFlight) {
      return
    }

    const generation = stopGeneration
    tickInFlight = true
    const promise = (async () => {
      try {
        await processDueSchedules()

        const staleBefore = Date.now() - runLeaseDurationMs
        const runs: ClaimedRun[] = []

        for (let index = 0; index < concurrency; index++) {
          const run = await storage.claimNextRun(workflowNames, staleBefore)
          if (!run) {
            break
          }

          runs.push(run)
        }

        if (generation !== stopGeneration) {
          return
        }

        await Promise.all(runs.map((run) => executeRun(run)))
      } finally {
        tickInFlight = false
        tickPromise = null
      }
    })()
    tickPromise = promise
    await promise
  }

  async function start(pollIntervalMs = 1000): Promise<void> {
    if (!Number.isFinite(pollIntervalMs) || pollIntervalMs <= 0) {
      throw new ConfigError('Engine pollIntervalMs must be a positive number')
    }

    await storage.initialize()
    if (running) {
      return
    }

    running = true

    const triggerPoll = () => {
      void runPollCycle().catch(reportError)
    }

    triggerPoll()
    timer = setInterval(triggerPoll, pollIntervalMs)
  }

  async function runPollCycle(): Promise<void> {
    if (!running) {
      return
    }

    await tick()
  }

  async function stop(): Promise<void> {
    stopGeneration++
    running = false

    if (timer) {
      clearInterval(timer)
      timer = null
    }

    for (const [runId] of activeRuns) {
      abortActiveRun(runId, new RunControlError('Engine stopped'))
      cleanupActiveRun(runId)
    }

    // End all streams and unblock any producer paused on backpressure, so a
    // pending tick can settle instead of deadlocking on a consumer that is gone.
    for (const subscriber of subscribers) {
      subscriber.close()
    }
    subscribers.clear()

    if (tickPromise) {
      await tickPromise.catch(noop)
    }
  }

  function registerActiveRun(run: ClaimedRun): ActiveRunState {
    const existing = activeRuns.get(run.id)
    if (existing) {
      cleanupActiveRun(run.id)
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
          abortActiveRun(run.id, new LeaseExpiredError(run.id))
        }
      } catch (error) {
        abortActiveRun(run.id, toError(error))
      } finally {
        activeRun.heartbeatInFlight = false
      }
    }

    activeRun.heartbeatTimer = setInterval(() => {
      void sendHeartbeat()
    }, heartbeatIntervalMs)
  }

  function cleanupActiveRun(runId: string): void {
    const activeRun = activeRuns.get(runId)
    if (!activeRun) {
      return
    }

    if (activeRun.heartbeatTimer) {
      clearInterval(activeRun.heartbeatTimer)
    }

    activeRuns.delete(runId)
  }

  function abortActiveRun(runId: string, reason: Error): void {
    const activeRun = activeRuns.get(runId)
    if (!activeRun) {
      return
    }

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
  intervalMs: number,
  input: PersistedValue,
): string {
  const canonicalInput = canonicalizePersistedValue(input, 'Schedule input')

  return createHash('sha256')
    .update(`${workflowName}\u0000${intervalMs}\u0000${canonicalInput}`)
    .digest('base64url')
    .slice(0, 22)
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
