import { randomUUID } from 'node:crypto'
import type { StandardSchemaV1 } from '@standard-schema/spec'
import { persistedValuesEqual } from '../storage/codec'
import {
  createBoundedAsyncIterator,
  type AbortableSubscriber,
} from './async-iterator'
import {
  ConfigError,
  DuplicateWorkflowError,
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
  PersistedValue,
  RunInfo,
  StepResult,
  StorageAdapter,
  WorkflowRun,
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
  /** Enqueue a workflow on a recurring interval. Returns a schedule ID. */
  schedule<TName extends string & keyof TWorkflowMap>(
    workflowName: TName,
    input: TWorkflowMap[TName],
    intervalMs: number,
  ): string
  /** Cancel a recurring schedule by ID. */
  unschedule(scheduleId: string): boolean
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
  /** Stop the polling loop, clear all schedules, and wait for in-flight ticks to finish. */
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
    storage,
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

  const registry = new Map<string, AnyWorkflow>()
  const schedules = new Map<string, ReturnType<typeof setInterval>>()
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
    } catch {
      if (signal.aborted) {
        throw toError(signal.reason)
      }
      // Hooks are observers and must not affect engine state.
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
    } catch {
      if (signal.aborted) {
        throw toError(signal.reason)
      }
      // Stream delivery is observational and must not affect engine state.
    }
  }

  function dispatchHook(event: EngineEvent): unknown {
    const copy = cloneEngineEvent(event, 'Hook event')
    switch (copy.type) {
      case 'runStart':
        return hooks?.onRunStart?.(copy)
      case 'stepStart':
        return hooks?.onStepStart?.(copy)
      case 'stepComplete':
        return hooks?.onStepComplete?.(copy)
      case 'runComplete':
        return hooks?.onRunComplete?.(copy)
      case 'runFailed':
        return hooks?.onRunFailed?.(copy)
    }
  }

  /** Report a background error to `onError`, swallowing any error it raises. */
  function reportError(error: unknown): void {
    const err = toError(error)
    void (async () => {
      try {
        await hooks?.onError?.(err)
      } catch { /* onError must not throw */ }
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
      } catch { /* onFailure must not affect engine state */ }
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

  function schedule(
    workflowName: string,
    input: PersistedValue,
    intervalMs: number,
  ): string {
    if (!Number.isFinite(intervalMs) || intervalMs <= 0) {
      throw new ConfigError('Schedule intervalMs must be a positive number')
    }

    const wf = registry.get(workflowName)
    if (!wf) throw new WorkflowNotFoundError(workflowName)

    const parsedInput = wf.parseInput(input)
    const scheduleId = randomUUID()
    const interval = setInterval(() => {
      void enqueue(workflowName, parsedInput).catch(reportError)
    }, intervalMs)

    schedules.set(scheduleId, interval)
    return scheduleId
  }

  function unschedule(scheduleId: string): boolean {
    const interval = schedules.get(scheduleId)
    if (!interval) return false

    clearInterval(interval)
    schedules.delete(scheduleId)
    return true
  }

  async function tick(): Promise<void> {
    if (registry.size === 0 || tickInFlight) {
      return
    }

    const generation = stopGeneration
    tickInFlight = true
    const promise = (async () => {
      try {
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

    for (const [scheduleId, interval] of schedules) {
      clearInterval(interval)
      schedules.delete(scheduleId)
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
    cancel,
    sendEvent,
    schedule,
    unschedule,
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
