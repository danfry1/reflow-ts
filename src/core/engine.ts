import { randomUUID } from 'node:crypto'
import { clonePersistedValue, persistedValuesEqual } from '../storage/codec'
import {
  createBoundedAsyncIterator,
  type AbortableSubscriber,
} from './async-iterator'
import {
  ConfigError,
  DuplicateWorkflowError,
  EarlyCompleteError,
  IdempotencyConflictError,
  LeaseExpiredError,
  ParallelCompleteError,
  RunCancelledError,
  RunControlError,
  StepTimeoutError,
  WorkflowNotFoundError,
} from './errors'
import type {
  ClaimedRun,
  PersistedValue,
  RunInfo,
  StorageAdapter,
  WorkflowRun,
} from './types'
import type { AnyWorkflow, StepDefinition, WorkflowInputMap } from './workflow'

/**
 * A lifecycle event emitted during workflow execution.
 *
 * Consumed both by the {@link EngineHooks} callbacks and by {@link Engine.stream}.
 * Every event carries the owning `workflow` name so a single stream can fan out
 * across multiple workflows.
 */
export type EngineEvent =
  | { readonly type: 'runStart'; readonly runId: string; readonly workflow: string }
  | { readonly type: 'stepStart'; readonly runId: string; readonly workflow: string; readonly stepName: string }
  | { readonly type: 'stepSkipped'; readonly runId: string; readonly workflow: string; readonly stepName: string }
  | {
      readonly type: 'stepComplete'
      readonly runId: string
      readonly workflow: string
      readonly stepName: string
      readonly output: PersistedValue
      readonly attempts: number
    }
  | { readonly type: 'runComplete'; readonly runId: string; readonly workflow: string; readonly output: PersistedValue }
  | { readonly type: 'runFailed'; readonly runId: string; readonly workflow: string; readonly stepName: string; readonly error: Error }

/** Narrow {@link EngineEvent} to a single `type` (or union of types). */
export type EngineEventOf<T extends EngineEvent['type']> = Extract<EngineEvent, { type: T }>

/**
 * Lifecycle hooks fired during workflow execution.
 *
 * Hooks may be synchronous or `async` — an async hook is awaited before the
 * engine proceeds, so it can apply backpressure or guarantee ordering. A hook
 * that throws (or rejects) never affects engine state; the error is swallowed.
 */
export interface EngineHooks {
  onRunStart?: (event: EngineEventOf<'runStart'>) => void
  onStepStart?: (event: EngineEventOf<'stepStart'>) => void
  /** Called when a step's `when` predicate returns false and the step is skipped. */
  onStepSkipped?: (event: EngineEventOf<'stepSkipped'>) => void
  onStepComplete?: (event: EngineEventOf<'stepComplete'>) => void
  onRunComplete?: (event: EngineEventOf<'runComplete'>) => void
  onRunFailed?: (event: EngineEventOf<'runFailed'>) => void
  /** Called when a background operation fails (scheduled enqueue, poll cycle). Without this hook, these errors are silently swallowed. */
  onError?: (error: Error) => void
}

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
  runAbortController: AbortController
  observerAbortController: AbortController
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
   * may affect engine state, so all errors are contained.
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
      (subscriber) => subscriber.push(cloneEngineEvent(event), signal),
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
    switch (event.type) {
      case 'runStart':
        return hooks?.onRunStart?.({ ...event })
      case 'stepStart':
        return hooks?.onStepStart?.({ ...event })
      case 'stepSkipped':
        return hooks?.onStepSkipped?.({ ...event })
      case 'stepComplete':
        return hooks?.onStepComplete?.({
          ...event,
          output: clonePersistedValue(event.output, 'Step hook output'),
        })
      case 'runComplete':
        return hooks?.onRunComplete?.({
          ...event,
          output: clonePersistedValue(event.output, 'Run hook output'),
        })
      case 'runFailed':
        return hooks?.onRunFailed?.({
          ...event,
          error: cloneError(event.error),
        })
    }
  }

  /** Report a background error to `onError`, swallowing any error it raises. */
  function reportError(error: unknown): void {
    const err = error instanceof Error ? error : new Error(String(error))
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

  async function executeRun(run: ClaimedRun): Promise<void> {
    const wf = registry.get(run.workflow)
    if (!wf) return

    const activeRun = registerActiveRun(run)
    const observerSignal = activeRun.observerAbortController.signal

    try {
      const currentRun = await storage.getRun(run.id)
      if (!currentRun || currentRun.status !== 'running') {
        return
      }

      await emit({ type: 'runStart', runId: run.id, workflow: run.workflow }, observerSignal)

      const existingSteps = await storage.getStepResults(run.id)
      const completedMap = new Map(existingSteps.map((step) => [step.name, step]))
      let prev: PersistedValue = undefined
      const stepsAccumulator: Record<string, PersistedValue> = {}

      for (const unit of wf.executionUnits) {
        if (unit.kind === 'step') {
          const stepDef = unit.definition
          if (activeRun.runAbortController.signal.aborted) {
            const latestRun = await storage.getRun(run.id)
            if (!latestRun || latestRun.status === 'cancelled') {
              return
            }
          }

          const existing = completedMap.get(stepDef.name)
          if (existing?.status === 'completed-early') {
            // This step called complete() in a previous execution — finish the run
            await emit({
              type: 'stepComplete',
              runId: run.id,
              workflow: run.workflow,
              stepName: stepDef.name,
              output: existing.output,
              attempts: existing.attempts,
            }, observerSignal)

            const completed = await storage.updateClaimedRunStatus(run.id, run.leaseId, 'completed')
            if (!completed) {
              throw new LeaseExpiredError(run.id)
            }

            await emit(
              { type: 'runComplete', runId: run.id, workflow: run.workflow, output: existing.output },
              observerSignal,
            )

            return
          }
          if (existing?.status === 'completed') {
            prev = existing.output
            stepsAccumulator[stepDef.name] = structuredClone(existing.output)
            continue
          }
          if (existing?.status === 'skipped') {
            // Skipped on a previous execution — leave `prev` untouched (the
            // last real output passes through) and move to the next unit.
            continue
          }

          const frozenSteps = snapshotSteps(stepsAccumulator)

          try {
            if (
              stepDef.when &&
              !(await stepDef.when({ input: run.input, prev, steps: frozenSteps }))
            ) {
              // Condition is false — persist a skip so it is not re-evaluated on
              // replay, leave `prev` unchanged, and continue to the next unit.
              const now = Date.now()
              const saved = await storage.saveStepResult({
                id: randomUUID(),
                runId: run.id,
                name: stepDef.name,
                status: 'skipped',
                output: null,
                error: null,
                attempts: 0,
                createdAt: now,
                updatedAt: now,
              }, run.leaseId)

              if (!saved) {
                throw new LeaseExpiredError(run.id)
              }

              await emit(
                { type: 'stepSkipped', runId: run.id, workflow: run.workflow, stepName: stepDef.name },
                observerSignal,
              )

              continue
            }

            await emit(
              { type: 'stepStart', runId: run.id, workflow: run.workflow, stepName: stepDef.name },
              observerSignal,
            )

            const outcome = await executeStep(run, activeRun, stepDef, prev, frozenSteps)

            if (outcome.kind === 'failed') {
              // A control-flow abort (cancel / engine stop / lease loss) can land
              // on an await point between steps — including a backpressured stream
              // emit. When it does, executeStep reports a synthetic failure for a
              // step that never really ran. That is a side effect of the abort, not
              // a real failure: leave the run untouched (status stays as-is, so a
              // stopped or lease-lost run remains reclaimable) instead of marking
              // it failed. A non-control abort reason (e.g. a heartbeat storage
              // error) is a genuine failure and still falls through below.
              if (
                activeRun.runAbortController.signal.aborted &&
                activeRun.runAbortController.signal.reason instanceof RunControlError
              ) {
                return
              }

              // Persist the failed step result
              const now = Date.now()
              const saved = await storage.saveStepResult({
                id: randomUUID(),
                runId: run.id,
                name: stepDef.name,
                status: 'failed',
                output: null,
                error: outcome.error.message,
                attempts: outcome.attempts,
                createdAt: now,
                updatedAt: now,
              }, run.leaseId)

              if (!saved) {
                throw new LeaseExpiredError(run.id)
              }

              throw outcome.error
            }

            // Unified success path for both normal and early completion
            const now = Date.now()
            const saved = await storage.saveStepResult({
              id: randomUUID(),
              runId: run.id,
              name: stepDef.name,
              status: outcome.kind === 'early-complete' ? 'completed-early' : 'completed',
              output: outcome.output,
              error: null,
              attempts: outcome.attempts,
              createdAt: now,
              updatedAt: now,
            }, run.leaseId)

            if (!saved) {
              throw new LeaseExpiredError(run.id)
            }

            await emit({
              type: 'stepComplete',
              runId: run.id,
              workflow: run.workflow,
              stepName: stepDef.name,
              output: outcome.output,
              attempts: outcome.attempts,
            }, observerSignal)

            if (outcome.kind === 'early-complete') {
              const completed = await storage.updateClaimedRunStatus(run.id, run.leaseId, 'completed')
              if (!completed) {
                throw new LeaseExpiredError(run.id)
              }

              await emit(
                { type: 'runComplete', runId: run.id, workflow: run.workflow, output: outcome.output },
                observerSignal,
              )

              return
            }

            prev = outcome.output
            stepsAccumulator[stepDef.name] = structuredClone(prev)
          } catch (error) {
            const err = error instanceof Error ? error : new Error(String(error))

            if (err instanceof EarlyCompleteError) {
              throw new Error(`EarlyCompleteError escaped executeStep for step "${stepDef.name}"`)
            }

            if (err instanceof RunControlError) {
              return
            }

            if (activeRun.runAbortController.signal.aborted) {
              const currentRun = await storage.getRun(run.id)
              if (!currentRun || currentRun.status === 'cancelled') {
                return
              }
            }

            const failed = await storage.updateClaimedRunStatus(run.id, run.leaseId, 'failed')
            if (!failed) {
              return
            }

            await emit(
              { type: 'runFailed', runId: run.id, workflow: run.workflow, stepName: stepDef.name, error: err },
              observerSignal,
            )

            if (wf.failureHandler) {
              try {
                await wf.failureHandler({
                  error: err,
                  stepName: stepDef.name,
                  input: run.input,
                })
              } catch { /* onFailure must not affect engine state */ }
            }

            return
          }
        } else {
          const result = await executeParallelGroup(run, activeRun, unit.branches, prev, stepsAccumulator, completedMap)
          if (result.kind === 'skipped-cancelled') {
            return
          }
          if (result.kind === 'failed') {
            const failed = await storage.updateClaimedRunStatus(run.id, run.leaseId, 'failed')
            if (!failed) {
              return
            }

            await emit(
              { type: 'runFailed', runId: run.id, workflow: run.workflow, stepName: result.branchName, error: result.error },
              observerSignal,
            )

            if (wf.failureHandler) {
              try {
                await wf.failureHandler({
                  error: result.error,
                  stepName: result.branchName,
                  input: run.input,
                })
              } catch { /* onFailure must not affect engine state */ }
            }

            return
          }
          prev = result.merged
        }
      }

      const latestRun = await storage.getRun(run.id)
      if (!latestRun || latestRun.status === 'cancelled') {
        return
      }

      const completed = await storage.updateClaimedRunStatus(run.id, run.leaseId, 'completed')
      if (!completed) {
        return
      }

      await emit(
        { type: 'runComplete', runId: run.id, workflow: run.workflow, output: prev },
        observerSignal,
      )
    } catch (error) {
      if (!(error instanceof RunControlError)) {
        throw error
      }
    } finally {
      cleanupActiveRun(run.id)
    }
  }

  type StepOutcome =
    | { kind: 'completed'; output: PersistedValue; attempts: number }
    | { kind: 'early-complete'; output: PersistedValue; attempts: number }
    | { kind: 'failed'; error: Error; attempts: number }

  async function executeStep(
    run: ClaimedRun,
    activeRun: ActiveRunState,
    stepDef: StepDefinition,
    prev: PersistedValue,
    steps: Record<string, PersistedValue>,
  ): Promise<StepOutcome> {
    const maxAttempts = stepDef.retry?.maxAttempts ?? 1
    if (maxAttempts < 1) {
      throw new ConfigError(`Step "${stepDef.name}" retry maxAttempts must be at least 1`)
    }
    const backoff = stepDef.retry?.backoff ?? 'linear'
    const initialDelay = stepDef.retry?.initialDelayMs ?? 1000
    const timeoutMs = stepDef.timeoutMs ?? stepDef.retry?.timeoutMs

    let lastError: Error | null = null

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      // Bail out of further retry attempts once the run/group signal is aborted.
      // A handler is not re-entered (runWithSignal short-circuits), but iterating
      // the loop just to reject again wastes work and confuses metrics.
      if (activeRun.runAbortController.signal.aborted) {
        break
      }

      const attemptSignal = createAttemptSignal(activeRun.runAbortController.signal, timeoutMs)

      try {
        const complete = (value?: PersistedValue): never => {
          throw new EarlyCompleteError(value)
        }
        const rawOutput = await runWithSignal(
          () => stepDef.handler({ input: run.input, prev, signal: attemptSignal.signal, complete, steps }),
          attemptSignal.signal,
        )
        const output: PersistedValue = rawOutput === undefined ? undefined : rawOutput

        return { kind: 'completed', output, attempts: attempt }
      } catch (error) {
        if (error instanceof EarlyCompleteError) {
          const earlyOutput: PersistedValue = error.value === undefined ? undefined : error.value
          return { kind: 'early-complete', output: earlyOutput, attempts: attempt }
        }

        const err = error instanceof Error ? error : new Error(String(error))

        if (err instanceof RunControlError) {
          throw err
        }

        lastError = err

        if (attempt < maxAttempts) {
          const delay = backoff === 'exponential'
            ? initialDelay * Math.pow(2, attempt - 1)
            : initialDelay * attempt

          if (delay > 0) {
            await delayWithSignal(delay, activeRun.runAbortController.signal)
          }
        }
      } finally {
        attemptSignal.cleanup()
      }
    }

    // RunControlError is rethrown inside the loop; if we reach here, lastError is a
    // regular failure (or null when the signal was aborted before any attempt ran).
    return { kind: 'failed', error: lastError ?? new Error('Unknown error'), attempts: maxAttempts }
  }

  type ParallelGroupResult =
    | { kind: 'completed'; merged: Record<string, PersistedValue> }
    | { kind: 'skipped-cancelled' }
    | { kind: 'failed'; branchName: string; error: Error }

  async function executeParallelGroup(
    run: ClaimedRun,
    activeRun: ActiveRunState,
    branches: readonly StepDefinition[],
    prev: PersistedValue,
    stepsAccumulator: Record<string, PersistedValue>,
    completedMap: Map<string, { id: string; status: string; output: PersistedValue; attempts: number }>,
  ): Promise<ParallelGroupResult> {
    if (activeRun.runAbortController.signal.aborted) {
      const latestRun = await storage.getRun(run.id)
      if (!latestRun || latestRun.status === 'cancelled') {
        return { kind: 'skipped-cancelled' }
      }
    }

    // Crash-recovery: any branch already persisted as 'completed' is reused.
    // Failed records do not count — they get retried fresh, matching sequential semantics.
    const merged: Record<string, PersistedValue> = {}
    const pendingBranches: StepDefinition[] = []
    for (const branchDef of branches) {
      const existing = completedMap.get(branchDef.name)
      if (existing?.status === 'completed') {
        merged[branchDef.name] = existing.output
        stepsAccumulator[branchDef.name] = structuredClone(existing.output)
      } else {
        pendingBranches.push(branchDef)
      }
    }

    if (pendingBranches.length === 0) {
      return { kind: 'completed', merged }
    }

    const frozenSteps = snapshotSteps(stepsAccumulator)

    const groupAbort = new AbortController()
    // Track which branch was the *original* failure that caused the group to abort.
    // Siblings that fail because they observed the abort are downstream effects, not
    // the underlying cause — distinguishing this keeps onRunFailed accurate.
    let causeBranch: BranchFailedError | null = null
    const onRunAbort = () => {
      if (!groupAbort.signal.aborted) {
        groupAbort.abort(activeRun.runAbortController.signal.reason)
      }
    }
    if (activeRun.runAbortController.signal.aborted) {
      groupAbort.abort(activeRun.runAbortController.signal.reason)
    } else {
      activeRun.runAbortController.signal.addEventListener('abort', onRunAbort, { once: true })
    }

    try {
      for (const branchDef of pendingBranches) {
        await emit(
          { type: 'stepStart', runId: run.id, workflow: run.workflow, stepName: branchDef.name },
          activeRun.observerAbortController.signal,
        )
      }

      const groupActiveRun: ActiveRunState = {
        ...activeRun,
        runAbortController: groupAbort,
      }

      // Run all pending branches; collect both successes and failures so siblings
      // get a chance to settle (allSettled, not all) before we tear down.
      const settled = await Promise.allSettled(
        pendingBranches.map(async (branchDef) => {
          const guardedHandler: StepDefinition['handler'] = (ctx) => {
            const guardedComplete = (): never => {
              throw new ParallelCompleteError(branchDef.name)
            }
            return branchDef.handler({ ...ctx, complete: guardedComplete })
          }

          const guardedDef: StepDefinition = { ...branchDef, handler: guardedHandler }
          const outcome = await executeStep(run, groupActiveRun, guardedDef, prev, frozenSteps)

          // `outcome.kind === 'early-complete'` is unreachable: guardedComplete throws
          // ParallelCompleteError (a regular error) before EarlyCompleteError can be
          // raised, so executeStep returns 'completed' or 'failed' for parallel branches.
          if (outcome.kind === 'failed') {
            const failure = new BranchFailedError(branchDef.name, outcome.error, outcome.attempts)
            if (!groupAbort.signal.aborted) {
              causeBranch = failure
              groupAbort.abort(outcome.error)
            }
            throw failure
          }

          return { name: branchDef.name, output: outcome.output, attempts: outcome.attempts }
        }),
      )

      // If the run itself was aborted by a control-flow signal while branches were
      // running (cancel, engine stop, or lease loss), surface that before reporting
      // branch failures. A branch that failed because it observed the run-level
      // abort is a downstream effect, not the cause — reporting it would mark a
      // stopped/reclaimable run as failed. A non-control abort (e.g. a heartbeat
      // storage error) is a genuine failure and still falls through below.
      if (
        activeRun.runAbortController.signal.aborted &&
        activeRun.runAbortController.signal.reason instanceof RunControlError
      ) {
        return { kind: 'skipped-cancelled' }
      }

      // Prefer the branch that actually caused the abort; only fall back to scanning
      // settled when no cause was recorded (e.g. group aborted from outside the loop).
      let firstFailure: BranchFailedError | null = causeBranch
      if (!firstFailure) {
        for (const result of settled) {
          if (result.status === 'rejected') {
            const err = result.reason instanceof Error ? result.reason : new Error(String(result.reason))
            if (err instanceof RunControlError) {
              return { kind: 'skipped-cancelled' }
            }
            if (err instanceof BranchFailedError) {
              firstFailure = err
              break
            }
            // Defensive: wrap unknown error as branch failure on first pending branch.
            firstFailure = new BranchFailedError(pendingBranches[0].name, err, 1)
            break
          }
        }
      }

      if (firstFailure) {
        // Persist the failed step result. Reuse the existing row id so a retry upserts
        // in place rather than appending a duplicate. If the lease is lost mid-write,
        // we still report the failure — the outer code will no-op the run-status
        // update because the same lease check will fail there.
        const failedBranch = firstFailure.branchName
        const existingRow = completedMap.get(failedBranch)
        const now = Date.now()
        await storage.saveStepResult({
          id: existingRow?.id ?? randomUUID(),
          runId: run.id,
          name: failedBranch,
          status: 'failed',
          output: null,
          error: firstFailure.branchError.message,
          attempts: firstFailure.attempts,
          createdAt: now,
          updatedAt: now,
        }, run.leaseId)

        return { kind: 'failed', branchName: failedBranch, error: firstFailure.branchError }
      }

      // All pending branches succeeded — persist their results and fire complete hooks.
      for (const result of settled) {
        if (result.status !== 'fulfilled') continue
        const branchResult = result.value
        const existingRow = completedMap.get(branchResult.name)
        const now = Date.now()
        const saved = await storage.saveStepResult({
          id: existingRow?.id ?? randomUUID(),
          runId: run.id,
          name: branchResult.name,
          status: 'completed',
          output: branchResult.output,
          error: null,
          attempts: branchResult.attempts,
          createdAt: now,
          updatedAt: now,
        }, run.leaseId)

        if (!saved) {
          throw new LeaseExpiredError(run.id)
        }

        await emit({
          type: 'stepComplete',
          runId: run.id,
          workflow: run.workflow,
          stepName: branchResult.name,
          output: branchResult.output,
          attempts: branchResult.attempts,
        }, activeRun.observerAbortController.signal)

        stepsAccumulator[branchResult.name] = structuredClone(branchResult.output)
        merged[branchResult.name] = branchResult.output
      }

      return { kind: 'completed', merged }
    } catch (error) {
      // Promise.allSettled above swallows per-branch failures, so this catch only
      // fires for LeaseExpiredError thrown during success-path persistence.
      const err = error instanceof Error ? error : new Error(String(error))

      if (err instanceof RunControlError) {
        return { kind: 'skipped-cancelled' }
      }

      return { kind: 'failed', branchName: pendingBranches[0].name, error: err }
    } finally {
      activeRun.runAbortController.signal.removeEventListener('abort', onRunAbort)
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
      runAbortController: new AbortController(),
      observerAbortController: new AbortController(),
      heartbeatTimer: null,
      heartbeatInFlight: false,
    }

    activeRuns.set(run.id, activeRun)
    startHeartbeat(run, activeRun)
    return activeRun
  }

  function startHeartbeat(run: ClaimedRun, activeRun: ActiveRunState): void {
    const sendHeartbeat = async () => {
      if (activeRun.heartbeatInFlight || activeRun.runAbortController.signal.aborted) {
        return
      }

      activeRun.heartbeatInFlight = true

      try {
        const ok = await storage.heartbeatRun(run.id, activeRun.leaseId)
        if (!ok) {
          abortActiveRun(run.id, new LeaseExpiredError(run.id))
        }
      } catch (error) {
        abortActiveRun(run.id, error instanceof Error ? error : new Error(String(error)))
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

    if (!activeRun.runAbortController.signal.aborted) {
      activeRun.runAbortController.abort(reason)
    }

    if (
      reason instanceof RunControlError &&
      !activeRun.observerAbortController.signal.aborted
    ) {
      activeRun.observerAbortController.abort(reason)
    }
  }

  return {
    enqueue,
    getRunStatus,
    cancel,
    schedule,
    unschedule,
    stream: createStream,
    tick,
    start,
    stop,
  } as Engine<WorkflowInputMap<TWorkflows>>
}

function cloneEngineEvent(event: EngineEvent): EngineEvent {
  switch (event.type) {
    case 'stepComplete':
      return {
        ...event,
        output: clonePersistedValue(event.output, 'Step event output'),
      }
    case 'runComplete':
      return {
        ...event,
        output: clonePersistedValue(event.output, 'Run event output'),
      }
    case 'runFailed':
      return {
        ...event,
        error: cloneError(event.error),
      }
    default:
      return { ...event }
  }
}

function cloneError(error: Error): Error {
  return Object.create(
    Object.getPrototypeOf(error),
    Object.getOwnPropertyDescriptors(error),
  ) as Error
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

function createAttemptSignal(
  runSignal: AbortSignal,
  timeoutMs?: number,
): { signal: AbortSignal; cleanup: () => void } {
  const controller = new AbortController()
  const cleanups: Array<() => void> = []

  const forwardAbort = (reason: unknown) => {
    if (!controller.signal.aborted) {
      controller.abort(toError(reason))
    }
  }

  if (runSignal.aborted) {
    forwardAbort(runSignal.reason)
  } else {
    const onRunAbort = () => forwardAbort(runSignal.reason)
    runSignal.addEventListener('abort', onRunAbort, { once: true })
    cleanups.push(() => runSignal.removeEventListener('abort', onRunAbort))
  }

  if (timeoutMs) {
    const timer = setTimeout(() => {
      forwardAbort(new StepTimeoutError(timeoutMs))
    }, timeoutMs)
    cleanups.push(() => clearTimeout(timer))
  }

  return {
    signal: controller.signal,
    cleanup: () => {
      for (const cleanup of cleanups) {
        cleanup()
      }
    },
  }
}

function delayWithSignal(ms: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) {
    return Promise.reject(toError(signal.reason))
  }

  return new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup()
      resolve()
    }, ms)

    const onAbort = () => {
      cleanup()
      reject(toError(signal.reason))
    }

    const cleanup = () => {
      clearTimeout(timer)
      signal.removeEventListener('abort', onAbort)
    }

    signal.addEventListener('abort', onAbort, { once: true })
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

function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error))
}

function noop() {}

function snapshotSteps(accumulator: Record<string, PersistedValue>): Readonly<Record<string, PersistedValue>> {
  return deepFreeze(structuredClone(accumulator))
}

function deepFreeze<T extends Record<string, unknown>>(obj: T): T {
  Object.freeze(obj)
  for (const value of Object.values(obj)) {
    if (value !== null && typeof value === 'object' && !Object.isFrozen(value)) {
      deepFreeze(value as Record<string, unknown>)
    }
  }
  return obj
}

/**
 * Carries branch metadata (name, original error, attempts) through Promise.all rejections.
 * @internal Not exported from the public API.
 */
class BranchFailedError extends Error {
  constructor(
    public readonly branchName: string,
    public readonly branchError: Error,
    public readonly attempts: number,
  ) {
    super(branchError.message)
    this.name = 'BranchFailedError'
  }
}
