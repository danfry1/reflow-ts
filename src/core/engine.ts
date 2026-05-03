import { randomUUID } from 'node:crypto'
import { persistedValuesEqual } from '../storage/codec'
import {
  ConfigError,
  DuplicateWorkflowError,
  EarlyCompleteError,
  IdempotencyConflictError,
  LeaseExpiredError,
  RunCancelledError,
  RunControlError,
  StepTimeoutError,
  WorkflowNotFoundError,
} from './errors'
import type {
  ClaimedRun,
  PersistedValue,
  RunInfo,
  StepResult,
  StorageAdapter,
  WorkflowRun,
} from './types'
import type { AnyWorkflow, StepDefinition, WorkflowInputMap } from './workflow'

/** Lifecycle hooks fired during workflow execution. */
export interface EngineHooks {
  onRunStart?: (event: { runId: string; workflow: string }) => Promise<void> | void
  onStepStart?: (event: { runId: string; stepName: string }) => Promise<void> | void
  onStepComplete?: (event: { runId: string; stepName: string; output: PersistedValue; attempts: number; cacheHit: boolean }) => Promise<void> | void
  onRunComplete?: (event: { runId: string; workflow: string; output: PersistedValue }) => Promise<void> | void
  onRunFailed?: (event: { runId: string; workflow: string; stepName: string; error: Error }) => Promise<void> | void
  /** Called when a background operation fails (scheduled enqueue, poll cycle). Without this hook, these errors are silently swallowed. */
  onError?: (error: Error) => Promise<void> | void
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
  heartbeatTimer: ReturnType<typeof setInterval> | null
  heartbeatInFlight: boolean
}

function parseTtl(cache: true | `${number}h` | `${number}d` | undefined): number | undefined {
  if (!cache || cache === true) return undefined
  const match = /^(\d+)([hd])$/.exec(cache)
  if (!match) return undefined
  const n = parseInt(match[1], 10)
  return match[2] === 'h' ? n * 3_600_000 : n * 86_400_000
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
  let running = false
  let timer: ReturnType<typeof setInterval> | null = null
  let tickInFlight = false
  let tickPromise: Promise<void> | null = null

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

  async function executeRun(run: ClaimedRun): Promise<void> {
    const wf = registry.get(run.workflow)
    if (!wf) return

    const activeRun = registerActiveRun(run)

    try {
      try {
        await hooks?.onRunStart?.({ runId: run.id, workflow: run.workflow })
      } catch (e) {
        console.error('[reflow-ts] onRunStart hook threw:', e)
      }

      const existingSteps = await storage.getStepResults(run.id)
      const completedMap = new Map(existingSteps.map((step) => [step.name, step]))
      let prev: PersistedValue = undefined
      const stepsAccumulator: Record<string, PersistedValue> = {}

      for (const stepDef of wf.steps) {
        if (activeRun.runAbortController.signal.aborted) {
          const latestRun = await storage.getRun(run.id)
          if (!latestRun || latestRun.status === 'cancelled') {
            return
          }
        }

        const existing = completedMap.get(stepDef.name)
        if (existing?.status === 'completed-early') {
          // This step called complete() in a previous execution — finish the run
          try {
            await hooks?.onStepComplete?.({
              runId: run.id,
              stepName: stepDef.name,
              output: existing.output,
              attempts: existing.attempts,
              cacheHit: false,
            })
          } catch (e) {
            console.error('[reflow-ts] onStepComplete hook threw:', e)
          }

          const completed = await storage.updateClaimedRunStatus(run.id, run.leaseId, 'completed')
          if (!completed) {
            throw new LeaseExpiredError(run.id)
          }

          try {
            await hooks?.onRunComplete?.({ runId: run.id, workflow: run.workflow, output: existing.output })
          } catch (e) {
            console.error('[reflow-ts] onRunComplete hook threw:', e)
          }

          return
        }
        if (existing?.status === 'completed') {
          prev = existing.output
          stepsAccumulator[stepDef.name] = structuredClone(existing.output)
          continue
        }

        // Cross-run cache lookup — only runs when the step has not already completed in this run
        if (stepDef.cache && stepDef.cacheKey) {
          const key = stepDef.cacheKey(run.input)
          const ttlMs = parseTtl(stepDef.cache)
          const cached = await storage.getCachedStepResult(stepDef.name, key, ttlMs)
          if (cached) {
            const now = Date.now()
            // Sentinel row (attempts: 0) records the cache hit in this run for within-run replay.
            // cacheKey is intentionally omitted — sentinel rows must not enter the cache index.
            const saved = await storage.saveStepResult(
              {
                id: randomUUID(),
                runId: run.id,
                name: stepDef.name,
                status: 'completed',
                output: cached.output,
                error: null,
                attempts: 0,
                createdAt: now,
                updatedAt: now,
              },
              run.leaseId,
            )
            if (!saved) throw new LeaseExpiredError(run.id)
            try {
              await hooks?.onStepComplete?.({
                runId: run.id,
                stepName: stepDef.name,
                output: cached.output,
                attempts: 0,
                cacheHit: true,
              })
            } catch (e) {
              console.error('[reflow-ts] onStepComplete hook threw:', e)
            }
            prev = cached.output
            stepsAccumulator[stepDef.name] = structuredClone(cached.output)
            continue
          }
        }

        const frozenSteps = snapshotSteps(stepsAccumulator)

        try {
          try {
            await hooks?.onStepStart?.({ runId: run.id, stepName: stepDef.name })
          } catch (e) {
            console.error('[reflow-ts] onStepStart hook threw:', e)
          }

          const outcome = await executeStep(run, activeRun, stepDef, prev, frozenSteps)

          if (outcome.kind === 'failed') {
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
          const cacheKeyForStep = (stepDef.cache && stepDef.cacheKey)
            ? stepDef.cacheKey(run.input)
            : undefined

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
          }, run.leaseId, cacheKeyForStep)

          if (!saved) {
            throw new LeaseExpiredError(run.id)
          }

          try {
            await hooks?.onStepComplete?.({
              runId: run.id,
              stepName: stepDef.name,
              output: outcome.output,
              attempts: outcome.attempts,
              cacheHit: false,
            })
          } catch (e) {
            console.error('[reflow-ts] onStepComplete hook threw:', e)
          }

          if (outcome.kind === 'early-complete') {
            const completed = await storage.updateClaimedRunStatus(run.id, run.leaseId, 'completed')
            if (!completed) {
              throw new LeaseExpiredError(run.id)
            }

            try {
              await hooks?.onRunComplete?.({ runId: run.id, workflow: run.workflow, output: outcome.output })
            } catch (e) {
              console.error('[reflow-ts] onRunComplete hook threw:', e)
            }

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

          try {
            await hooks?.onRunFailed?.({ runId: run.id, workflow: run.workflow, stepName: stepDef.name, error: err })
          } catch (e) {
            console.error('[reflow-ts] onRunFailed hook threw:', e)
          }

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
      }

      const latestRun = await storage.getRun(run.id)
      if (!latestRun || latestRun.status === 'cancelled') {
        return
      }

      const completed = await storage.updateClaimedRunStatus(run.id, run.leaseId, 'completed')
      if (!completed) {
        return
      }

      try {
        await hooks?.onRunComplete?.({ runId: run.id, workflow: run.workflow, output: prev })
      } catch (e) {
        console.error('[reflow-ts] onRunComplete hook threw:', e)
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

    if (lastError instanceof RunControlError) {
      throw lastError
    }

    return { kind: 'failed', error: lastError ?? new Error('Unknown error'), attempts: maxAttempts }
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
      void enqueue(workflowName, parsedInput).catch((error) => {
        void Promise.resolve(hooks?.onError?.(error instanceof Error ? error : new Error(String(error)))).catch(noop)
      })
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
      void runPollCycle().catch((error) => {
        void Promise.resolve(hooks?.onError?.(error instanceof Error ? error : new Error(String(error)))).catch(noop)
      })
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
    if (!activeRun || activeRun.runAbortController.signal.aborted) {
      return
    }

    activeRun.runAbortController.abort(reason)
  }

  return {
    enqueue,
    getRunStatus,
    cancel,
    schedule,
    unschedule,
    tick,
    start,
    stop,
  } as Engine<WorkflowInputMap<TWorkflows>>
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
