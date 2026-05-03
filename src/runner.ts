import { _createEngine } from './core/engine'
import type { EngineHooks, EnqueueOptions } from './core/engine'
import type { PersistedValue, StorageAdapter } from './core/types'
import { SQLiteStorage } from './storage/sqlite-node'
import type { AnyWorkflow, WorkflowInputMap } from './core/workflow'

export interface EngineRunnerOptions {
  /** Storage backend for persisting runs and step results. Defaults to an in-memory SQLite instance. */
  storage?: StorageAdapter
  /** Optional lifecycle hooks. */
  hooks?: EngineHooks
  /** Maximum runs to process in parallel per tick (default: 1). */
  concurrency?: number
  /** How long a run's lease is valid before another engine can reclaim it in ms (default: 30000). */
  runLeaseDurationMs?: number
  /** How often to renew the lease while a run is executing in ms (default: leaseDuration / 3). */
  heartbeatIntervalMs?: number
  /** How often the engine polls for new runs in ms (default: 1000). */
  pollIntervalMs?: number
}

export interface EngineRunner<T> {
  enqueue(input: PersistedValue): Promise<void>
  [Symbol.asyncIterator](): AsyncIterator<T>
  finish(): Promise<void>
  dispose(): void
  [Symbol.asyncDispose](): Promise<void>
}

export interface MultiWorkflowEngineRunner<TWorkflowMap extends Record<string, PersistedValue> = Record<string, PersistedValue>> {
  enqueue<TName extends string & keyof TWorkflowMap>(
    workflowName: TName,
    input: TWorkflowMap[TName],
    options?: EnqueueOptions,
  ): Promise<void>
  [Symbol.asyncIterator](): AsyncIterator<unknown>
  finish(): Promise<void>
  dispose(): void
  [Symbol.asyncDispose](): Promise<void>
}

export interface MultiWorkflowEngineRunnerOptions extends EngineRunnerOptions {
  workflows: AnyWorkflow[]
}

/**
 * Wraps one or more workflows in a producer-consumer queue.
 *
 * Single-workflow form (workflow name inferred):
 *   const engine = createEngine(workflow, { storage })
 *   await engine.enqueue(input)
 *
 * Multi-workflow form:
 *   const engine = createEngine({ workflows: [wf1, wf2], storage })
 *   await engine.enqueue('wf1', input)
 *
 * - `enqueue(...)` submits a run and blocks until the previous run completes (backpressure).
 * - `for await (const item of engine)` yields the final output of each completed run.
 * - `finish()` waits for the in-flight run then stops.
 * - `dispose()` stops immediately without waiting.
 * - `[Symbol.asyncDispose]()` waits for the in-flight run then stops (same as `finish()`).
 */
export function createEngine<T>(workflow: AnyWorkflow, options?: EngineRunnerOptions): EngineRunner<T>
export function createEngine<const TWorkflows extends readonly AnyWorkflow[]>(
  options: { workflows: TWorkflows; storage?: StorageAdapter; hooks?: EngineHooks },
): MultiWorkflowEngineRunner<WorkflowInputMap<TWorkflows>>
export function createEngine<T>(
  workflowOrOptions: AnyWorkflow | (MultiWorkflowEngineRunnerOptions & { workflows: readonly AnyWorkflow[] }),
  runnerOptions: EngineRunnerOptions = {},
): EngineRunner<T> | MultiWorkflowEngineRunner {
  const isSingleWorkflow = 'name' in workflowOrOptions && 'steps' in workflowOrOptions

  const singleWorkflow = isSingleWorkflow ? (workflowOrOptions as AnyWorkflow) : undefined
  const workflows = isSingleWorkflow
    ? [workflowOrOptions as AnyWorkflow]
    : (workflowOrOptions as MultiWorkflowEngineRunnerOptions).workflows
  const options = isSingleWorkflow ? runnerOptions : (workflowOrOptions as MultiWorkflowEngineRunnerOptions)
  const userHooks = options.hooks

  const queue: T[] = []
  let done = false
  let waiter: (() => void) | null = null
  let permit: (() => void) | null = null

  const engine = _createEngine({
    storage: options.storage ?? new SQLiteStorage(':memory:'),
    workflows,
    concurrency: options.concurrency,
    runLeaseDurationMs: options.runLeaseDurationMs,
    heartbeatIntervalMs: options.heartbeatIntervalMs,
    hooks: {
      onRunStart: userHooks?.onRunStart,
      onStepStart: userHooks?.onStepStart,
      onStepComplete: userHooks?.onStepComplete,
      onError: userHooks?.onError,
      onRunComplete: async (event) => {
        queue.push(event.output as T)
        waiter?.(); waiter = null
        permit?.(); permit = null
        await userHooks?.onRunComplete?.(event)
      },
      onRunFailed: async (event) => {
        console.error(`\n[${event.stepName}] failed: ${event.error.message}`)
        permit?.(); permit = null
        await userHooks?.onRunFailed?.(event)
      },
    },
  })

  let permitPromise: Promise<void> = Promise.resolve()
  let started = false

  async function ensureStarted(): Promise<void> {
    if (!started) {
      started = true
      await engine.start(options.pollIntervalMs)
    }
  }

  async function enqueueSingle(input: PersistedValue): Promise<void> {
    await ensureStarted()
    await permitPromise
    permitPromise = new Promise<void>((resolve) => { permit = resolve })
    await engine.enqueue(singleWorkflow!.name, input)
  }

  async function enqueueMulti(workflowName: string, input: PersistedValue, enqueueOptions?: EnqueueOptions): Promise<void> {
    await ensureStarted()
    await permitPromise
    permitPromise = new Promise<void>((resolve) => { permit = resolve })
    await engine.enqueue(workflowName, input, enqueueOptions)
  }

  async function* generate(): AsyncGenerator<T> {
    while (!done || queue.length > 0) {
      if (queue.length > 0) {
        yield queue.shift()!
      } else {
        await new Promise<void>((resolve) => { waiter = resolve })
      }
    }
  }

  async function finish(): Promise<void> {
    await permitPromise
    done = true
    waiter?.(); waiter = null
    await engine.stop()
  }

  function dispose(): void {
    done = true
    waiter?.(); waiter = null
    permit?.(); permit = null
    void engine.stop()
  }

  if (isSingleWorkflow) {
    return {
      enqueue: enqueueSingle,
      [Symbol.asyncIterator]: generate,
      finish,
      dispose,
      [Symbol.asyncDispose]: finish,
    } as EngineRunner<T>
  }

  return {
    enqueue: enqueueMulti,
    [Symbol.asyncIterator]: generate,
    finish,
    dispose,
    [Symbol.asyncDispose]: finish,
  } as MultiWorkflowEngineRunner
}
