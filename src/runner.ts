import { createEngine } from './core/engine'
import type { PersistedValue, StorageAdapter } from './core/types'
import { SQLiteStorage } from './storage/sqlite-node'
import type { AnyWorkflow } from './core/workflow'

export interface EngineRunnerOptions {
  storage?: StorageAdapter
}

export interface EngineRunner<T> {
  enqueue(input: PersistedValue): Promise<void>
  [Symbol.asyncIterator](): AsyncIterator<T>
  finish(): Promise<void>
  dispose(): void
  [Symbol.asyncDispose](): Promise<void>
}

/**
 * Wraps a single workflow in a producer-consumer queue.
 *
 * - `enqueue(input)` submits a run and blocks until the previous run completes (backpressure).
 * - `for await (const item of runner)` yields the final output of each completed run.
 * - `finish()` waits for the in-flight run then stops the engine.
 * - `dispose()` stops immediately without waiting.
 *
 * `onRunComplete` drives the queue push and permit release.
 * `onStepComplete` is the structured logging hook point (cacheHit, stepName, etc.).
 */
export function createEngineRunner<T>(
  workflow: AnyWorkflow,
  options: EngineRunnerOptions = {},
): EngineRunner<T> {
  const queue: T[] = []
  let done = false
  let waiter: (() => void) | null = null
  let permit: (() => void) | null = null

  const engine = createEngine({
    storage: options.storage ?? new SQLiteStorage(':memory:'),
    workflows: [workflow],
    hooks: {
      onStepComplete: (_event) => {
        // Structured logging hook point — _event.cacheHit, stepName, output available here
      },
      onRunComplete: ({ output }) => {
        queue.push(output as T)
        waiter?.(); waiter = null
        permit?.(); permit = null
      },
      onRunFailed: ({ stepName, error }) => {
        console.error(`\n[${stepName}] failed: ${error.message}`)
        permit?.(); permit = null
      },
    },
  })

  // permitPromise starts resolved so the first enqueue() proceeds immediately.
  let permitPromise: Promise<void> = Promise.resolve()
  let started = false

  async function ensureStarted(): Promise<void> {
    if (!started) {
      started = true
      await engine.start()
    }
  }

  async function enqueue(input: PersistedValue): Promise<void> {
    await ensureStarted()
    // Wait for the previous run to complete before submitting the next one.
    await permitPromise
    permitPromise = new Promise<void>((resolve) => { permit = resolve })
    await engine.enqueue(workflow.name, input)
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
    // Wait for any in-flight run before stopping.
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

  return {
    enqueue,
    [Symbol.asyncIterator]: generate,
    finish,
    dispose,
    [Symbol.asyncDispose]: finish,
  }
}
