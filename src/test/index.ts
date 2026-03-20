import { createEngine } from '../core/engine'
import type { PersistedValue } from '../core/types'
import { MemoryStorage } from '../storage/memory'
import type {
  AnyWorkflow,
  WorkflowInputMap,
  WorkflowStepsMap,
} from '../core/workflow'

/** The result of a single step, discriminated by status. */
export type StepOutput<T extends PersistedValue = PersistedValue> =
  | { status: 'completed'; output: T; error: null }
  | { status: 'failed'; output: null; error: string }

/** The result of running a workflow to completion via `testEngine.run()`. */
export interface RunResult<TSteps extends Record<string, PersistedValue> = Record<string, PersistedValue>> {
  status: 'completed' | 'failed'
  /** Step results keyed by step name, with typed outputs. */
  steps: { [K in keyof TSteps]: StepOutput<TSteps[K]> }
}

/** A test-only engine that runs workflows synchronously with in-memory storage. */
export interface TestEngine<
  TInputMap extends Record<string, PersistedValue> = Record<string, PersistedValue>,
  TStepsMap extends Record<string, Record<string, PersistedValue>> = Record<string, Record<string, PersistedValue>>,
> {
  run<TName extends string & keyof TInputMap>(
    workflowName: TName,
    input: TInputMap[TName],
  ): Promise<
    RunResult<TName extends keyof TStepsMap ? TStepsMap[TName] : Record<string, PersistedValue>>
  >
}

/**
 * Create a test engine with in-memory storage. Runs a workflow to completion in a single `tick()`
 * and returns typed step results.
 *
 * @example
 * ```ts
 * const te = testEngine({ workflows: [myWorkflow] })
 * const result = await te.run('my-workflow', { id: '123' })
 * expect(result.status).toBe('completed')
 * ```
 */
export function testEngine<const TWorkflows extends readonly AnyWorkflow[]>(config: {
  workflows: TWorkflows
}): TestEngine<WorkflowInputMap<TWorkflows>, WorkflowStepsMap<TWorkflows>> {
  return {
    async run(workflowName, input) {
      const storage = new MemoryStorage()
      await storage.initialize()

      const engine = createEngine({
        storage,
        workflows: config.workflows,
      })

      const run = await engine.enqueue(workflowName, input)
      await engine.tick()

      const runInfo = await storage.getRun(run.id)
      if (!runInfo) {
        throw new Error(`Run "${run.id}" not found after tick()`)
      }

      if (runInfo.status !== 'completed' && runInfo.status !== 'failed') {
        throw new Error(`Run "${run.id}" ended with unexpected status "${runInfo.status}"`)
      }

      const stepResults = await storage.getStepResults(run.id)
      const steps: Record<string, StepOutput> = {}

      for (const step of stepResults) {
        steps[step.name] = step.status === 'completed' || step.status === 'completed-early'
          ? {
              status: 'completed',
              output: step.output,
              error: null,
            }
          : {
              status: 'failed',
              output: null,
              error: step.error ?? 'Unknown error',
            }
      }

      return {
        status: runInfo.status,
        steps,
      } as never
    },
  }
}
