import { createEngine } from '../core/engine'
import { InternalError, TestRunIncompleteError } from '../core/errors'
import type { PersistedValue, StepResult } from '../core/types'
import { MemoryStorage } from '../storage/memory'

export { MemoryStorage }
import type {
  AnyWorkflow,
  WorkflowInputMap,
  WorkflowOutputMap,
  WorkflowStepsMap,
} from '../core/workflow'

/** The result of a single step, discriminated by status. */
export type StepOutput<T extends PersistedValue = PersistedValue> =
  | { status: 'completed'; output: T; error: null }
  | { status: 'failed'; output: null; error: string }

/** A conditional step whose `when` returned `false`, so it did not run. */
export interface SkippedStepOutput {
  status: 'skipped'
  output: null
  error: null
}

/** The result of running a workflow to completion via `testEngine.run()`. */
export interface RunResult<
  TSteps extends Record<string, PersistedValue> = Record<string, PersistedValue>,
  TOutput = PersistedValue,
> {
  status: 'completed' | 'failed'
  /** What the run produced: set once it has completed, `undefined` when it failed. */
  output?: TOutput
  /**
   * Step results keyed by step name, with typed outputs. A step with `when`,
   * whose entry in `steps` is optional, may also be skipped.
   */
  steps: { [K in keyof TSteps]: {} extends Pick<TSteps, K> ? StepOutput<TSteps[K]> | SkippedStepOutput : StepOutput<TSteps[K]> }
}

/** A test-only engine that runs each workflow in one pass with in-memory storage. */
export interface TestEngine<
  TInputMap extends Record<string, PersistedValue> = Record<string, PersistedValue>,
  TStepsMap extends Record<string, Record<string, PersistedValue>> = Record<string, Record<string, PersistedValue>>,
  TOutputMap extends Record<string, unknown> = Record<string, PersistedValue>,
> {
  run<TName extends string & keyof TInputMap>(
    workflowName: TName,
    input: TInputMap[TName],
  ): Promise<
    RunResult<
      TName extends keyof TStepsMap ? TStepsMap[TName] : Record<string, PersistedValue>,
      TName extends keyof TOutputMap ? TOutputMap[TName] : PersistedValue
    >
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
}): TestEngine<WorkflowInputMap<TWorkflows>, WorkflowStepsMap<TWorkflows>, WorkflowOutputMap<TWorkflows>> {
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
        throw new InternalError(`Run "${run.id}" not found immediately after being enqueued`)
      }

      if (runInfo.status !== 'completed' && runInfo.status !== 'failed') {
        throw new TestRunIncompleteError(run.id, runInfo.status)
      }

      const stepResults = await storage.getStepResults(run.id)
      const steps: Record<string, StepOutput | SkippedStepOutput> = {}

      for (const step of stepResults) {
        steps[step.name] = stepOutputOf(step)
      }

      return {
        status: runInfo.status,
        output: runInfo.status === 'completed' ? runInfo.output : undefined,
        steps,
      } as never
    },
  }
}

/** A finished run's step, as `testEngine().run()` reports it. */
function stepOutputOf(step: StepResult): StepOutput | SkippedStepOutput {
  switch (step.status) {
    case 'completed':
    case 'completed-early':
      return { status: 'completed', output: step.output, error: null }
    case 'skipped':
      return { status: 'skipped', output: null, error: null }
    case 'failed':
      return { status: 'failed', output: null, error: step.error ?? 'Unknown error' }
    default:
      // A run that completed or failed leaves no step mid-way.
      throw new InternalError(`Step "${step.name}" of a finished run is still "${step.status}"`)
  }
}
