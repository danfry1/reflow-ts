import type { StandardSchemaV1 } from '@standard-schema/spec'
import { ConfigError, DuplicateStepError, ValidationError } from './errors'
import type { PersistedValue, RetryConfig } from './types'

/** Context passed to every step handler. */
export interface StepContext<
  TInput extends PersistedValue,
  TPrev extends PersistedValue,
  TStepsSoFar extends Record<string, PersistedValue> = Record<string, PersistedValue>,
> {
  /** The validated workflow input (same for every step in the run). */
  input: TInput
  /** The return value of the previous step (`undefined` for the first step). */
  prev: TPrev
  /** Aborted on cancellation, lease loss, or step timeout. */
  signal: AbortSignal
  /** Complete the workflow early, skipping remaining steps. Optionally persist a value as the final result. */
  complete: (value?: PersistedValue) => never
  /** Results of all previously completed steps, keyed by step name. */
  steps: TStepsSoFar
}

/** Internal representation of a step used by the engine. */
export interface StepDefinition {
  name: string
  handler: (ctx: StepContext<PersistedValue, PersistedValue, Record<string, PersistedValue>>) => Promise<PersistedValue | void>
  retry?: RetryConfig
  timeoutMs?: number
}

/** A single execution unit: either a sequential step or a parallel group. */
export type ExecutionUnit =
  | { readonly kind: 'step'; readonly definition: StepDefinition }
  | { readonly kind: 'parallel'; readonly branches: readonly StepDefinition[] }

/** Configuration object form for `.step()` when you need retry or timeout options. */
export interface StepConfig<
  TInput extends PersistedValue,
  TPrev extends PersistedValue,
  TOutput extends PersistedValue | void,
  TStepsSoFar extends Record<string, PersistedValue> = Record<string, PersistedValue>,
> {
  retry?: RetryConfig
  /** Timeout per attempt in milliseconds. Takes precedence over `retry.timeoutMs`. */
  timeoutMs?: number
  handler: (ctx: StepContext<TInput, TPrev, TStepsSoFar>) => Promise<TOutput>
}

/** A parallel branch: either a bare handler or a StepConfig with retry/timeout. */
export type ParallelBranch<
  TInput extends PersistedValue,
  TPrev extends PersistedValue,
  TOutput extends PersistedValue | void = PersistedValue | void,
  TStepsSoFar extends Record<string, PersistedValue> = Record<string, PersistedValue>,
> =
  | ((ctx: StepContext<TInput, TPrev, TStepsSoFar>) => Promise<TOutput>)
  | StepConfig<TInput, TPrev, TOutput, TStepsSoFar>

/** Extract the output type from a ParallelBranch. */
export type InferBranchOutput<B> =
  B extends (ctx: never) => Promise<infer O> ? O :
  B extends { handler: (ctx: never) => Promise<infer O> } ? O :
  never

/** Context passed to the `onFailure` handler when a workflow run fails. */
export interface FailureContext<TInput extends PersistedValue = PersistedValue> {
  /** The error that caused the failure. */
  error: Error
  /** The name of the step that failed. */
  stepName: string
  /** The original validated workflow input. */
  input: TInput
}

/**
 * A typed workflow definition.
 *
 * @typeParam TName - Literal string name of the workflow (e.g. `'order-fulfillment'`)
 * @typeParam TInput - The validated input schema type
 * @typeParam TPrev - The output type of the most recently added step (used for chaining)
 * @typeParam TSteps - Accumulated map of `{ stepName: outputType }` across `.step()` calls
 */
export interface Workflow<
  TName extends string = string,
  TInput extends PersistedValue = PersistedValue,
  TPrev extends PersistedValue = PersistedValue,
  TSteps extends Record<string, PersistedValue> = Record<string, PersistedValue>,
> {
  readonly name: TName
  readonly inputSchema: StandardSchemaV1<TInput>
  readonly executionUnits: readonly ExecutionUnit[]
  readonly failureHandler?: (ctx: FailureContext<TInput>) => Promise<void>

  step<TStepName extends string, TOutput extends PersistedValue | void>(
    name: TStepName,
    handler: (ctx: StepContext<TInput, TPrev, TSteps>) => Promise<TOutput>,
  ): Workflow<TName, TInput, NormalizeOutput<TOutput>, TSteps & Record<TStepName, NormalizeOutput<TOutput>>>

  step<TStepName extends string, TOutput extends PersistedValue | void>(
    name: TStepName,
    config: StepConfig<TInput, TPrev, TOutput, TSteps>,
  ): Workflow<TName, TInput, NormalizeOutput<TOutput>, TSteps & Record<TStepName, NormalizeOutput<TOutput>>>

  parallel<TBranches extends Record<string, ParallelBranch<TInput, TPrev, PersistedValue | void, TSteps>>>(
    branches: TBranches,
  ): Workflow<
    TName,
    TInput,
    Prettify<{ [K in keyof TBranches & string]: NormalizeOutput<InferBranchOutput<TBranches[K]>> }>,
    TSteps & { [K in keyof TBranches & string]: NormalizeOutput<InferBranchOutput<TBranches[K]>> }
  >

  onFailure(
    handler: (ctx: FailureContext<TInput>) => Promise<void>,
  ): Workflow<TName, TInput, TPrev, TSteps>

  parseInput(input: unknown): TInput
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type AnyWorkflow = Workflow<string, any, any, any>

/** Extract the input type from a Workflow */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type InferInput<W> = W extends Workflow<any, infer I, any, any> ? I : never

/** Extract the accumulated steps map from a Workflow */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type InferSteps<W> = W extends Workflow<any, any, any, infer S> ? S : never

/** Map a workflow tuple/array to `{ workflowName: inputType }` */
export type WorkflowInputMap<T extends readonly AnyWorkflow[]> = {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  [W in T[number] as W extends Workflow<infer N, any, any, any> ? N : never]: InferInput<W>
}

/** Map a workflow tuple/array to `{ workflowName: stepsRecord }` */
export type WorkflowStepsMap<T extends readonly AnyWorkflow[]> = {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  [W in T[number] as W extends Workflow<infer N, any, any, any> ? N : never]: InferSteps<W>
}

/** Flatten intersection types for readable IDE tooltips */
type Prettify<T> = { [K in keyof T]: T[K] } & {}
type NormalizeOutput<T extends PersistedValue | void> = Exclude<T, void> | (void extends T ? undefined : never)

export type PrettyStepsMap<T extends Record<string, unknown>> = Prettify<T>

/**
 * Create a new workflow definition.
 *
 * @example
 * ```ts
 * const workflow = createWorkflow({
 *   name: 'process-content',
 *   input: z.object({ url: z.string() }),
 * })
 *   .step('scrape', async ({ input }) => { ... })
 *   .step('summarize', async ({ prev }) => { ... })
 * ```
 */
export function createWorkflow<TName extends string, TInput extends PersistedValue>(config: {
  name: TName
  input: StandardSchemaV1<TInput>
}): Workflow<TName, TInput, undefined, {}> {
  return buildWorkflow(config.name, config.input, [])
}

function getAllStepNames(units: readonly ExecutionUnit[]): Set<string> {
  const names = new Set<string>()
  for (const unit of units) {
    if (unit.kind === 'step') {
      names.add(unit.definition.name)
    } else {
      for (const branch of unit.branches) {
        names.add(branch.name)
      }
    }
  }
  return names
}

function buildWorkflow<
  TName extends string,
  TInput extends PersistedValue,
  TPrev extends PersistedValue,
  TSteps extends Record<string, PersistedValue>,
>(
  name: TName,
  inputSchema: StandardSchemaV1<TInput>,
  executionUnits: ExecutionUnit[],
  failureHandler?: (ctx: FailureContext<TInput>) => Promise<void>,
): Workflow<TName, TInput, TPrev, TSteps> {
  return {
    name,
    inputSchema,
    executionUnits,
    failureHandler,

    step<TStepName extends string, TOutput extends PersistedValue | void>(
      stepName: TStepName,
      handlerOrConfig:
        | ((ctx: StepContext<TInput, TPrev, TSteps>) => Promise<TOutput>)
        | StepConfig<TInput, TPrev, TOutput, TSteps>,
    ): Workflow<
      TName,
      TInput,
      NormalizeOutput<TOutput>,
      TSteps & Record<TStepName, NormalizeOutput<TOutput>>
    > {
      if (getAllStepNames(executionUnits).has(stepName)) {
        throw new DuplicateStepError(name, stepName)
      }

      const isConfig = typeof handlerOrConfig === 'object' && 'handler' in handlerOrConfig
      const handler = isConfig ? handlerOrConfig.handler : handlerOrConfig
      const retry = isConfig ? handlerOrConfig.retry : undefined
      const timeoutMs = isConfig ? handlerOrConfig.timeoutMs : undefined

      const newStep: StepDefinition = {
        name: stepName,
        handler: handler as unknown as StepDefinition['handler'],
        retry,
        timeoutMs,
      }
      return buildWorkflow<
        TName,
        TInput,
        NormalizeOutput<TOutput>,
        TSteps & Record<TStepName, NormalizeOutput<TOutput>>
      >(
        name,
        inputSchema,
        [...executionUnits, { kind: 'step', definition: newStep }],
        failureHandler,
      )
    },

    parallel<TBranches extends Record<string, ParallelBranch<TInput, TPrev, PersistedValue | void, TSteps>>>(
      branches: TBranches,
    ): Workflow<
      TName,
      TInput,
      Prettify<{ [K in keyof TBranches & string]: NormalizeOutput<InferBranchOutput<TBranches[K]>> }>,
      TSteps & { [K in keyof TBranches & string]: NormalizeOutput<InferBranchOutput<TBranches[K]>> }
    > {
      const branchEntries = Object.entries(branches)
      if (branchEntries.length === 0) {
        throw new ConfigError('parallel() requires at least one branch')
      }

      const existingNames = getAllStepNames(executionUnits)
      for (const [branchName] of branchEntries) {
        if (existingNames.has(branchName)) {
          throw new DuplicateStepError(name, branchName)
        }
      }

      const branchDefs: StepDefinition[] = branchEntries.map(([branchName, handlerOrConfig]) => {
        if (typeof handlerOrConfig === 'object' && handlerOrConfig !== null && 'handler' in handlerOrConfig) {
          return {
            name: branchName,
            handler: handlerOrConfig.handler as unknown as StepDefinition['handler'],
            retry: handlerOrConfig.retry,
            timeoutMs: handlerOrConfig.timeoutMs,
          }
        }
        return {
          name: branchName,
          handler: handlerOrConfig as unknown as StepDefinition['handler'],
          retry: undefined,
          timeoutMs: undefined,
        }
      })

      return buildWorkflow<
        TName,
        TInput,
        Prettify<{ [K in keyof TBranches & string]: NormalizeOutput<InferBranchOutput<TBranches[K]>> }>,
        TSteps & { [K in keyof TBranches & string]: NormalizeOutput<InferBranchOutput<TBranches[K]>> }
      >(
        name,
        inputSchema,
        [...executionUnits, { kind: 'parallel', branches: branchDefs }],
        failureHandler,
      )
    },

    onFailure(
      handler: (ctx: FailureContext<TInput>) => Promise<void>,
    ): Workflow<TName, TInput, TPrev, TSteps> {
      return buildWorkflow(name, inputSchema, executionUnits, handler)
    },

    parseInput(input: unknown): TInput {
      const result = inputSchema['~standard'].validate(input)
      if (result instanceof Promise) {
        throw new TypeError('Async schema validation is not supported')
      }
      if (result.issues) {
        const messages = result.issues.map((i) => i.message).join(', ')
        throw new ValidationError(`Input validation failed: ${messages}`, result.issues)
      }
      return result.value as TInput
    },
  }
}
