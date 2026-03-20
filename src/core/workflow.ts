import type { StandardSchemaV1 } from '@standard-schema/spec'
import { DuplicateStepError, ValidationError } from './errors'
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
  readonly steps: readonly StepDefinition[]
  readonly failureHandler?: (ctx: FailureContext<TInput>) => Promise<void>

  step<TStepName extends string, TOutput extends PersistedValue | void>(
    name: TStepName,
    handler: (ctx: StepContext<TInput, TPrev, TSteps>) => Promise<TOutput>,
  ): Workflow<TName, TInput, NormalizeOutput<TOutput>, TSteps & Record<TStepName, NormalizeOutput<TOutput>>>

  step<TStepName extends string, TOutput extends PersistedValue | void>(
    name: TStepName,
    config: StepConfig<TInput, TPrev, TOutput, TSteps>,
  ): Workflow<TName, TInput, NormalizeOutput<TOutput>, TSteps & Record<TStepName, NormalizeOutput<TOutput>>>

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

function buildWorkflow<
  TName extends string,
  TInput extends PersistedValue,
  TPrev extends PersistedValue,
  TSteps extends Record<string, PersistedValue>,
>(
  name: TName,
  inputSchema: StandardSchemaV1<TInput>,
  steps: StepDefinition[],
  failureHandler?: (ctx: FailureContext<TInput>) => Promise<void>,
): Workflow<TName, TInput, TPrev, TSteps> {
  return {
    name,
    inputSchema,
    steps,
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
      if (steps.some((step) => step.name === stepName)) {
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
        [...steps, newStep],
        failureHandler,
      )
    },

    onFailure(
      handler: (ctx: FailureContext<TInput>) => Promise<void>,
    ): Workflow<TName, TInput, TPrev, TSteps> {
      return buildWorkflow(name, inputSchema, steps, handler)
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
