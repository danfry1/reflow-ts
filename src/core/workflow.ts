import type { StandardSchemaV1 } from '@standard-schema/spec'
import { parseDuration } from './duration'
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
  /** The id of the run this step belongs to. */
  runId: string
  /**
   * Which attempt at this step this is, from 1. For a sequential step on a
   * started engine the count is saved with each retry, so an attempt cut off
   * by a crash is made again under the same number. A parallel branch, or a
   * step on an engine driven only by `tick()`, retries in memory, and its
   * count starts again from 1 after a crash.
   */
  attempt: number
  /**
   * A key that is the same every time this step runs for this run — across
   * retries, crashes, and other processes picking the run up — and different
   * for every other step and run. Pass it to an API that accepts an
   * idempotency key, such as a payment provider, so a step repeated after a
   * crash does not repeat its effect.
   */
  idempotencyKey: string
}

/**
 * Context passed to a step's `when` predicate. A subset of {@link StepContext}:
 * the inputs needed to decide whether the step should run, with no `signal` or
 * `complete` — the predicate makes a routing decision, it does not execute.
 */
export interface StepConditionContext<
  TInput extends PersistedValue,
  TPrev extends PersistedValue,
  TStepsSoFar extends Record<string, PersistedValue> = Record<string, PersistedValue>,
> {
  /** The validated workflow input. */
  input: TInput
  /** The output of the previous step. */
  prev: TPrev
  /** Results of all previously completed steps, keyed by step name. */
  steps: TStepsSoFar
}

/** A predicate deciding whether a step runs. Evaluated once; the decision is persisted and not re-evaluated on replay. */
export type StepCondition<
  TInput extends PersistedValue,
  TPrev extends PersistedValue,
  TStepsSoFar extends Record<string, PersistedValue> = Record<string, PersistedValue>,
> = (ctx: StepConditionContext<TInput, TPrev, TStepsSoFar>) => boolean | Promise<boolean>

/**
 * Reuse a step's result from an earlier run instead of running it again.
 *
 * A duration (`'24h'`, or milliseconds) caches the result for that long,
 * keyed on the step's input and the output of every step before it. The object form
 * sets the key and a version:
 *
 * - `key` — what makes two runs the same for this step, from `{ input, prev,
 *   steps }`. Defaults to all three. Give it when the step depends on less,
 *   or on something else, such as the time or an environment variable.
 * - `version` — change it when the step's code changes, so results from the
 *   old code are not reused.
 *
 * A result is cached only when the step completes; a failure is not, and the
 * step runs again next time.
 */
export type StepCache<
  TInput extends PersistedValue,
  TPrev extends PersistedValue,
  TStepsSoFar extends Record<string, PersistedValue> = Record<string, PersistedValue>,
> =
  | number
  | string
  | {
      /** How long a result is reused: milliseconds, or a duration such as `'24h'`. */
      ttl: number | string
      /** What makes two runs the same for this step. Defaults to `{ input, prev, steps }`. */
      key?: (ctx: StepConditionContext<TInput, TPrev, TStepsSoFar>) => PersistedValue
      /** Change it when the step's code changes, so results from the old code are not reused. */
      version?: string | number
    }

/** A step's cache settings, resolved. */
export interface StepCacheDefinition {
  readonly ttlMs: number
  readonly key?: (ctx: StepConditionContext<PersistedValue, PersistedValue, Record<string, PersistedValue>>) => PersistedValue
  readonly version?: string | number
}

/** Internal representation of a step used by the engine. */
export interface StepDefinition {
  name: string
  handler: (ctx: StepContext<PersistedValue, PersistedValue, Record<string, PersistedValue>>) => Promise<PersistedValue | void>
  retry?: RetryConfig
  timeoutMs?: number
  when?: (ctx: StepConditionContext<PersistedValue, PersistedValue, Record<string, PersistedValue>>) => boolean | Promise<boolean>
  cache?: StepCacheDefinition
}

/** Resolve a step's `cache` setting, refusing a TTL that is not a positive duration. */
export function resolveCache(
  workflowName: string,
  stepName: string,
  cache: StepCache<PersistedValue, PersistedValue> | undefined,
): StepCacheDefinition | undefined {
  if (cache === undefined) return undefined
  const settings = typeof cache === 'object' ? cache : { ttl: cache }
  const ttlMs = parseDuration(settings.ttl)
  if (ttlMs <= 0) {
    throw new ConfigError(`Step "${stepName}" in workflow "${workflowName}" needs a cache ttl above zero`)
  }
  return { ttlMs, key: settings.key, version: settings.version }
}

/** A single execution unit: a sequential step, a parallel group, a durable sleep, or a wait for an external event. */
export type ExecutionUnit =
  | { readonly kind: 'step'; readonly definition: StepDefinition }
  | { readonly kind: 'parallel'; readonly branches: readonly StepDefinition[] }
  | { readonly kind: 'sleep'; readonly name: string; readonly durationMs: number }
  | {
      readonly kind: 'waitForEvent'
      readonly name: string
      readonly schema?: StandardSchemaV1<PersistedValue>
      readonly timeoutMs?: number
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
  /** Reuse this step's result from an earlier run. See {@link StepCache}. Sequential steps only. */
  cache?: StepCache<TInput, TPrev, TStepsSoFar>
  handler: (ctx: StepContext<TInput, TPrev, TStepsSoFar>) => Promise<TOutput>
}

/**
 * Configuration form for a step guarded by a `when` predicate.
 *
 * Carried as its own type rather than an optional field on {@link StepConfig}
 * so the builder can reflect the skip in the types it produces: after a
 * conditional step, `prev` widens to include the value that passes through when
 * the step is skipped, and the step's own entry in `steps` becomes optional.
 */
export interface ConditionalStepConfig<
  TInput extends PersistedValue,
  TPrev extends PersistedValue,
  TOutput extends PersistedValue | void,
  TStepsSoFar extends Record<string, PersistedValue> = Record<string, PersistedValue>,
> extends StepConfig<TInput, TPrev, TOutput, TStepsSoFar> {
  /**
   * Predicate deciding whether this step runs. Evaluated once with the current
   * `input`, `prev`, and `steps`; when it returns `false` the step is skipped,
   * `prev` passes through unchanged, and the skip is persisted so it is not
   * re-evaluated on replay.
   *
   * `when` is not given the abort `signal` — a slow async predicate is not
   * interrupted by cancellation, which is observed once it resolves. Supported
   * on sequential `.step()` only; a `.parallel()` branch throws `ConfigError`.
   */
  when: StepCondition<TInput, TPrev, TStepsSoFar>
}

/** A parallel branch: either a bare handler or a StepConfig with retry/timeout. */
export type ParallelBranch<
  TInput extends PersistedValue,
  TPrev extends PersistedValue,
  TOutput extends PersistedValue | void = PersistedValue | void,
  TStepsSoFar extends Record<string, PersistedValue> = Record<string, PersistedValue>,
> =
  | ((ctx: StepContext<TInput, TPrev, TStepsSoFar>) => Promise<TOutput>)
  // Caching replays one step's result on its own; a branch finishes with its
  // siblings, so it is refused here and, for JavaScript callers, at runtime.
  | (Omit<StepConfig<TInput, TPrev, TOutput, TStepsSoFar>, 'cache'> & { cache?: never })

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
> extends WorkflowDefinition<TName, TInput> {
  readonly kind: 'chain'
  /** The chained steps, in order, as the engine runs them. */
  readonly executionUnits: readonly ExecutionUnit[]

  step<TStepName extends string, TOutput extends PersistedValue | void>(
    name: TStepName,
    handler: (ctx: StepContext<TInput, TPrev, TSteps>) => Promise<TOutput>,
  ): Workflow<TName, TInput, NormalizeOutput<TOutput>, TSteps & Record<TStepName, NormalizeOutput<TOutput>>>

  /**
   * Add a step guarded by a `when` predicate.
   *
   * The return type reflects that the step may not run: `prev` for the next
   * step widens to `TOutput | TPrev` (the value that passes through on a skip),
   * and this step's entry in `steps` becomes optional. Both force callers to
   * handle the skipped case rather than trusting a type that lies at runtime.
   */
  step<TStepName extends string, TOutput extends PersistedValue | void>(
    name: TStepName,
    config: ConditionalStepConfig<TInput, TPrev, TOutput, TSteps>,
  ): Workflow<
    TName,
    TInput,
    NormalizeOutput<TOutput> | TPrev,
    Prettify<TSteps & Partial<Record<TStepName, NormalizeOutput<TOutput>>>>
  >

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

  /**
   * Durably pause the workflow for `duration` before continuing. The run is
   * persisted as `sleeping` and its lease released, so the process can exit (or
   * crash) during the wait — another engine reclaims and resumes it once the
   * time elapses. `prev` passes through unchanged to the next step.
   *
   * `duration` is a number of milliseconds or a string with a unit suffix
   * (`'500ms'`, `'30s'`, `'24h'`, `'7d'`). `name` must be unique within the workflow.
   *
   * The sleep emits `stepStart` when it begins and `stepComplete` when it
   * elapses. If the run resumes on a *different* engine instance after
   * suspending, that instance emits only `stepComplete` — `stepStart` was
   * emitted by the instance that began the sleep.
   */
  sleep(name: string, duration: number | string): Workflow<TName, TInput, TPrev, TSteps>

  /**
   * Durably pause the workflow until an external event named `eventName` is
   * delivered via `engine.sendEvent(runId, eventName, payload)`. The run is
   * persisted as `waiting` with its lease released, so it survives process exit
   * and is resumed (on any engine instance) when the event arrives — or fails
   * with `WaitTimeoutError` if `options.timeoutMs` elapses first.
   *
   * The delivered payload becomes the next step's `prev` (and `steps[eventName]`).
   * If `options.schema` is given, the payload is validated against it on delivery.
   * `eventName` must be unique within the workflow. Events delivered before the
   * run reaches the wait are buffered and consumed when it gets there.
   */
  waitForEvent<TEventName extends string, TPayload extends PersistedValue = PersistedValue>(
    eventName: TEventName,
    options?: { schema?: StandardSchemaV1<unknown, TPayload>; timeoutMs?: number },
  ): Workflow<TName, TInput, TPayload, Prettify<TSteps & Record<TEventName, TPayload>>>

  onFailure(
    handler: (ctx: FailureContext<TInput>) => Promise<void>,
  ): Workflow<TName, TInput, TPrev, TSteps>
}

/**
 * What every workflow has, however it is written: chained steps
 * ({@link Workflow}, `kind: 'chain'`) or a function ({@link FunctionWorkflow},
 * `kind: 'function'`).
 */
export interface WorkflowDefinition<
  TName extends string = string,
  TInput extends PersistedValue = PersistedValue,
> {
  /** How the workflow is written: chained steps, or a function. */
  readonly kind: 'chain' | 'function'
  readonly name: TName
  readonly inputSchema: StandardSchemaV1<TInput>
  readonly failureHandler?: (ctx: FailureContext<TInput>) => Promise<void>
  /**
   * Names a run for people looking at it, such as in the dashboard: the
   * customer's email, an order number. Given the run's input. Without it a
   * run is shown by its idempotency key, or else its id.
   */
  readonly label?: (input: TInput) => string
  /**
   * The most runs of this workflow that may be running at once, across every
   * process sharing the storage. Runs past it wait their turn as `pending`.
   * Unset means no limit beyond the engine's own `concurrency`.
   */
  readonly concurrency?: number

  parseInput(input: unknown): TInput
}

/**
 * A workflow written as a function. Created by `createWorkflow(config, handler)`.
 *
 * @typeParam TOutput - What the function returns: the run's output
 * @typeParam TEvents - `{ eventName: payloadType }` for the events it declares
 *
 * @beta
 */
export interface FunctionWorkflow<
  TName extends string = string,
  TInput extends PersistedValue = PersistedValue,
  TOutput = PersistedValue,
  TEvents extends Record<string, PersistedValue> = Record<string, PersistedValue>,
> extends WorkflowDefinition<TName, TInput> {
  readonly kind: 'function'
  /**
   * The workflow's body, its input and events erased, as the engine calls it.
   * Prefixed with `~` as engine-facing, not for calling directly.
   */
  readonly '~handler': (ctx: WorkflowContext<PersistedValue, Record<string, PersistedValue>>) => Promise<PersistedValue | void>
  /** The declared events, with the schema each payload is validated against. */
  readonly events: Readonly<Record<string, StandardSchemaV1<unknown, PersistedValue>>>
  /** The most operations one run may start. See {@link FunctionWorkflowConfig.maxOperations}. */
  readonly maxOperations: number
  /** Type-only: carries the output and event types for inference. Never set at runtime. */
  readonly '~types'?: { readonly output: TOutput; readonly events: TEvents }
}

/**
 * Context passed to a workflow written as a function.
 *
 * @beta
 */
export interface WorkflowContext<
  TInput extends PersistedValue,
  TEvents extends Record<string, PersistedValue> = {},
> {
  /** The validated workflow input. */
  input: TInput
  /** Durable steps: each one's result is saved, and replayed rather than repeated when the run resumes. */
  step: WorkflowSteps<TEvents>
  /** The id of this run. */
  runId: string
}

/**
 * A `step.run` handler, as the engine calls it.
 *
 * @beta
 */
export type StepRunHandler = (ctx: StepRunContext) => Promise<unknown>

/**
 * Whether values of `T` can be saved as a step's result or a run's output:
 * plain data — objects (interfaces included), arrays, strings, numbers,
 * booleans, `null`, `undefined`, and `Date` — and not functions, `Map`s,
 * `Set`s, promises, `bigint`s, or symbols, anywhere inside.
 *
 * @beta
 */
export type Persistable<T, TDepth extends readonly unknown[] = []> =
  // `unknown` and `any` say nothing either way, as an SDK's free-form field
  // often does, so they are taken on trust and checked when saved.
  unknown extends T ? true
  : [T] extends [void] ? true
  // From here `T` distributes over a union, so `void | number` — a handler
  // that sometimes returns nothing — is checked member by member.
  : T extends void ? true
  : T extends PersistedValue ? true
  : T extends Date ? true
  : T extends (...args: never) => unknown ? false
  : T extends ReadonlyMap<unknown, unknown> | ReadonlySet<unknown> | PromiseLike<unknown> | bigint | symbol ? false
  // A type that refers to itself — a tree, a linked list, a syntax tree —
  // is checked a few levels deep and trusted below, rather than recursing
  // until the compiler gives up.
  : TDepth['length'] extends 8 ? true
  : T extends ReadonlyArray<infer E> ? Persistable<E, [...TDepth, unknown]>
  : T extends object ? (false extends { [K in keyof T]-?: Persistable<T[K], [...TDepth, unknown]> }[keyof T] ? false : true)
  : false

/**
 * `T` when it can be saved, and otherwise a type no value has, named for the
 * reason — so a handler returning something that cannot be saved is an error
 * where it returns it.
 *
 * @beta
 */
export type Saveable<T> = Persistable<T> extends true ? T : { readonly 'a step result or run output must be data that can be saved': never }

/**
 * A `step.run` handler resolving to `TOutput`, which must be data that can be saved.
 *
 * @beta
 */
export type StepHandler<TOutput> = (ctx: StepRunContext) => Promise<Saveable<TOutput>>

/**
 * Options for `step.waitForEvent()`.
 *
 * @beta
 */
export interface WaitForEventOptions<TEventName extends string = string> {
  /**
   * The event to wait for, when the wait has a name of its own — to wait for
   * the same event more than once in a run. Defaults to the wait's name.
   */
  event?: TEventName
  /**
   * Give up after this long — milliseconds, or a duration such as `'10d'` —
   * and resolve to `null`. Without it, the wait lasts until the event arrives.
   */
  timeout?: number | string
}

/**
 * What `step.waitForEvent()` resolves to: the payload, or also `null` when a
 * timeout may be set — including one that is optional, or set on only some
 * of a union of option types.
 *
 * @beta
 */
export type WaitForEventResult<TPayload, TOptions> = TOptions extends unknown
  ? 'timeout' extends keyof TOptions
    ? [Exclude<TOptions['timeout' & keyof TOptions], undefined>] extends [never] ? TPayload : TPayload | null
    : TPayload
  : never

/**
 * The durable operations of a workflow written as a function.
 *
 * Every operation has a name, unique within a run, under which its result is
 * saved. When a run resumes — after a sleep, a wait, a retry's backoff, or a
 * crash — the function runs again from the top and each operation that already
 * finished returns its saved result at once, in the order the operations first
 * finished. Code between steps runs again each time, so anything with an
 * effect or a result that can differ, such as a request, the time, or a
 * random number, belongs inside `step.run()`.
 *
 * @beta
 */
export interface WorkflowSteps<TEvents extends Record<string, PersistedValue> = {}> {
  /**
   * Run `handler` as a durable step and resolve to what it returns. A step
   * that completed on an earlier execution is not run again. Pass
   * `{ handler, retry, timeout, cache }` instead for retries, a timeout, or
   * caching.
   *
   * When every attempt fails, rejects with a `StepFailedError` (code
   * `'STEP_FAILED'`) whose `cause` is the handler's last error. Catch it to
   * carry on; it rejects the same way when the run replays, with a `cause`
   * rebuilt from the saved message, name, and `code`.
   */
  run<TOutput>(
    name: string,
    handlerOrConfig: StepHandler<TOutput> | (Omit<FunctionStepConfig, 'handler'> & { handler: StepHandler<TOutput> }),
  ): Promise<NormalizeOutput<TOutput>>

  /**
   * Durably pause for `duration`: milliseconds, or a string such as `'30s'`
   * or `'7d'`. When nothing else is running, the run is saved as `sleeping`
   * and its lease released, so the process can exit during the wait.
   */
  sleep(name: string, duration: number | string): Promise<void>

  /** Durably pause until `time`. A time already past resolves at once. */
  sleepUntil(name: string, time: Date | number): Promise<void>

  /**
   * Durably wait for `engine.sendEvent(runId, name, payload)` and resolve to
   * the payload. Events must be declared in the workflow's `events`, whose
   * schema validates each payload when it is sent. Events sent before the run
   * reaches the wait are kept, in the order they were sent, and each wait
   * takes the oldest.
   *
   * With a `timeout`, resolves to `null` if the event has not arrived in time.
   * Several waits may be pending at once on the built-in storage adapters; a
   * custom one without `supportsEventNames` allows one at a time.
   */
  waitForEvent<TEventName extends keyof TEvents & string, TOptions extends WaitForEventOptions<TEventName> = {}>(
    name: TEventName,
    options?: TOptions,
  ): Promise<WaitForEventResult<TEvents[TEventName], TOptions>>
  /**
   * Wait, under a name of its own, for the event `options.event` — to wait
   * for the same event more than once in a run. As the form above otherwise.
   */
  waitForEvent<TEventName extends keyof TEvents & string, TOptions extends WaitForEventOptions<TEventName> & { event: TEventName }>(
    name: string,
    options: TOptions,
  ): Promise<WaitForEventResult<TEvents[TEventName], TOptions>>
}

/**
 * Context passed to a step handler in a workflow written as a function.
 *
 * @beta
 */
export interface StepRunContext {
  /** Aborted on cancellation, lease loss, or the attempt's timeout. */
  signal: AbortSignal
  /** The id of the run this step belongs to. */
  runId: string
  /** Which attempt at this step this is, from 1. See {@link StepContext.attempt}. */
  attempt: number
  /** The same every time this step runs for this run. See {@link StepContext.idempotencyKey}. */
  idempotencyKey: string
}

/**
 * Retries for `step.run()`: how many attempts, and how long to wait between
 * them. `initialDelay` is milliseconds or a duration such as `'30s'`
 * (default one second), growing by `backoff` (default `'linear'`).
 *
 * @beta
 */
export type FunctionRetryConfig = Pick<RetryConfig, 'maxAttempts'> & Partial<Pick<RetryConfig, 'backoff'>> & {
  initialDelay?: number | string
}

/**
 * Reuse a `step.run()` result from an earlier run: as {@link StepCache}, but
 * `key` takes no arguments, as everything the step depends on is in scope.
 *
 * @beta
 */
export type FunctionStepCache =
  | Exclude<StepCache<PersistedValue, PersistedValue>, object>
  | (Omit<Extract<StepCache<PersistedValue, PersistedValue>, object>, 'key'> & { key?: () => PersistedValue })

/**
 * Options for `step.run()` in a workflow written as a function.
 *
 * @beta
 */
export interface FunctionStepConfig {
  retry?: FunctionRetryConfig
  /** Give up on an attempt after this long: milliseconds, or a duration such as `'30s'`. */
  timeout?: number | string
  /**
   * Reuse this step's result from an earlier run. A duration keys it on the
   * workflow input and everything earlier steps produced; the object form's
   * `key` names what makes two runs the same instead. See {@link StepCache}.
   */
  cache?: FunctionStepCache
  handler: StepRunHandler
}

/**
 * Configuration for a workflow written as a function.
 *
 * @beta
 */
export interface FunctionWorkflowConfig<
  TName extends string,
  TInput extends PersistedValue,
  TEvents extends Record<string, PersistedValue>,
> extends WorkflowConfig<TName, TInput> {
  /** The events the workflow may wait for, each with the schema its payload must match. */
  events?: { [K in keyof TEvents]: StandardSchemaV1<unknown, TEvents[K]> }
  /**
   * Called once a run has failed, after it is recorded as failed. `stepName`
   * is empty when the function's own code threw rather than a step.
   */
  onFailure?: (ctx: FailureContext<TInput>) => Promise<void>
  /**
   * The most operations one run may start, counting each `step.run`, sleep,
   * and wait once (default 10,000). A run resumes by replaying every
   * operation before the point it stopped, so one that grows without bound
   * costs more on each resume; past the limit it fails with `ConfigError`.
   * Raise it for a run that needs more, or split the work across runs.
   */
  maxOperations?: number
}

/** Configuration shared by both ways of writing a workflow. */
export interface WorkflowConfig<TName extends string, TInput extends PersistedValue> {
  name: TName
  input: StandardSchemaV1<TInput>
  /** A name for each run, from its input, shown wherever runs are listed. See {@link WorkflowDefinition.label}. */
  label?: (input: TInput) => string
  /** The most runs of this workflow running at once, across all processes. See {@link WorkflowDefinition.concurrency}. */
  concurrency?: number
}

/** Any chained workflow, whatever its name, input, and steps. */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type ChainWorkflow = Workflow<string, any, any, any>

/**
 * Any workflow, chained or written as a function. Narrow on `kind` — or use
 * {@link ChainWorkflow} — before reading `executionUnits` or chaining steps.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type AnyWorkflow = ChainWorkflow | FunctionWorkflow<string, any, any, any>

/** Extract the input type from a Workflow */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type InferInput<W> = W extends WorkflowDefinition<any, infer I> ? I : never

/**
 * Extract a run's output type. A chained workflow's output may come from
 * `complete()` anywhere in the chain, so it is untyped.
 *
 * @beta
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type InferOutput<W> = W extends FunctionWorkflow<any, any, infer O, any> ? O : PersistedValue

/**
 * Extract the `{ eventName: payloadType }` map of a workflow written as a function.
 *
 * @beta
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type InferEvents<W> = W extends FunctionWorkflow<any, any, any, infer E> ? E : Record<string, PersistedValue>

/**
 * Map a workflow tuple/array to `{ workflowName: outputType }`
 *
 * @beta
 */
export type WorkflowOutputMap<T extends readonly AnyWorkflow[]> = {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  [W in T[number] as W extends WorkflowDefinition<infer N, any> ? N : never]: InferOutput<W>
}

/**
 * Extract the accumulated steps map from a Workflow. A workflow written as a
 * function names its steps as it runs, so its map is untyped.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type InferSteps<W> = W extends Workflow<any, any, any, infer S> ? S : Record<string, PersistedValue>

/** Map a workflow tuple/array to `{ workflowName: inputType }` */
export type WorkflowInputMap<T extends readonly AnyWorkflow[]> = {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  [W in T[number] as W extends WorkflowDefinition<infer N, any> ? N : never]: InferInput<W>
}

/** Map a workflow tuple/array to `{ workflowName: stepsRecord }` */
export type WorkflowStepsMap<T extends readonly AnyWorkflow[]> = {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  [W in T[number] as W extends WorkflowDefinition<infer N, any> ? N : never]: InferSteps<W>
}

/** Flatten intersection types for readable IDE tooltips */
type Prettify<T> = { [K in keyof T]: T[K] } & {}
/** A step's output as later code sees it: `void` becomes `undefined`, and every other type, `undefined` included, is kept. */
type NormalizeOutput<T> = T extends void ? undefined : T

export type PrettyStepsMap<T extends Record<string, unknown>> = Prettify<T>

/**
 * Create a workflow written as a function: one async function whose durable
 * operations are `step.run`, `step.sleep`, `step.sleepUntil`, and
 * `step.waitForEvent`, with ordinary code between them. See the guide's
 * "Workflows as Functions".
 *
 * @beta Workflows written as a function are new: their API may still change in a minor release.
 *
 * @example
 * ```ts
 * const workflow = createWorkflow({
 *   name: 'process-content',
 *   input: z.object({ url: z.string() }),
 * }, async ({ input, step }) => {
 *   const page = await step.run('scrape', async () => { ... })
 *   return await step.run('summarize', async () => { ... })
 * })
 * ```
 */
export function createWorkflow<
  TName extends string,
  TInput extends PersistedValue,
  TOutput,
  TEvents extends Record<string, PersistedValue> = {},
>(
  config: FunctionWorkflowConfig<TName, TInput, TEvents>,
  handler: (ctx: WorkflowContext<TInput, TEvents>) => Promise<Saveable<TOutput>>,
): FunctionWorkflow<TName, TInput, NormalizeOutput<TOutput>, TEvents>
/**
 * Create a new workflow definition, built by chaining steps.
 *
 * Declared last, so `ReturnType<typeof createWorkflow>` is the chained builder.
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
export function createWorkflow<TName extends string, TInput extends PersistedValue>(
  config: WorkflowConfig<TName, TInput>,
): Workflow<TName, TInput, undefined, {}>
export function createWorkflow<TName extends string, TInput extends PersistedValue>(
  config: FunctionWorkflowConfig<TName, TInput, Record<string, PersistedValue>>,
  handler?: (ctx: WorkflowContext<TInput, Record<string, PersistedValue>>) => Promise<PersistedValue | void>,
): Workflow<TName, TInput, undefined, {}> | FunctionWorkflow<TName, TInput> {
  const { concurrency } = config
  if (concurrency !== undefined && (!Number.isInteger(concurrency) || concurrency < 1)) {
    throw new ConfigError(`Workflow "${config.name}" concurrency must be a positive integer`)
  }
  if (!handler) {
    return buildWorkflow(config.name, config.input, [], undefined, { label: config.label, concurrency })
  }
  const inputSchema = config.input
  return {
    kind: 'function',
    name: config.name,
    inputSchema,
    // The engine hands the body its own validated input, so erasing the input
    // type here loses nothing it relies on.
    '~handler': handler as unknown as FunctionWorkflow['~handler'],
    events: config.events ?? {},
    maxOperations: resolveMaxOperations(config.name, config.maxOperations),
    failureHandler: config.onFailure,
    label: config.label,
    concurrency,
    parseInput: (input) => parseWith(inputSchema, input),
  }
}

/** What a workflow carries beside its steps, unchanged as steps are added. */
interface WorkflowSettings<TInput extends PersistedValue> {
  readonly label?: (input: TInput) => string
  readonly concurrency?: number
}

function getAllStepNames(units: readonly ExecutionUnit[]): Set<string> {
  const names = new Set<string>()
  for (const unit of units) {
    if (unit.kind === 'step') {
      names.add(unit.definition.name)
    } else if (unit.kind === 'sleep' || unit.kind === 'waitForEvent') {
      names.add(unit.name)
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
  failureHandler: ((ctx: FailureContext<TInput>) => Promise<void>) | undefined,
  settings: WorkflowSettings<TInput>,
): Workflow<TName, TInput, TPrev, TSteps> {
  return {
    kind: 'chain',
    name,
    inputSchema,
    executionUnits,
    failureHandler,
    label: settings.label,
    concurrency: settings.concurrency,

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
      const when = isConfig && 'when' in handlerOrConfig ? handlerOrConfig.when : undefined
      const cache = isConfig ? handlerOrConfig.cache : undefined

      const newStep: StepDefinition = {
        name: stepName,
        handler: handler as unknown as StepDefinition['handler'],
        retry,
        timeoutMs,
        when: when as unknown as StepDefinition['when'],
        cache: resolveCache(name, stepName, cache as unknown as StepCache<PersistedValue, PersistedValue> | undefined),
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
        settings,
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
          if ('when' in handlerOrConfig && handlerOrConfig.when !== undefined) {
            // `when` skips a step by passing `prev` through to the next one.
            // There is no well-defined passthrough for one branch of a
            // concurrent group, so reject it rather than silently ignoring it.
            throw new ConfigError(
              `Conditional "when" is not supported on parallel branch "${branchName}" in workflow "${name}"`,
            )
          }
          if ('cache' in handlerOrConfig && handlerOrConfig.cache !== undefined) {
            throw new ConfigError(`Step caching is not supported on parallel branch "${branchName}" in workflow "${name}"`)
          }
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
        settings,
      )
    },

    sleep(sleepName: string, duration: number | string): Workflow<TName, TInput, TPrev, TSteps> {
      if (getAllStepNames(executionUnits).has(sleepName)) {
        throw new DuplicateStepError(name, sleepName)
      }

      const durationMs = parseDuration(duration)

      return buildWorkflow<TName, TInput, TPrev, TSteps>(
        name,
        inputSchema,
        [...executionUnits, { kind: 'sleep', name: sleepName, durationMs }],
        failureHandler,
        settings,
      )
    },

    waitForEvent<TEventName extends string, TPayload extends PersistedValue>(
      eventName: TEventName,
      options?: { schema?: StandardSchemaV1<unknown, TPayload>; timeoutMs?: number },
    ): Workflow<TName, TInput, TPayload, Prettify<TSteps & Record<TEventName, TPayload>>> {
      if (getAllStepNames(executionUnits).has(eventName)) {
        throw new DuplicateStepError(name, eventName)
      }
      if (options?.timeoutMs !== undefined && (!Number.isFinite(options.timeoutMs) || options.timeoutMs <= 0)) {
        throw new ConfigError(`waitForEvent "${eventName}" timeoutMs must be a positive number`)
      }

      return buildWorkflow<TName, TInput, TPayload, Prettify<TSteps & Record<TEventName, TPayload>>>(
        name,
        inputSchema,
        [
          ...executionUnits,
          {
            kind: 'waitForEvent',
            name: eventName,
            schema: options?.schema as StandardSchemaV1<PersistedValue> | undefined,
            timeoutMs: options?.timeoutMs,
          },
        ],
        failureHandler,
        settings,
      )
    },

    onFailure(
      handler: (ctx: FailureContext<TInput>) => Promise<void>,
    ): Workflow<TName, TInput, TPrev, TSteps> {
      return buildWorkflow(name, inputSchema, executionUnits, handler, settings)
    },

    parseInput(input: unknown): TInput {
      return parseWith(inputSchema, input)
    },
  }
}

/** The most operations a function workflow's run may start, unless it says otherwise. */
const DEFAULT_MAX_OPERATIONS = 10_000

/** A function workflow's operation limit, refusing one that is not a positive integer. */
function resolveMaxOperations(workflowName: string, maxOperations: number | undefined): number {
  if (maxOperations === undefined) return DEFAULT_MAX_OPERATIONS
  if (!Number.isInteger(maxOperations) || maxOperations < 1) {
    throw new ConfigError(`Workflow "${workflowName}" maxOperations must be a positive integer`)
  }
  return maxOperations
}

/** Validate a workflow's input against its schema. */
function parseWith<TInput extends PersistedValue>(inputSchema: StandardSchemaV1<TInput>, input: unknown): TInput {
  const result = inputSchema['~standard'].validate(input)
  if (result instanceof Promise) {
    throw new TypeError('Async schema validation is not supported')
  }
  if (result.issues) {
    const messages = result.issues.map((i) => i.message).join(', ')
    throw new ValidationError(`Input validation failed: ${messages}`, result.issues)
  }
  return result.value
}
