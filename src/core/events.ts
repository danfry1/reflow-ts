import { clonePersistedValue } from '../storage/codec'
import type { PersistedValue } from './types'

/**
 * A lifecycle event emitted during workflow execution.
 *
 * Consumed both by the {@link EngineHooks} callbacks and by `engine.stream()`.
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
  | {
      readonly type: 'runFailed'
      readonly runId: string
      readonly workflow: string
      readonly stepName: string
      readonly error: Error
    }

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
  /**
   * Called when a background operation fails (a scheduled enqueue, a poll
   * cycle). Without this hook those errors are silently swallowed, since there
   * is no caller left to surface them to.
   */
  onError?: (error: Error) => void
}

/**
 * Copy an event for delivery to one observer.
 *
 * Payloads are cloned per recipient so a hook or stream consumer that mutates
 * what it receives cannot corrupt engine state or another observer's copy.
 */
export function cloneEngineEvent(event: EngineEvent, path: string): EngineEvent {
  switch (event.type) {
    case 'stepComplete':
      return { ...event, output: clonePersistedValue(event.output, `${path} step output`) }
    case 'runComplete':
      return { ...event, output: clonePersistedValue(event.output, `${path} run output`) }
    case 'runFailed':
      return { ...event, error: cloneError(event.error) }
    default:
      return { ...event }
  }
}

/** Shallow-copy an error, preserving its prototype and own properties (including `stack`). */
export function cloneError(error: Error): Error {
  return Object.create(
    Object.getPrototypeOf(error),
    Object.getOwnPropertyDescriptors(error),
  ) as Error
}
