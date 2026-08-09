import { StepTimeoutError, toError } from '../errors'

export { toError }

/**
 * Race a promise against an abort signal.
 *
 * The underlying work is not cancelled — it cannot be, for an arbitrary user
 * handler — but the caller stops waiting on it, so an aborted run unwinds
 * promptly instead of blocking on a handler that ignores its signal.
 */
export function runWithSignal<T>(
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

/**
 * Build the signal for a single step attempt: aborts when the run aborts, and
 * additionally after `timeoutMs` with a {@link StepTimeoutError}.
 *
 * Per-attempt rather than per-step, so a timeout consumes one retry rather than
 * poisoning every remaining attempt. Callers must invoke `cleanup()` once the
 * attempt settles or the timer and the run-signal listener leak.
 */
export function createAttemptSignal(
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

/** Sleep for `ms`, rejecting early if `signal` aborts. Used for retry backoff. */
export function delayWithSignal(ms: number, signal: AbortSignal): Promise<void> {
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
