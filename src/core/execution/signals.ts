import { StepTimeoutError, toError } from '../errors'

export { toError }

/** Each signal's callbacks for {@link onAbort}, behind one real listener. */
const abortCallbacks = new WeakMap<AbortSignal, Set<() => void>>()

/**
 * Call `callback` once `signal` aborts; returns a function that removes it.
 *
 * Every callback for a signal shares one `abort` listener and is kept in a
 * set, so adding and removing one costs the same however many are waiting —
 * `addEventListener` scans a signal's listeners on each add, which made
 * thousands of concurrent steps on one run's signal cost time growing with
 * the square of their number. Callbacks run in the order they were added,
 * as listeners would. A signal already aborted calls nothing, so callers
 * check `signal.aborted` first.
 */
export function onAbort(signal: AbortSignal, callback: () => void): () => void {
  let callbacks = abortCallbacks.get(signal)
  if (!callbacks) {
    const created = new Set<() => void>()
    callbacks = created
    abortCallbacks.set(signal, created)
    signal.addEventListener('abort', () => {
      for (const waiting of [...created]) waiting()
      created.clear()
    }, { once: true })
  }
  const registered = callbacks
  registered.add(callback)
  return () => {
    registered.delete(callback)
  }
}

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
    const cleanup = onAbort(signal, () => {
      cleanup()
      reject(toError(signal.reason))
    })

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
    cleanups.push(onAbort(runSignal, () => forwardAbort(runSignal.reason)))
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

    const stopListening = onAbort(signal, () => {
      cleanup()
      reject(toError(signal.reason))
    })

    const cleanup = () => {
      clearTimeout(timer)
      stopListening()
    }
  })
}
