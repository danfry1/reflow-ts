export interface AbortableSubscriber<T> {
  push(value: T, signal: AbortSignal): Promise<void>
  close(): void
}

export interface AsyncDisposableIterator<T> extends AsyncIterableIterator<T> {
  [Symbol.asyncDispose](): Promise<void>
}

interface PendingPush<T> {
  value: T
  resolve: () => void
  reject: (error: Error) => void
  signal: AbortSignal
  onAbort: () => void
  settled: boolean
}

export function createBoundedAsyncIterator<T>(
  capacity: number,
  onDispose: () => void,
): {
  iterator: AsyncDisposableIterator<T>
  subscriber: AbortableSubscriber<T>
} {
  const buffer: T[] = []
  const pullWaiters: Array<(result: IteratorResult<T>) => void> = []
  const pushWaiters: Array<PendingPush<T>> = []
  let closed = false
  let disposed = false

  const settlePush = (
    waiter: PendingPush<T>,
    outcome: { kind: 'resolve' } | { kind: 'reject'; error: Error },
  ): void => {
    if (waiter.settled) return
    waiter.settled = true
    waiter.signal.removeEventListener('abort', waiter.onAbort)
    if (outcome.kind === 'resolve') {
      waiter.resolve()
    } else {
      waiter.reject(outcome.error)
    }
  }

  const shiftPushWaiter = (): PendingPush<T> | undefined => {
    const waiter = pushWaiters.shift()
    if (waiter) {
      waiter.settled = true
      waiter.signal.removeEventListener('abort', waiter.onAbort)
    }
    return waiter
  }

  const fillAvailableCapacity = (): void => {
    while (!closed && buffer.length < capacity && pushWaiters.length > 0) {
      const waiter = shiftPushWaiter()
      if (!waiter) break
      buffer.push(waiter.value)
      waiter.resolve()
    }
  }

  const releasePushWaiters = (): void => {
    for (const waiter of pushWaiters.splice(0)) {
      settlePush(waiter, { kind: 'resolve' })
    }
  }

  const releasePullWaiters = (): void => {
    for (const waiter of pullWaiters.splice(0)) {
      waiter({ value: undefined, done: true })
    }
  }

  const subscriber: AbortableSubscriber<T> = {
    push(value, signal) {
      if (closed) return Promise.resolve()
      if (signal.aborted) return Promise.reject(toError(signal.reason))

      const pullWaiter = pullWaiters.shift()
      if (pullWaiter) {
        pullWaiter({ value, done: false })
        return Promise.resolve()
      }

      if (pushWaiters.length === 0 && buffer.length < capacity) {
        buffer.push(value)
        return Promise.resolve()
      }

      return new Promise<void>((resolve, reject) => {
        const pending: PendingPush<T> = {
          value,
          resolve,
          reject,
          signal,
          settled: false,
          onAbort: () => {
            const index = pushWaiters.indexOf(pending)
            if (index !== -1) {
              pushWaiters.splice(index, 1)
            }
            settlePush(pending, { kind: 'reject', error: toError(signal.reason) })
          },
        }
        pushWaiters.push(pending)
        signal.addEventListener('abort', pending.onAbort, { once: true })

        if (signal.aborted) {
          pending.onAbort()
        }
      })
    },
    close() {
      if (closed) return
      closed = true
      releasePushWaiters()
      releasePullWaiters()
    },
  }

  const dispose = (): void => {
    if (disposed) return
    disposed = true
    closed = true
    buffer.length = 0
    releasePushWaiters()
    releasePullWaiters()
    onDispose()
  }

  const iterator: AsyncDisposableIterator<T> = {
    next(): Promise<IteratorResult<T>> {
      if (disposed) {
        return Promise.resolve({ value: undefined, done: true })
      }

      if (buffer.length > 0) {
        const value = buffer.shift() as T
        fillAvailableCapacity()
        return Promise.resolve({ value, done: false })
      }

      const pending = shiftPushWaiter()
      if (pending) {
        pending.resolve()
        return Promise.resolve({ value: pending.value, done: false })
      }

      if (closed) {
        return Promise.resolve({ value: undefined, done: true })
      }

      return new Promise<IteratorResult<T>>((resolve) => pullWaiters.push(resolve))
    },
    return(): Promise<IteratorResult<T>> {
      dispose()
      return Promise.resolve({ value: undefined, done: true })
    },
    throw(error?: unknown): Promise<IteratorResult<T>> {
      dispose()
      return Promise.reject(error)
    },
    [Symbol.asyncIterator]() {
      return iterator
    },
    [Symbol.asyncDispose](): Promise<void> {
      dispose()
      return Promise.resolve()
    },
  }

  return { iterator, subscriber }
}

function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error))
}
