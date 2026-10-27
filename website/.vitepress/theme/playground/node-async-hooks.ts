/**
 * A stand-in for the `AsyncLocalStorage` the engine uses to refuse a step
 * started from inside another step's handler. The browser has no async
 * context, so this one holds its store only while the callback runs
 * synchronously; the playground's workflows never nest steps.
 */
export class AsyncLocalStorage<T> {
  private store: T | undefined

  getStore(): T | undefined {
    return this.store
  }

  run<R>(store: T, callback: () => R): R {
    const previous = this.store
    this.store = store
    try {
      return callback()
    } finally {
      this.store = previous
    }
  }
}
